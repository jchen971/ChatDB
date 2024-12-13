import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re
import pymysql  # or your preferred database library

# Download required NLTK data files (run these once)
#nltk.download('punkt', quiet=True)
#nltk.download('averaged_perceptron_tagger', quiet=True)
#nltk.download('stopwords', quiet=True)

# Natural Language Processing Functions
def natural_language_query(connection, sentence, selected_table):
    query, params = translate_to_sql(sentence, connection, selected_table)
    if query:
        print("\nGenerated SQL Query:")
        print(query)
        print("Parameters:", params)

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            try:
                cursor.execute(query, params)
                results = cursor.fetchall()
            except Exception as e:
                print(f"Error executing query: {e}")
                return

        if results:
            print("\nQuery Results:")
            for row in results:
                print(row)
        else:
            print("\nNo results returned by the query.")
    else:
        print("Failed to generate a query.")

def get_table_structure(connection, selected_table):
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(f"DESCRIBE {selected_table}")
        columns_info = cursor.fetchall()
        column_data_types = {row['Field']: row['Type'] for row in columns_info}
    return column_data_types

def translate_to_sql(sentence, connection, selected_table):
    # Fetch table structure
    table_structure = get_table_structure(connection, selected_table)
    table_columns = list(table_structure.keys())

    # Define synonyms for mapping
    synonyms = {
        'product': 'product_id',
        'product id': 'product_id',
        'date': 'purchase_date',
        'purchase date': 'purchase_date',
        'time': 'purchase_time',
        'purchase time': 'purchase_time',
        'country': 'country_code',
        'country code': 'country_code',
        'price': 'price_per_unit',
        'price per unit': 'price_per_unit',
        'units': 'units_sold',
        'units sold': 'units_sold',
        'total units sold': 'units_sold',
        'currency': 'currency',
        # Add more synonyms as needed
    }

    # Tokenize the sentence
    tokens = word_tokenize(sentence.lower())

    # Join tokens back to a string for pattern matching
    sentence_lower = ' '.join(tokens)

    # Attempt to detect if the sentence matches a known pattern
    pattern_name, components = detect_pattern(sentence_lower)
    if pattern_name:
        # Map components to columns
        mapped_columns = map_components_to_columns(components, table_columns, synonyms)
        query, params = generate_aggregate_query(mapped_columns, selected_table, pattern_name)
        if query:
            return query, params  # Return the generated query

    # Initialize query components
    select_columns = []
    conditions = []
    action = "SELECT"
    params = []

    # Function to normalize text
    def normalize(text):
        return re.sub(r'\s+|_', '', text.lower())

    # Identify columns mentioned in the sentence
    attributes = extract_attributes(tokens, table_columns, synonyms)
    if attributes:
        select_columns = attributes
    else:
        select_columns = ["*"]

    # Operator map with multi-word operators
    operator_map = {
        'greater than or equal to': '>=',
        'less than or equal to': '<=',
        'greater than': '>',
        'more than': '>',
        'higher than': '>',
        'less than': '<',
        'fewer than': '<',
        'lower than': '<',
        'equal to': '=',
        'equals': '=',
        'is': '=',
        'between': 'BETWEEN',
        'not equal to': '!=',
        'not equals': '!=',
        'before': '<',
        'after': '>',
    }

    # Build patterns for operators, sorted by length to match longer phrases first
    operator_phrases = sorted(operator_map.keys(), key=lambda x: -len(x))
    operator_pattern = '|'.join(map(re.escape, operator_phrases))

    # Extract conditions from the sentence
    stop_words = set(stopwords.words('english'))
    for column in table_columns:
        column_variants = [column.lower().replace('_', ' ')]
        # Include synonyms
        for key, value in synonyms.items():
            if value == column:
                column_variants.append(key)

        # Build a regex pattern for each operator
        patterns = []

        # BETWEEN operator
        patterns.append(
            rf"({'|'.join(map(re.escape, column_variants))})\s+(?:is\s+)?between\s+(\S+)\s+and\s+(\S+)"
        )

        # Other operators
        for op_phrase in operator_phrases:
            if op_phrase != 'between':
                patterns.append(
                    rf"({'|'.join(map(re.escape, column_variants))})\s+(?:is\s+)?{op_phrase}\s+(\S+)"
                )

        # Equality without operator phrases
        patterns.append(
            rf"({'|'.join(map(re.escape, column_variants))})\s+(?:is\s+)?(\S+)"
        )

        # Try matching patterns
        for pattern in patterns:
            regex = re.compile(pattern, re.IGNORECASE)
            matches = regex.findall(sentence_lower)
            if matches:
                for match in matches:
                    col_match = match[0]
                    groups = match[1:]
                    if 'between' in pattern:
                        value1, value2 = groups
                        conditions.append(f"{column} BETWEEN %s AND %s")
                        params.extend([value1, value2])
                    else:
                        value = groups[-1]
                        # Skip if value is a stopword
                        if value.lower() in stop_words:
                            continue
                        # Extract the operator from the matched string
                        op_match = re.search(operator_pattern, pattern, re.IGNORECASE)
                        if op_match:
                            op_phrase = op_match.group()
                            op_symbol = operator_map.get(op_phrase.lower(), '=')
                        else:
                            op_symbol = '='
                        conditions.append(f"{column} {op_symbol} %s")
                        params.append(value)
                break  # Stop after the first match for this column

    # Check for aggregate functions in the sentence
    aggregate_functions = {
        'total': 'SUM',
        'sum': 'SUM',
        'average': 'AVG',
        'avg': 'AVG',
        'count': 'COUNT',
        'maximum': 'MAX',
        'max': 'MAX',
        'minimum': 'MIN',
        'min': 'MIN',
    }

    # Detect if an aggregate function is used
    agg_func = None
    for word in tokens:
        if word in aggregate_functions:
            agg_func = aggregate_functions[word]
            break

    # Handle grouping if necessary
    group_by_columns = []
    group_by_clause = ''
    if 'each' in tokens or 'per' in tokens or 'by' in tokens:
        # Attempt to find the grouping column
        for idx, token in enumerate(tokens):
            if token in ['each', 'per', 'by']:
                next_tokens = tokens[idx + 1:idx + 4]  # Get the next few tokens
                possible_column = ' '.join(next_tokens)
                normalized_column = synonyms.get(possible_column, possible_column)
                for column in table_columns:
                    if normalize(column) == normalize(normalized_column):
                        group_by_columns.append(column)
                        break
        if group_by_columns:
            group_by_clause = f" GROUP BY {', '.join(group_by_columns)}"

    # Adjust select columns based on aggregate function and grouping
    if agg_func and select_columns != ["*"]:
        if group_by_columns:
            select_clause = ', '.join(
                group_by_columns +
                [f"{agg_func}({col}) AS {agg_func.lower()}_{col}" for col in select_columns if col not in group_by_columns]
            )
        else:
            select_clause = ', '.join([f"{agg_func}({col}) AS {agg_func.lower()}_{col}" for col in select_columns])
    elif group_by_columns:
        select_clause = ', '.join(set(select_columns + group_by_columns))
    else:
        select_clause = ', '.join(set(select_columns))

    # Build the SQL query
    query = f"{action} {select_clause} FROM {selected_table}"

    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    query += group_by_clause

    # Handle ordering if superlative adjectives are found
    order_by_clause = ''
    limit_clause = ''
    superlative_map = {
        'highest': ('DESC', 1),
        'largest': ('DESC', 1),
        'most': ('DESC', 1),
        'lowest': ('ASC', 1),
        'smallest': ('ASC', 1),
        'least': ('ASC', 1),
        'latest': ('DESC', 1),
        'earliest': ('ASC', 1),
        'oldest': ('ASC', 1),
        'newest': ('DESC', 1),
    }

    for idx, word in enumerate(tokens):
        if word in superlative_map:
            order_direction, limit = superlative_map[word]
            # Assume the next word is the column to order by
            next_tokens = tokens[idx + 1:idx + 4]
            possible_column = ' '.join(next_tokens)
            normalized_column = synonyms.get(possible_column, possible_column)
            for column in table_columns:
                if normalize(column) == normalize(normalized_column):
                    order_by_clause = f" ORDER BY {column} {order_direction}"
                    limit_clause = f" LIMIT {limit}"
                    break
            if order_by_clause:
                break
    query += order_by_clause + limit_clause
    query += ";"
    return query, params

def detect_pattern(sentence):
    patterns = {
        'total_A_by_B': r'(?:what\s+is\s+)?(?:the\s+)?(?:total|sum)\s+(?P<A>[\w\s]+?)\s+(?:by|for\s+each|per)\s+(?P<B>[\w\s]+)',
        'count_A_by_B': r'(?:what\s+is\s+)?(?:the\s+)?(?:number|count)\s+of\s+(?P<A>[\w\s]+?)\s+(?:by|for\s+each|per)\s+(?P<B>[\w\s]+)',
        'average_A_by_B': r'(?:what\s+is\s+)?(?:the\s+)?(?:average|avg)\s+of\s+(?P<A>[\w\s]+?)\s+(?:by|for\s+each|per)\s+(?P<B>[\w\s]+)',
        'count_by_B': r'(?:what\s+is\s+)?(?:the\s+)?(?:number|count)\s+(?:of\s+)?(?:records|entries|rows)?\s*(?:by|for\s+each|per)\s+(?P<B>[\w\s]+)',
        'list_A_grouped_by_B': r'(?:list\s+of\s+)?(?P<A>[\w\s]+?)\s+(?:grouped\s+by|by)\s+(?P<B>[\w\s]+)',
        # Add other patterns as needed
    }

    for pattern_name, pattern_regex in patterns.items():
        match = re.search(pattern_regex, sentence)
        if match:
            return pattern_name, match.groupdict()
    return None, None

def map_components_to_columns(components, table_columns, synonyms):
    def normalize(text):
        return re.sub(r'\s+|_', '', text.lower())

    mapped_columns = {}
    for key, value in components.items():
        normalized_value = value.strip().lower()
        normalized_value = synonyms.get(normalized_value, normalized_value)

        for column in table_columns:
            if normalize(column) == normalize(normalized_value):
                mapped_columns[key] = column
                break
        else:
            if key == 'A' and value in ['records', 'entries', 'rows']:
                mapped_columns[key] = '*'  # For COUNT(*) cases
            else:
                mapped_columns[key] = None
    return mapped_columns

def generate_aggregate_query(mapped_columns, selected_table, pattern_name):
    B = mapped_columns.get('B')
    A = mapped_columns.get('A')

    if pattern_name == 'count_by_B':
        if B is None:
            return None, None
        query = f"SELECT {B}, COUNT(*) AS count FROM {selected_table} GROUP BY {B};"
        params = []
        return query, params

    if None in mapped_columns.values():
        return None, None

    query = ''
    params = []
    if pattern_name == 'total_A_by_B':
        query = f"SELECT {B}, SUM({A}) AS total_{A} FROM {selected_table} GROUP BY {B};"
    elif pattern_name == 'count_A_by_B':
        query = f"SELECT {B}, COUNT({A}) AS count_{A} FROM {selected_table} GROUP BY {B};"
    elif pattern_name == 'average_A_by_B':
        query = f"SELECT {B}, AVG({A}) AS average_{A} FROM {selected_table} GROUP BY {B};"
    elif pattern_name == 'list_A_grouped_by_B':
        query = f"SELECT {B}, {A} FROM {selected_table} GROUP BY {B}, {A};"
    else:
        return None, None

    return query, params

def normalize(text):
    return re.sub(r'\s+|_', '', text.lower())

def extract_attributes(tokens, table_columns, synonyms):
    attributes = []
    token_text = ' '.join(tokens)
    possible_attributes = []

    # Create a list of possible column names including synonyms
    possible_columns = set(table_columns)
    possible_columns.update(synonyms.keys())

    # Iterate over possible n-grams in the tokenized text
    for n in range(1, 4):  # Adjust n for longer attributes if necessary
        for i in range(len(tokens) - n + 1):
            gram = ' '.join(tokens[i:i + n])
            normalized_gram = synonyms.get(gram, gram)
            for column in possible_columns:
                if normalize(column) == normalize(normalized_gram):
                    attributes.append(synonyms.get(column, column))
                    break

    # Remove duplicates and return
    return list(set(attributes))
