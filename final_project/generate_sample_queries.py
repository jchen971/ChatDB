import random

def generate_sample_queries(connection, selected_table, query_type=None, aggregation_function=None):
    try:
        # Fetch column names and data types for the selected table
        with connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE {selected_table}")
            columns_info = cursor.fetchall()
            columns = [row[0] for row in columns_info]
            column_data_types = {row[0]: row[1] for row in columns_info}

        if not columns:
            print(f"No columns found in the table {selected_table}.")
            return None

        # If query_type is not specified, choose one randomly
        if query_type is None:
            query_type = random.choice(["simple_select", "where", "group_by", "order_by", "aggregation"])

        query = ""
        descriptive_sentence = ""

        if query_type == "simple_select":
            # Select a random column or all columns
            if random.choice([True, False]):
                selected_columns = "*"
                descriptive_sentence = f"Select all columns from the table '{selected_table}'."
            else:
                selected_columns = random.choice(columns)
                descriptive_sentence = f"Select the column '{selected_columns}' from the table '{selected_table}' limited to 10 rows."
            query = f"SELECT {selected_columns} FROM {selected_table} LIMIT 10;"

        elif query_type == "where":
            # Select a random column and apply a random condition
            condition_column = random.choice(columns)
            # Determine the data type for condition value
            data_type = column_data_types[condition_column]
            if "int" in data_type or "float" in data_type:
                condition_value = random.randint(1, 100)
                operator = random.choice(["=", ">", "<", ">=", "<=", "!="])
                query = f"SELECT * FROM {selected_table} WHERE {condition_column} {operator} {condition_value} LIMIT 10;"
                descriptive_sentence = f"Select all columns from '{selected_table}' where '{condition_column}' {operator} {condition_value}, limited to 10 rows."
            elif "date" in data_type:
                condition_value = "2022-01-01"
                operator = random.choice(["=", ">", "<", ">=", "<=", "!="])
                query = f"SELECT * FROM {selected_table} WHERE {condition_column} {operator} '{condition_value}' LIMIT 10;"
                descriptive_sentence = f"Select all columns from '{selected_table}' where '{condition_column}' {operator} '{condition_value}', limited to 10 rows."
            else:
                # For string types
                condition_value = "'SampleValue'"
                operator = random.choice(["=", "LIKE"])
                query = f"SELECT * FROM {selected_table} WHERE {condition_column} {operator} {condition_value} LIMIT 10;"
                descriptive_sentence = f"Select all columns from '{selected_table}' where '{condition_column}' {operator} {condition_value}, limited to 10 rows."

        elif query_type == "group_by":
            if len(columns) > 1:
                group_by_column = random.choice(columns)
                aggregate_column = random.choice([col for col in columns if col != group_by_column])
                query = (f"SELECT {group_by_column}, COUNT({aggregate_column}) as count "
                         f"FROM {selected_table} GROUP BY {group_by_column};")
                descriptive_sentence = f"group rows by the column '{group_by_column}' and counts the occurrences of '{aggregate_column}'."
            else:
                query = (f"SELECT {columns[0]}, COUNT({columns[0]}) as count "
                         f"FROM {selected_table} GROUP BY {columns[0]};")
                descriptive_sentence = f"group rows by the column '{columns[0]}' and counts the occurrences."


        elif query_type == "order_by":
            order_by_column = random.choice(columns)
            order_direction = random.choice(["ASC", "DESC"])
            limit = random.randint(5, 15)

            # Add a fallback ordering by a different column if values are the same in order_by_column
            fallback_column = random.choice([col for col in columns if col != order_by_column])

            query = f"SELECT * FROM {selected_table} ORDER BY {order_by_column} {order_direction}, {fallback_column} ASC LIMIT {limit};"
            descriptive_sentence = (f"Select all columns from '{selected_table}', ordered by '{order_by_column}' in "
                                    f"{order_direction}ending order and by '{fallback_column}' in ascending order if there is a tie, "
                                    f"limited to {limit} rows.")

        elif query_type == "aggregation":
            # Select a numeric column for aggregation
            numeric_columns = [col for col in columns if "int" in column_data_types[col] or "float" in column_data_types[col]]
            if not numeric_columns:
                print("No numeric columns available for aggregation.")
                return None

            agg_column = random.choice(numeric_columns)

            # Use the specified aggregation function or choose one randomly
            if aggregation_function is None:
                aggregation_function = random.choice(["MIN", "MAX", "COUNT", "AVG", "SUM"])

            if aggregation_function == "MIN":
                query = f"SELECT MIN({agg_column}) AS min_value FROM {selected_table};"
                descriptive_sentence = f"Find the minimum value of the column '{agg_column}' in the table '{selected_table}'."
            elif aggregation_function == "MAX":
                query = f"SELECT MAX({agg_column}) AS max_value FROM {selected_table};"
                descriptive_sentence = f"Find the maximum value of the column '{agg_column}' in the table '{selected_table}'."
            elif aggregation_function == "COUNT":
                query = f"SELECT COUNT({agg_column}) AS total_count FROM {selected_table} WHERE {agg_column} IS NOT NULL;"
                descriptive_sentence = f"Find the total number of non-null entries in the column '{agg_column}' from the table '{selected_table}'."
            elif aggregation_function == "AVG":
                query = f"SELECT AVG({agg_column}) AS average_value FROM {selected_table};"
                descriptive_sentence = f"Find the average value of the column '{agg_column}' in the table '{selected_table}'."
            elif aggregation_function == "SUM":
                query = f"SELECT SUM({agg_column}) AS total_sum FROM {selected_table};"
                descriptive_sentence = f"Find the sum of all values in the column '{agg_column}' from the table '{selected_table}'."
            else:
                print(f"Unsupported aggregation function: {aggregation_function}")
                return None

        else:
            print(f"Unsupported query type: {query_type}")
            return None

        print(f"Generated Sample Query with query type '{query_type}':\n")
        print(query)
        print("\nDescription:")
        print(descriptive_sentence)

        return query, descriptive_sentence

    except Exception as e:
        print(f"Error while exploring tables: {e}")
        return None
