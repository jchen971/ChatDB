ChatDB: An Interactive SQL Query Generation Tool

ChatDB is a Python-based tool that enables users to analyze datasets using SQL queries generated dynamically based on natural language inputs, keywords, or randomization. It works with MySQL databases and provides descriptive insights into query outputs, making it easy to explore data even without SQL expertise.

generate_sample_queries.py: Handles query generation based on keywords or query types (e.g., GROUP BY, WHERE). Includes logic to ensure accurate and meaningful SQL queries are created.

handle_natural_language.py: Processes user inputs in natural language. Uses NLP tools (e.g., NLTK) to parse and extract key components for query generation.

simple_chatdb.py: The main script to run ChatDB. Manages user input, interacts with the database, and coordinates query generation and execution.
Outputs results and SQL descriptions.

requirements.txt: Lists all required Python libraries for the program, including their versions (e.g., nltk, pymysql).
