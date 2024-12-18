# ChatDB: An Interactive SQL Query Generation Tool #

## Overview ##
ChatDB is a Python-based tool that enables users to analyze datasets using SQL queries generated dynamically based on natural language inputs, keywords, or randomization. It works with MySQL databases and provides descriptive insights into query outputs, making it easy to explore data even without SQL expertise.

## Description ##
1. generate_sample_queries.py: Handles query generation based on keywords or query types (e.g., GROUP BY, WHERE). Includes logic to ensure accurate and meaningful SQL queries are created.

2. handle_natural_language.py: Processes user inputs in natural language. Uses NLP tools (e.g., NLTK) to parse and extract key components for query generation.

3. simple_chatdb.py: The main script to run ChatDB. Manages user input, interacts with the database, and coordinates query generation and execution.
Outputs results and SQL descriptions.

4. requirements.txt: Lists all required Python libraries for the program, including their versions (e.g., nltk, pymysql).

## Commands
- git clone https://github.com/jchen971/ChatDB.git
- cd ChatDB
- pip install -r requirement.txt
- python simple_chatdb.py

