import os
import pymysql
import pandas as pd
import random
from generate_sample_queries import generate_sample_queries
from handle_natural_language import natural_language_query


# Step 1: Function to upload CSV file and create a table in the database with appropriate column types
def upload_csv_to_database(connection, csv_path, table_name):
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Validate if DataFrame is empty
        if df.empty:
            print("The CSV file is empty. Please provide a valid CSV file.")
            return
        
        # Determine appropriate column types based on data
        columns = df.columns
        column_definitions = []

        # Add an auto-increment ID as the primary key
        column_definitions.append("id INT AUTO_INCREMENT PRIMARY KEY")
        
        for col in columns:
            if pd.api.types.is_integer_dtype(df[col]):
                column_definitions.append(f"{col} INT")
            elif pd.api.types.is_float_dtype(df[col]):
                column_definitions.append(f"{col} DECIMAL(10, 2)")
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                column_definitions.append(f"{col} DATETIME")
            else:
                max_length = df[col].astype(str).map(len).max()
                column_definitions.append(f"{col} VARCHAR({max(255, max_length)})")
        
        # Create table query
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)});"
        
        # Execute create table query
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
        
        # Insert data into the table
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))});"
        rows_inserted = 0

        with connection.cursor() as cursor:
            for _, row in df.iterrows():
                try:
                    # Convert row values to tuple and handle any NaN values by replacing them with None
                    row_values = tuple(row.where(pd.notna(row), None))
                    cursor.execute(insert_query, row_values)
                    rows_inserted += 1
                except pymysql.MySQLError as e:
                    print(f"Failed to insert row: {row_values}. Error: {e}")
        
        connection.commit()
        print(f"Table '{table_name}' created and {rows_inserted} rows inserted successfully.")

    except FileNotFoundError:
        print("The specified CSV file was not found. Please check the file path and try again.")
    except pd.errors.EmptyDataError:
        print("The provided CSV file is empty. Please provide a valid CSV file.")
    except pymysql.MySQLError as e:
        print(f"MySQL Error occurred: {e}")
    except Exception as e:
        print(f"Error occurred: {e}")

# Step 1: Function to list available tables in the database and let user select one
def list_tables_and_select(connection):
    try:
        with connection.cursor() as cursor:
            # Fetch available tables
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()

            if not tables:
                print("No tables found in the database.")
                return None

            # Display available tables with indexing for easier selection
            print("Available tables:")
            table_list = [table[0] for table in tables]
            for index, table in enumerate(table_list, start=1):
                print(f"{index}. {table}")

            # Automatically return selected table without extra input
            user_input = input("Enter the number of the table you want to explore: ")
            if user_input.isdigit():
                table_index = int(user_input)
                if 1 <= table_index <= len(table_list):
                    return table_list[table_index - 1]
                else:
                    print("Invalid index. Please enter a number from the list above.")
                    return list_tables_and_select(connection)
            else:
                print("Invalid input. Please enter a valid table number.")
                return list_tables_and_select(connection)

    except Exception as e:
        print(f"Error occurred while listing tables: {e}")
        return None

# Step 2: Function to explore a specific table and show its columns and sample data
def explore_table(connection, table_name):
    try:
        # Fetch the table columns and attributes
        with connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table_name};")
            columns_info = cursor.fetchall()
            
            if not columns_info:
                print(f"No columns found in the table {table_name}")
                return
            
            print(f"\nTable: {table_name}")
            for column_info in columns_info:
                print(f" - {column_info[0]} ({column_info[1]})")
            
            # Fetch and display sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
            sample_data = cursor.fetchall()
            if sample_data:
                print("Sample data:")
                for row in sample_data:
                    print(row)
            else:
                print("No data available in the table.")
    except Exception as e:
        print(f"Error occurred while exploring the table: {e}")


db_config = {
    "host": "localhost",
    "user": "root",
    "password": "12345678",
    "database": "costco",
    "port": 3306,
    
}


if __name__ == "__main__":
    try:
        connection = pymysql.connect(**db_config)
        
        user_choice = input("Would you like to (1) Upload a CSV file or (2) Select an existing table? Enter 1 or 2: ")
        if user_choice == '1':
            csv_path = input("Enter the path of the CSV file to upload: ")
            if os.path.exists(csv_path):
                table_name = input("Enter the table name to create in the database: ")
                upload_csv_to_database(connection, csv_path, table_name)
                explore_table(connection, table_name)
            else:
                print("CSV file does not exist.")
        elif user_choice == '2':
            table_name = list_tables_and_select(connection)
            if table_name:
                explore_table(connection, table_name)
        else:
            print("Invalid choice. Please enter 1 or 2.")
            exit()
        
        query_choice = input("Would you like to (1) Generate SQL queries randomly, (2) Generate SQL queries by keywords, or (3) Generate SQL queries by natural language? Enter 1, 2, or 3: ")
        if query_choice == '1':
            query_result = generate_sample_queries(connection, table_name)
            if query_result:
                query, description = query_result
                print(f"\nGenerated Query: {query}\nDescription: {description}")
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    print("Result:")
                    for row in result:
                        print(row)
        if query_choice == '2':
            keyword_query = input("Enter the keywords for query generation: ").lower()
            valid_keywords = ["sum", "min", "max", "count", "group by", "having", "order by"]
            if keyword_query in valid_keywords:
                # Map the keyword to query_type and aggregation_function if applicable
                if keyword_query in ["sum", "min", "max", "count"]:
                    query_type = 'aggregation'
                    aggregation_function = keyword_query.upper()
                    query_result = generate_sample_queries(connection, table_name, query_type, aggregation_function)
                else:
                    keyword_to_query_type = {
                        'group by': 'group_by',
                        'having': 'group_by',  # Adjust as needed
                        'order by': 'order_by',
                    }
                    query_type = keyword_to_query_type[keyword_query]
                    query_result = generate_sample_queries(connection, table_name, query_type)
                if query_result:
                    query, description = query_result
                    print(f"\nGenerated Query: {query}\nDescription: {description}")
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                        result = cursor.fetchall()
                        print("\nQuery Results:")
                        for row in result:
                            print(row)
                else:
                    print("Failed to generate a query.")
            else:
                print("Invalid keyword. Please enter one of the following:")
                print(", ".join(valid_keywords))


        elif query_choice == '3':
            user_natural_language_query = input("Enter a natural language query: ")

            try:
                query = natural_language_query(connection, user_natural_language_query, table_name)
                if query:
                    print(f"\nGenerated SQL Query: {query}")
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                        result = cursor.fetchall()
                        print("\nQuery Results:")
                        for row in result:
                            print(row)
                # else:
                    # print("Failed to generate a query.")
            except Exception as e:
                print(f"Failed to generate or execute the query: {e}")

        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

    except pymysql.MySQLError as e:
        print(f"Database error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if 'connection' in locals() and connection.open:
            connection.close()
            print("Database connection closed.")
