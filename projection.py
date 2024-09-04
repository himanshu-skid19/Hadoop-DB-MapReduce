#!/usr/bin/env python3
import re
import sys


def parse_sql(sql_statement):
    """
    Parses a simple SQL statement to extract the column name and value.
    Assumes the SQL statement is of the form: SELECT column1, column2, ... FROM table; 
    """

    # Regular expression to match the columns
    match = re.search(r"SELECT\s+((?:\w+(?:\s*,\s*\w+)*))(?:\s+FROM\s+(\w+))?", sql_statement, re.IGNORECASE)

    if match:
        columns = [col.strip() for col in match.group(1).split(',')]
        table = match.group(2)
        return columns, table
    else:
        raise ValueError("Invalid SQL statement format")
    

sql_statement = "SELECT VendorID, passenger_count FROM tripdata"
columns, table = parse_sql(sql_statement)


def mapper():
    headers = None
    column_indices = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        if headers is None:
            headers = line.split(',')
            column_indices = [headers.index(col) for col in columns if col in headers]
            continue

        values = line.split(',')
        selected_values = [values[i] for i in column_indices]
        projection = ','.join(selected_values)
        print(f"{projection}\t{projection}")


def reducer():
    last_key = None

    for line in sys.stdin:
        line = line.strip()
        parts = line.split('\t', 1)

        if len(parts) < 2:  # Check if the line is incorrectly formatted
            continue  # Skip this line or handle the error differently

        key, _ = parts

        print(key)
        


    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == 'mapper':
            mapper()
        elif sys.argv[1] == 'reducer':
            reducer()
        else:
            print("Invalid argument. Use 'mapper' or 'reducer'.")
    else:
        print("No arguments provided.")
