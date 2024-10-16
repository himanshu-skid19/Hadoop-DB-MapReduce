#!/usr/bin/env python3
import re
import sys
import os

def parse_sql_inner_join(sql_statement):
    """
    Parses a SQL statement to extract the tables involved in an inner join,
    the columns to be projected, and the join conditions.

    Args:
        sql_statement (str): The SQL query.

    Returns:
        dict: A dictionary containing the projections, table names, and the join condition.
    """
    # Regex to capture the SELECT, FROM, JOIN, and ON parts of the SQL statement
    pattern = r"SELECT\s+(.*?)\s+FROM\s+(\w+)\s+INNER JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)"
    match = re.search(pattern, sql_statement, re.IGNORECASE)
    
    if not match:
        raise ValueError("Invalid SQL statement or no inner join found")

    # Extract the projections, table names, and join condition
    projections = match.group(1).strip()
    table1 = match.group(2).strip()
    table2 = match.group(3).strip()
    join_condition_left = match.group(4).strip()
    join_condition_right = match.group(5).strip()

    # Split projections if multiple columns are selected
    if projections == "*":
        projections = ['*']  # Wildcard selection (all columns)
    else:
        projections = [col.strip() for col in projections.split(',')]

    # Return parsed components in a structured format
    return {
        "projections": projections,
        "table1": table1,
        "table2": table2,
        "join_condition": {
            "left": join_condition_left,
            "right": join_condition_right
        }
    }


def mapper(file_type):
    """
    Mapper function for inner join.
    This function determines whether the line is from 'views' or 'carts' dynamically.
    """
    # Get the input file name from the environment variable
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Split the line into fields (assuming comma-delimited CSV)
        fields = line.split(',')
        
        if file_type == 'views':
            category_id = fields[1]  # Assuming 'category_id' is the second field in views
            print(f"{category_id}\tviews,{','.join(fields)}")
        elif file_type == 'carts':
            category_id = fields[1]  # Assuming 'category_id' is the second field in carts
            print(f"{category_id}\tcarts,{','.join(fields)}")


def reducer():
    """
    Reducer function for inner join.
    It performs the join between two datasets based on a shared key (category_id).
    """
    current_category_id = None
    views_data = []
    carts_data = []

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Split the line into key and value
        category_id, value = line.split('\t', 1)
        dataset_id, data = value.split(',', 1)

        # If this is a new category_id, process the previous one
        if current_category_id != category_id:
            if current_category_id:
                # Perform the join between views and carts on the previous category_id
                for view in views_data:
                    for cart in carts_data:
                        print(f"{current_category_id}\t{view},{cart}")
            
            # Reset for new category_id
            current_category_id = category_id
            views_data = []
            carts_data = []

        # Add data to the respective dataset list
        if dataset_id == "views":
            views_data.append(data)
        elif dataset_id == "carts":
            carts_data.append(data)
    
    # Process the last category_id
    if current_category_id:
        for view in views_data:
            for cart in carts_data:
                print(f"{current_category_id}\t{view},{cart}")


if __name__ == "__main__":
    if len(sys.argv) > 2:
        if sys.argv[1] == 'mapper':
            mapper(sys.argv[2])  # Pass the file type ('views' or 'carts')
        elif sys.argv[1] == 'reducer':
            reducer()
        else:
            print("Invalid argument. Use 'mapper' or 'reducer'.", file=sys.stderr)
    elif len(sys.argv) == 2:
        # If only 'mapper' or 'reducer' is provided, use a default for the mapper
        if sys.argv[1] == 'mapper':
            mapper('views')  # Default to 'views' if no file type is provided
        elif sys.argv[1] == 'reducer':
            reducer()
        else:
            print("Invalid argument. Use 'mapper' or 'reducer'.", file=sys.stderr)
    else:
        # Default to reducer if no arguments provided and stdin is piped
        if not sys.stdin.isatty():
            reducer()
        else:
            print("No arguments provided. Use 'mapper' or 'reducer' and the file type.", file=sys.stderr)
