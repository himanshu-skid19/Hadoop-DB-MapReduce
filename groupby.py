#!/usr/bin/env python3
import re
import sys
from collections import defaultdict
import csv

def parse_sql(sql_statement):
    """
    Parses a SQL statement to extract filtering conditions, projections, grouping, and aggregations.
    Assumes the SQL statement is of the form: 
    SELECT [columns/aggregations] FROM table [WHERE conditions] [GROUP BY columns];
    """
    where_clause = {}
    projections = []
    group_by = []
    aggregations = {}

    # Extract everything after SELECT and before FROM
    projection_match = re.search(r"SELECT\s+(.*?)\s+FROM", sql_statement, re.IGNORECASE)
    if projection_match:
        projection_items = [item.strip() for item in projection_match.group(1).split(',')]
        for item in projection_items:
            agg_match = re.match(r"(\w+)\((.*?)\)", item)
            if agg_match:
                agg_func, agg_col = agg_match.groups()
                aggregations[agg_col] = agg_func.upper()
            else:
                projections.append(item)
    
    # Extract the table name
    table_match = re.search(r"FROM\s+(\w+)", sql_statement, re.IGNORECASE)
    table = table_match.group(1) if table_match else None
    
    # Extract WHERE conditions without single quotes
    where_match = re.search(r"WHERE\s+(.*?)(?:GROUPBY|$)", sql_statement, re.IGNORECASE)
    if where_match:
        # Pattern to match conditions without single quotes
        condition_pattern = r"(\w+)\s*([=<>!]+)\s*([^\s,]+)"
        matches = re.findall(condition_pattern, where_match.group(1))
        for match in matches:
            where_clause[match[0]] = {'operator': match[1], 'value': match[2]}
    
    # Extract GROUP BY columns
    group_by_match = re.search(r"GROUPBY\s+(.*?)$", sql_statement, re.IGNORECASE)
    if group_by_match:
        group_by = [col.strip() for col in group_by_match.group(1).split(',')]
    
    return where_clause, projections, table, group_by, aggregations



# print(where_clause)
# print(projections)
# print(table)
# print(group_by)
# print(aggregations)

def mapper(where_clause, projections, table, group_by, aggregations):
    headers = []
    headers_processed = False
    with open('/mnt/c/Users/himan/Desktop/Dump/Hadoop/headers.csv', 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        if headers_processed == False:
            # headers = line.split(',')
            column_indices = [headers.index(col) for col in projections if col in headers]
            aggregation_indices = [headers.index(col) for col in aggregations if col in headers]
            headers_processed = True
            continue

        values = line.split(',')

        # Create the key for the GROUP BY columns
        group_key = ','.join([values[i] for i in column_indices])

        # Create the value for the aggregation columns
        agg_values = [values[i] for i in aggregation_indices]

        # Output key-value pair
        print(f"{group_key}\t{','.join(agg_values)}")


# Define the aggregation functions outside the loop for clarity and efficiency
def sum_agg(values):
    return sum(values)

def avg_agg(values):
    return sum(values) / len(values) if values else 0

def max_agg(values):
    return max(values)

def min_agg(values):
    return min(values)

def count_agg(values):
    return len(values)

# Map of aggregation functions
aggregation_funcs = {
    'SUM': sum_agg,
    'AVG': avg_agg,
    'MAX': max_agg,
    'MIN': min_agg,
    'COUNT': count_agg
}
current_key = None
agg_data = defaultdict(list)
def reducer():
    global current_key, agg_data
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            key, value = line.split('\t')
            values = list(map(float, value.split(',')))
        except ValueError:
            print(f"Skipping line: {line}")
            continue  # Skip lines that don't properly parse

        if key != current_key:
            if current_key is not None:
                # Output results for the previous key
                results = [str(aggregation_funcs[func](agg_data[col])) for col, func in aggregations.items()]
                print(f"{current_key}\t{','.join(results)}")
                agg_data.clear()  # Reset the aggregation data

            current_key = key  # Set the new key

        # Accumulate values for each column that needs aggregation
        for col, val in zip(aggregations.keys(), values):
            agg_data[col].append(val)

    # Output the last key
    if current_key:
        results = [str(aggregation_funcs[func](agg_data[col])) for col, func in aggregations.items()]
        print(f"{current_key}\t{','.join(results)}")



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python projection.py <SQL statement> <mapper|reducer>")
        sys.exit(1)

    # Get the SQL statement from the command line argument
    sql_statement = sys.argv[1]

    print(f"SQL Statement Received: {sql_statement}", file=sys.stderr)  # Debugging line

    where_clause, projections, table, group_by, aggregations = parse_sql(sql_statement)

    # Determine whether to run mapper or reducer
    if sys.argv[2] == 'mapper':
        mapper(where_clause, projections, table, group_by, aggregations)
    elif sys.argv[2] == 'reducer':
        reducer()
    else:
        print("Invalid argument. Use 'mapper' or 'reducer'.")

