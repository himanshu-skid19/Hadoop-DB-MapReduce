#!/usr/bin/env python3
import re
import sys
import csv


def parse_sql(sql_statement):
    """
    Parses a SQL statement to extract multiple filtering conditions including operators.
    Assumes the SQL statement is of the form: SELECT * FROM table WHERE column1 = value1 AND column2 > value2;
    """
    where_clause = {}
    projections = []
    
    # Capture all conditions in the WHERE clause, including operators and values
    # Pattern modified to accept numeric values (e.g., 10) and alphanumeric values (e.g., value1)
    pattern = r"(\w+)\s*([=<>!]+)\s*([^\s,]+)"
    
    # Extract everything after WHERE and before optional GROUP BY or ORDER BY
    conditions = re.search(r"WHERE\s+(.*?)(?:GROUP BY|ORDER BY|$)", sql_statement, re.IGNORECASE)
    
    if conditions:
        # Find all key-operator-value triplets in the conditions
        matches = re.findall(pattern, conditions.group(1))
        for match in matches:
            where_clause[match[0]] = {'operator': match[1], 'value': match[2]}

        # Extract everything after SELECT and before WHERE
        projection = re.search(r"SELECT\s+((?:\*|\w+(?:\s*,\s*\w+)*))(?:\s+FROM\s+(\w+))?", sql_statement, re.IGNORECASE)
        if projection: 
            if projection.group(1).strip() == '*':
                projections = ['*']
            else:
                projections = [col.strip() for col in projection.group(1).split(',')]
            table = projection.group(2)
        else:
            raise ValueError("Invalid SQL statement format")
    
    if not where_clause:
        raise ValueError("No valid WHERE clause found")
    
    return where_clause, projections, table




def mapper(filters, projections, table):
    headers = []
    with open('/mnt/c/Users/himan/Desktop/Dump/Hadoop/headers.csv', 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Read the first row as headers

        # Detect and process headers or data
    filter_indices = {col: headers.index(col) for col in filters if col in headers}

    if projections[0] != '*':
        column_indices = [headers.index(col) for col in projections if col in headers]
    else:
        column_indices = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # print(f"DEBUG: filter_indices: {filter_indices}", file=sys.stderr)
        # Process data lines after headers are processed
        data = line.split(',')
        num_conditions = len(filters)
        num_passed = 0
        for col, val in filters.items():
            try:
                value = float(val['value'])
                data_val = float(data[filter_indices[col]])
            except:
                value = val['value']
                data_val = data[filter_indices[col]]

            if val['operator'] == '=':
                if data_val != value:
                    break
            elif val['operator'] == '>=':
                if data_val < value:
                    break
            elif val['operator'] == '<=':
                if data_val > value:
                    break
            elif val['operator'] == '>':
                if data_val <= value:
                    break
            elif val['operator'] == '<':
                if data_val >= value:
                    break
            else:
                raise ValueError(f"Unsupported operator: {val['operator']}")
            
            num_passed += 1
        if num_passed == num_conditions:
            if column_indices == None:
                print(line)
            else:
                selected_values = [data[i] for i in column_indices]
                projection = ','.join(selected_values)
                print(projection)
        
        # if all(data[filter_indices[col]] == val['value'] for col, val in filters.items()):
        #     print(line)
        

def reducer():
    for line in sys.stdin:
        print(line.strip())

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python projection.py <SQL statement> <mapper|reducer>")
        sys.exit(1)

    # Get the SQL statement from the command line argument
    sql_statement = sys.argv[1]

    print(f"SQL Statement Received: {sql_statement}", file=sys.stderr)  # Debugging line

    filters, projections, table = parse_sql(sql_statement)

    # Determine whether to run mapper or reducer
    if sys.argv[2] == 'mapper':
        mapper(filters, projections, table)
    elif sys.argv[2] == 'reducer':
        reducer()
    else:
        print("Invalid argument. Use 'mapper' or 'reducer'.")