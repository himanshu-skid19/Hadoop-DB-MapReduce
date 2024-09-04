#!/usr/bin/env python3
import re
import sys

def parse_sql(sql_statement):
    """
    Parses a SQL statement to extract multiple filtering conditions including operators.
    Assumes the SQL statement is of the form: SELECT * FROM table WHERE column1 = 'value1' AND column2 > 'value2';
    """
    where_clause = {}
    projections = []
    
    # Capture all conditions in the WHERE clause, including operators
    pattern = r"(\w+)\s*([=<>!]+)\s*'([^']+)'"
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

# Example usage:
sql_statement = "SELECT VendorID, passenger_count, trip_distance, tpep_pickup_datetime FROM tripdata WHERE VendorID = '2' AND passenger_count >= '4' AND trip_distance <= '10'"

filters, projections, table = parse_sql(sql_statement)
# print(filters)  
# print(projections)
# print(table)

def mapper():
    headers = None
    filter_indices = {}
    headers_processed = False  # Flag to check if headers have been processed
    column_indices = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Detect and process headers or data
        if not headers_processed:
            # Assume the first line could be either headers or data
            fields = line.split(',')
            
            # Check if all filter columns are present in the fields
            if all(column in fields for column in filters):
                headers = fields
                filter_indices = {column: headers.index(column) for column in filters}
                if projections[0] != '*':
                    column_indices = [headers.index(col) for col in projections if col in headers]
                headers_processed = True
                continue
            else:
                # If not all filter columns are present, treat as data
                headers = [f"col_{i}" for i in range(len(fields))]
                filter_indices = {column: i for i, column in enumerate(filters)}
                headers_processed = True
        

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
    if len(sys.argv) > 1:
        if sys.argv[1] == 'mapper':
            mapper()
        elif sys.argv[1] == 'reducer':
            reducer()
        else:
            print("Invalid argument. Use 'mapper' or 'reducer'.")
    else:
        print("No arguments provided.")