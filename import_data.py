import subprocess
import csv
import sys

def import_data(file_path, hdfs_path):
    try:
        command = ["hadoop", "fs", "-put", file_path, hdfs_path]

        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode == 0:
            print(f"Data imported successfully to {hdfs_path}")
        else:
            print(f"Failed to import data: {result.stderr.decode('utf-8')}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")


def extract_headers_and_save(data_file, output_csv):
    """
    Extract headers from the first line of the data file and save it as a CSV file.
    :param data_file: Path to the input data file (containing headers on the first line).
    :param output_csv: Path to the output CSV file to save the headers.
    """
    # Open the input data file
    with open(data_file, 'r') as f:
        # Read the first line as the headers
        headers = f.readline().strip().split(',')
    
    # Save headers to the output CSV file
    with open(output_csv, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
    
    print(f"Headers extracted and saved to {output_csv}: {headers}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python import_data.py <data_file> <hdfs_path>")
        sys.exit(1)

    data_file = sys.argv[1]
    hdfs_path = sys.argv[2]

    extract_headers_and_save(data_file, hdfs_path)
    # import_data("headers.csv", hdfs_path)


