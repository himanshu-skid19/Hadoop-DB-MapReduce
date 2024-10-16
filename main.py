import subprocess
import sys
import os

# Function to run a bash command via subprocess
def run_bash_command(command):
    """
    Run a bash command via subprocess.
    Args:
        command (str): The bash command to run.
    """
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}\n{e}")
        sys.exit(1)


def upload_to_hadoop(local_path, hadoop_path):
    """
    Uploads a file or directory to Hadoop HDFS.
    Args:
        local_path (str): The local path to the file or directory.
        hadoop_path (str): The destination HDFS path.
    """
    command = f"hadoop fs -put {local_path} {hadoop_path}"
    print(f"Uploading {local_path} to {hadoop_path}...")
    run_bash_command(f"hadoop fs -mkdir -p {hadoop_path}")
    run_bash_command(command)
    print(f"Upload complete.")


def run_projection(input_path, output_path, sql_statement):
    """
    Runs the projection operation on the specified input file.
    Args:
        input_path (str): HDFS input path.
        output_path (str): HDFS output path.
    """
    command = f"hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -input {input_path} -output {output_path} -mapper 'python3 projection.py \"{sql_statement}\" mapper' -reducer 'python3 projection.py \"{sql_statement}\" reducer' -file projection.py"
    run_bash_command(command)
    print("Projection operation complete.")


def run_inner_join():
    """
    Runs the inner join operation on two input files.
    Args:
        input_path1 (str): HDFS path to the first file.
        input_path2 (str): HDFS path to the second file.
        output_path (str): HDFS output path.
    """

    run_bash_command("hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/views.csv | python3 inner_join.py mapper views > temp_views_output.txt")
    run_bash_command("hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/carts.csv | python3 inner_join.py mapper carts > temp_carts_output.txt")
    run_bash_command("cat temp_views_output.txt temp_carts_output.txt | sort > sorted_output.txt")
    run_bash_command("hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/carts.csv | python3 inner_join.py mapper carts > temp_carts_output.txt")
    run_bash_command("cat sorted_output.txt | python3 inner_join.py reducer")
    print("Inner join operation complete.")



# Function to run the filter operation
def run_filter(input_path, output_path, sql_statement):
    """
    Runs the filter operation on the specified input file.
    Args:
        input_path (str): HDFS input path.
        output_path (str): HDFS output path.
        sql_statement (str): SQL statement for filtering.
    """
    # Escape double quotes in the SQL statement
    sql_statement_escaped = sql_statement.replace('"', '\\"')

    # Construct the Hadoop streaming command
    command = (
        f"hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar "
        f"-input {input_path} "
        f"-output {output_path} "
        f"-mapper 'python3 filter.py \"{sql_statement_escaped}\" mapper' "
        f"-reducer 'python3 filter.py \"{sql_statement_escaped}\" reducer' "
        f"-file filter.py"
    )
    
    # Run the command
    run_bash_command(command)
    print("Filter operation complete.")

# Function to run the groupby operation
def run_groupby(input_path,sql_statement):
    """
    Runs the groupby operation on the specified input file.
    Args:
        input_path (str): HDFS input path.
        output_path (str): HDFS output path.
        groupby_column (str): The column to group by.
    """
    command = f"hadoop fs -cat {input_path}/tripdata.csv | python3 groupby.py '{sql_statement}' mapper | sort | python3 groupby.py '{sql_statement}' reducer"
    run_bash_command(command)
    print("Groupby operation complete.")



# Main function to control the SQL operations
def main():
    # run_bash_command("start-all.sh")
    print("Welcome to the Hadoop SQL Operations tool!")
    print("Choose an operation to perform:")
    print("1. Projection")
    print("2. Inner Join")
    print("3. Filter")
    print("4. Groupby")

    has_uploaded = input("Have you already uploaded the data to HDFS? (yes/no): ")
    hadoop_path = "/home/hadoop/hadoopdata/hdfs/data"
    if has_uploaded.lower() == 'no':
        data_path = input("Enter the local path to your data: ")
        # Upload the data to HDFS
        upload_to_hadoop(data_path, hadoop_path)

    operation = input("Enter the number corresponding to the operation: ")
    if operation == '1':
        sql_statement = input("Enter the sql statement: ")
        output_file = "/home/hadoop/hadoopdata/hdfs/output/"
        try:
            run_bash_command("hadoop fs -rm -r /home/hadoop/hadoopdata/hdfs/output")
        except:
            pass
        run_projection(hadoop_path, output_file, sql_statement)
        show_output = input("Would you like to view the output? {yes/no} ")
        if show_output == "yes":
            run_bash_command("hadoop fs -cat /home/hadoop/hadoopdata/hdfs/output/part-*")
    
    elif operation == '2':
        run_inner_join()
    
    elif operation == '3':
        sql_statement = input("Enter the sql statement: ")
        output_file = "/home/hadoop/hadoopdata/hdfs/output/"
        try:
            run_bash_command("hadoop fs -rm -r /home/hadoop/hadoopdata/hdfs/output")
        except:
            pass
        run_filter(hadoop_path, output_file, sql_statement)
        show_output = input("Would you like to view the output? {yes/no} ")
        if show_output == "yes":
            run_bash_command("hadoop fs -cat /home/hadoop/hadoopdata/hdfs/output/part-*")

    
    elif operation == '4':
        sql_statement = input("Enter the sql statement: ")
        run_groupby(hadoop_path, sql_statement)
    
    else:
        print("Invalid operation selected. Exiting.")


if __name__ == "__main__":
    main()