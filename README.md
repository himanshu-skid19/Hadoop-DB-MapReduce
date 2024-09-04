# MapReduce Project

This project is a simple implementation of MapReduce for processing CSV data. It includes a mapper and a reducer.

## How to run

1. Install Python 3.x

2. Clone the repository

3. In WSL switch user to hadoop by running the command:

```bash
su - hadoop
```

4. run the command to start the hadoop cluster:

```bash
start-dfs.sh
```

5. run the command to check status of the hadoop cluster:

```bash
jps
```

6. If not made already, make the directories in HDFS:

```bash
hadoop fs -mkdir -p /home/hadoop/hadoopdata/hdfs/data
```

7. Copy the file to HDFS:

```bash
hadoop fs -put /mnt/c/Users/himan/Desktop/Dump/Hadoop/archive/tripdata.csv /home/hadoop/hadoopdata/hdfs/data
```

8. Run the command to format the namenode:

```bash
hadoop dfs -rm -r /user/hadoop/output
```

9. Remove the existing output directory if needed by running the command:

```bash
hadoop fs -rm -r /home/hadoop/hadoopdata/hdfs/output
```

10. Command to copy a file from windows to WSL:

```bash
cp /mnt/c/Users/himan/Desktop/Dump/Hadoop/mapper.py ~/mapper.py

```

11. Convert mapper.py and reducer.py to executables by running:

```
chmod +x ~/mapper.py
chmod +x ~/reducer.py

```

12. Convert Windows Line Endings to Unix line Endings (if you edited your script in windwos, it might have Windwos-style line endings - '\r\n' which should be converted to Unix Style Line Endings - '\n')

```bash
dos2unix ~/mapper.py
dos2unix ~/reducer.py
```

13. test locally before running on hadoop:

```bash
hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/tripdata.csv | python3 
mapper.py | sort | python3 reducer.py

hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/tripdata.csv | python3 projection.py mapper | sort | python3 projection.py reducer

```



14. Run the hadoop streaming Job:

```bash
hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar   -input /home/hadoop/hadoopdata/hdfs/data/   -output /home/hadoop/hadoopdata/hdfs/output/   -mapper "python3 mapper.py"   -reducer "python3 reducer.py"
```

15. Check the output:

```bash
hadoop fs -ls /home/hadoop/hadoopdata/hdfs/output/
hadoop fs -cat /home/hadoop/hadoopdata/hdfs/output/part-*
```

16. Save the output to a file:

```bash
hadoop fs -get /home/hadoop/hadoopdata/hdfs/output/part-00000 /home/hadoop/hadoopdata/hdfs/output/part-00000
```




