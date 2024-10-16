# MapReduce Project

This project is a simple implementation of MapReduce for processing CSV data. It includes a mapper and a reducer. Make sure you have wsl installed.

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


7. Remove the existing output directory if needed by running the command:

```bash
hadoop fs -rm -r /home/hadoop/hadoopdata/hdfs/output
```

10. Command to copy a file from windows to WSL:


```bash
cp /mnt/c/Users/himan/Desktop/Dump/Hadoop/mapper.py ~/mapper.py

```
The above is an example of how to copy, ensure you copy all the files in this repo in the hadoop filesystem.

11. Convert mapper.py and reducer.py to executables by running:

```
chmod +x ~/mapper.py

```
Do this for all the .py files in the repo

12. Convert Windows Line Endings to Unix line Endings (if you edited your script in windwos, it might have Windwos-style line endings - '\r\n' which should be converted to Unix Style Line Endings - '\n')

```bash
dos2unix ~/mapper.py
dos2unix ~/reducer.py
```
Again, do this for all the .py files in the repo

13. test locally before running on hadoop:

```bash
hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/tripdata.csv | python3 
mapper.py | sort | python3 reducer.py

hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/tripdata.csv | python3 projection.py mapper | sort | python3 projection.py reducer

```
This is just to test the files locally if you so wish


14. Run ```main.py```



