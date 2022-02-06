#MaxKValues
A Hadoop MapReduce job for finding the largest k numbers from a dataset of integers.

## Build Java Job

```bash
git clone repository url

mvn clean

mvn install
```

## Run in local Hadoop environment
Install Hadoop

Start Hadoop environment

```start-dfs.cmd``` (In Windows)

```start-yarn.cmd```

Upload data

```hadoop fs –mkdir /inputdir```

```hadoop dfs –put data.txt /inputdir```

Run MaxKValues job

```hadoop jar emr-max-k-values.jar /inputdir/data.txt /outputdir/```

View results in /outputdir/

## Run in AWS EMR

Create EMR Cluster

Upload jar and input file to AWS S3

Add a Step in EMR with paths for jar, input files and output directory. 

View results in output directory
