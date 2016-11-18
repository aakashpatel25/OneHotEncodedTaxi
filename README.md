# OneHotEncodedTaxi
Big Data Project

### How to read Parquet file

inputs = 'parquetfiledirectory/'

myParquet = sqlContext.read.parquet(inputs)
tipsData = myParquet.select(column names)

tipsData.show()


## Execute Spark Command on AWS EMR

To run simple Spark Program
```shell
spark-submit --master yarn filename
```

```shell
spark-submit --deploy-mode cluster --master yarn --num-executors 5 --executor-cores 5 --executor-memory 20g –conf spark.yarn.submit.waitAppCompletion=false wordcount.py s3://inputbucket/input.txt s3://outputbucket/
```

### From AWS CLI
```shell
aws emr add-steps --cluster-id j-xxxxx --steps Type=spark,Name=SparkWordCountApp,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,--num-executors,5,--executor-cores,5,--executor-memory,20g,s3://codelocation/wordcount.py,s3://inputbucket/input.txt,s3://outputbucket/],ActionOnFailure=CONTINUE
```


## Take input from S3 using EMR
To take input from S3 bucket in the spark program while running it on EMR just write path to S3 bucket extensively. Example below illustrates simple program that uses S3 as input source.

```Python
inputs = 's3://taxidata.com/2010/1/'

myParquet = sqlContext.read.parquet(inputs)

pickupDistTip = myParquet.select('driverId','pickupTime')

pickupDistTip.show()
```

In the example above it can be seen that we have specified S3 path from where the data is being fetched.