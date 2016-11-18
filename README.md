# OneHotEncodedTaxi
Big Data Project

<br><br>

## Data Dictionary

```Python
# Schema
StructType([
    			StructField('carId', LongType(), True),
    			StructField('driverId', LongType(), True),
    			StructField('vendorType', StringType(), False),
    			StructField('pickupTime', TimestampType(), True),
    			StructField('paymentType', StringType(), False),
    			StructField('fareAmount', FloatType(), True),
    			StructField('surcharge', FloatType(), True),
    			StructField('mtaTax', FloatType(), True),
    			StructField('tipAmount', FloatType(), True),
    			StructField('tollAmount', FloatType(), True),
    			StructField('totalAmount', FloatType(), True),
    			StructField('dropoffTime', TimestampType(), False),
    			StructField('passengerCount', IntegerType(), True),
    			StructField('tripTime', IntegerType(), True),
    			StructField('tripDistance', FloatType(), False),
    			StructField('pickupLong', DoubleType(), False),
    			StructField('pickupLat', DoubleType(), Flase),
    			StructField('dropLong', DoubleType(), False),
    			StructField('dropLat', DoubleType(), False),
])

# List of columns in the data dictonary.
columns = ['carId','driverId','vendorType','pickupTime','paymentType','fareAmount','surcharge' \
		   'mtaTax','tipAmount','tollAmount','totalAmount','dropoffTime','passengerCount' \
		   'tripTime','tripDistance','pickupLong','pickupLat','dropLong','dropLat']
```

<br><br>

## Read Parquet file

inputs = 'parquetfiledirectory/'

myParquet = sqlContext.read.parquet(inputs)
tipsData = myParquet.select(column names)

tipsData.show()

<br><br>

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
<br><br>

## Take input from S3 using EMR
To take input from S3 bucket in the spark program while running it on EMR just write path to S3 bucket extensively. Example below illustrates simple program that uses S3 as input source.

```Python
inputs = 's3://taxidata.com/2010/1/'

myParquet = sqlContext.read.parquet(inputs)

pickupDistTip = myParquet.select('driverId','pickupTime')

pickupDistTip.show()
```

In the example above it can be seen that we have specified S3 path from where the data is being fetched.