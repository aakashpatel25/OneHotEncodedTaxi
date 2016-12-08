# OneHotEncodedTaxi
CMPT - 732 (Programming in Big Data Project)

New York is one of the busiest cities in the world. People in New York tend to use Taxi alot since last few years. Currently, in New York City, there are few million rides been taken by people in a month. There are many hidden pattern which can be used to analyze the general behavior of the population as well as the New York Taxi services. We are using a dataset of around 1.5 Billion trips to analyze New York City's Yellow Taxi data from 2010 to 2013 to determine various patterns such as driver's yearly salary, surcharge prediction, disputes behavior, passenger count, trip counts and many more using Amazon Web Services. Size of the dataset was 140 GB. Used Python, Spark, Parquet, Tableau, AWS EC2, AWS EMR, AWS S3, AWS IAM for performing analysis.

<br>

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
    			StructField('dropLong', DoubleType(), False),Curr
    			StructField('dropLat', DoubleType(), False),
])

# List of columns in the data dictonary.
columns = ['carId','driverId','vendorType','pickupTime','paymentType','fareAmount','surcharge' \
		   'mtaTax','tipAmount','tollAmount','totalAmount','dropoffTime','passengerCount' \
		   'tripTime','tripDistance','pickupLong','pickupLat','dropLong','dropLat']
```

<br>

## Read Parquet file

inputs = 'parquetfiledirectory/'

myParquet = sqlContext.read.parquet(inputs)
tipsData = myParquet.select(column names)

tipsData.show()

<br>

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

<br>

## Take input from S3 using EMR
To take input from S3 bucket in the spark program while running it on EMR just write path to S3 bucket extensively. Example below illustrates simple program that uses S3 as input source.

```Python
inputs = 's3://taxidata.com/2010/1/'

myParquet = sqlContext.read.parquet(inputs)

pickupDistTip = myParquet.select('driverId','pickupTime')

pickupDistTip.show()
```

In the example above it can be seen that we have specified S3 path from where the data is being fetched.