# OneHotEncodedTaxi
CMPT - 732 (Programming in Big Data Project)

New York is one of the busiest cities in the world. People in New York tend to use Taxi alot since last few years. Currently, in New York City, there are few million rides been taken by people in a month. There are many hidden pattern which can be used to analyze the general behavior of the population as well as the New York Taxi services. We are using a dataset of around 1.5 billion trips to analyze New York City's Yellow Taxi data from 2010 to 2013 to determine various patterns such as driver's yearly salary, surcharge prediction, disputes behavior, passenger count, trips taken on range of miles and many more using Amazon Web Services and PySpark. Size of the dataset is 140 GB. Technologies primarily used includes Python, Spark, Parquet, Tableau, AWS EC2, AWS EMR, AWS S3, AWS IAM for performing analysis.

## Major Analysis
- Surcharge Analysis
> The surcharge analysis includes the programs to find the trips over the term of 4 years which are surcharged ( charged extra ) and it appears that almost 50% of the trips in NYC are surcharged. It also includes a ML (Machine Learning) Algorithm which helps to predict if there is a possibility of surcharge in a trip given the time, and the location of the trip.

- Salary Analysis
> The salary analysis includes programs to find the average monthly/yearly salary of a cab driver in NYC. It appears that that on an average a cab driver in NYC makes around 75k/year. 

- Dispute Analysis
> The dispute analysis includes the programs to find the disputed trips over the period of 4 years. It appears that number of disputes has been increased from 2010 to 2013 because the number of trips is constantly growing. However, the total disputes are decreasing as the credit card payments are increasing over the period.

- Speed Analysis
>The speed analysis includes the programs to divide the trips by the speed in miles/hr to find the number of taxi rides in NYC which are actually completed by breaking the speed limit.

- Tips Analysis
>The tips analysis includes the programs to find the average number of tips received by a cab driver in the span of 4 years. It also includes the trend of tips received by a cab driver in a day/month/year. It appears the cab driver gets paid more tip in afternoon than other time of the day.

- Trips Analysis
> The trips analysis includes the programs to find the number of trips completed by a cab driver in a day/week/month/year. The trips analysis also includes the analysis to check the trend of the trips within a range of miles over the year and month. It appears that the long distance trips (above 50 miles) increases in holidays than the number of short distance trips (less than 10 miles).

- Payment Type Analysis
>The payment type analysis includes the program to find the number of trips completed by using the payment type as cash or credit card. It also includes the amount paid via cash/credit card over the span of 4 years. It appears that the number of cash transactions where high in 2010 whereas the number of credit card transactions are constantly increasing by 2013.

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

### Copying data from AWS S3 to local repository
```shell
aws s3 cp s3://taxidata.com/paymentAnalysis /user/home/BigdataProjectDirectory
```


## Take input from S3 using EMR
To take input from S3 bucket in the spark program while running it on EMR just write path to S3 bucket extensively. Example below illustrates simple program that uses S3 as input source.

```Python
inputs = 's3://taxidata.com/2010/1/'

inputParquet = sqlContext.read.parquet(inputs)

pickupDistTip = inputParquet.select('driverId','pickupTime')

pickupDistTip.show()
```

In the example above it can be seen that we have specified S3 path from where the data is being fetched.