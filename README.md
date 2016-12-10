# One Hot Encoded Taxi: Analyzing Big Data for New York City Taxis
#CMPT - 732 (Programming in Big Data Project)

New York is one of the busiest cities in the world. People in New York have been using taxis increasingly frequently in the past few years. Currently, in New York City, a few million rides are taken every month. There are many hidden patterns that can be extract to analyze the general behavior of the population as well as the New York Taxi services. We are using a dataset of around 1.5 billion trips to analyze New York City's Yellow Taxi data from 2010 to 2013 to mine various metrics such as driver's yearly salary, surcharge prediction, dispute behavior, passenger count, trips taken on range of miles and many more using Amazon Web Services and PySpark. The size of the raw dataset is around 140 GB. Technologies primarily used includes Python, Spark, Parquet, Tableau, AWS EC2, AWS EMR, AWS S3, AWS IAM for performing analysis.

## Taxi Data

In 2013, the New York City Taxi and Limousine commission released data on taxi trips from yellow, green and for-hire vehicles from 2010 through 2013 through a freedom of information law request. You can download this dataset from the Illinois Data Bank (https://doi.org/10.13012/J8PN93H8). This download is a 28 GB zip file, but expands to over 140 GB when recursively unzipped. 

We used Amazon EC2 to download, recursively unzip and transfer this file into an Amazon S3 bucket. 

We have provided the following scripts to assist you in this data transfer in the Scripts folder:
* movefiles.sh: moves files into their proper directory in S3
* uploadToAws.sh: uploads files from local directory into AWS
* recursiveUnzip.sh: recursively unzips all of the files in the specified folder

Additional details for transferring data from EC2 to an S3 bucket can be found in Tutorials/AWSTransferDataFromEC2toS3.pdf. 

In order to process our data more efficiently, we joined the trip and fare data into a single dataset and stored this data in Parquet file format (more efficient for tabular data). There is a PySpark script for data processing located in DataCleaning/process_data.py and a script to run this script on AWS in Scripts/processDataScript.sh. 

After loading and converting the data, the files will be stored in S3 with the following directory structure:
2010/
    1/
    2/
   ...
    12/
2011/
    1/
    2/
    ...
    12/
...

with a folder for each year containing a subfolder for each month. There are 30 parquet files for each month, each containing approximately a day's worth of data. 

If you wish to share your newly created S3 bucket with others, you will need to to edit your bucket policies to allow access. We have included a sample bucket policy in the AWS Bucket Sharing folder.

## Data Analysis

After converting the data into parquet files, it becomes very simple to load the data into a Spark dataframe. Once loaded into a dataframe, the data has this schema:

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
```

<br>

We have provided a number of PySpark scripts that we used to generate summary statistics from the raw data. Each file reads all data from S3 (assuming the directory structure above) and generates CSV files of the summary statistics. Here is a list of the current scripts and the summary statistics they generate (the location of the generated csv files is indicated in brackets):

DisputeAnalysis/dispute_analysis.py: count of disputed trips by hour (DisputeAnalysis/DisputePerHour) and month (DisputeAnalysis/DisputesPerMonth). Total amount of money involved in disputed trips (DisputeAnalysis/DisputedAmount)
PaymentTypeAnalysis/payment_type_analysis.py: count of trips paid by cash or credit card (PaymentTypeAnalysis/cashCCTrips). Amount of money paid in cash or credit card every month (PaymentTypeAnalysis/amountPaidCSHCC)
SalaryAnalysis/AverageSalaryMonthly.py: average salary of each driver per month (MonthYearSalary) and per year (YearlySalary)
SpeedAnalysis/speedAnalysis.py: average speed per month (monthlyAvgSpeed) and per hour (hourlyAvgSpeed). Count of number of trips per average speed rounded to the nearest .1 mph (countBySpeed)
SurchargeAnalysis/Stats.py: count of surcharge amounts per hour (SurchargeAnalysis/HourlySurge) and per week (SurchargeAnalysis/WeekWiseSurge)
TipsAnalysis/tipanalysis.py: average amount in tips per hour (TipAnalysis/hourlytips) and per day (TipAnalysis/dailytips). Average tips earned by driver (TipAnalysis/averageDriverTips)
TripAnalysis/TripCountAnalysis.py: raw count of all trips (TripCount), trips per week (WeekWiseTrips)
TripAnalysis/trips_on_distance.py: count of trips by bucketed ranges (TripDistanceAnalysis/tripsByRange)

Additionally, in SurchargeAnalysis/SurchargeMl.py, you'll find code to build a logistic regression model to predict whether or not a taxi trip will have a surcharge added to the fare. 

In the Data Cleaning folder, we have scripts to assign each trip's coordinates to its corresponding neighborhood within New York City using Python's Shapely library. In order to use this library on EMR, you will need to install Shapely and all of its dependencies on each node in your cluster. We have provided a bootstrap script in DataCleaning/bootstrap.sh to do this; it just needed to be specified as a bootstrap action while spinning up the EMR cluster. Note: this script is not functional in its current form, but can be modified or used as an example if you're looking to do any area-based analysis yourself. The geojson file of NYC neighorhoods is taken from http://catalog.opendata.city/dataset/pediacities-nyc-neighborhoods.

These scripts have the input and output directory paths already specified. You will need to change the name of the S3 directory to match your own.

To run these scripts on EMR, you can run this basic command 
```shell
spark-submit --master yarn filename
```

## Interesting Insights from our Analysis

* On average, a cab driver in NYC makes around 75k/year. 

* The number of disputes has been increasing from 2010 to 2013 in response to the increasing number of trips. However, the total disputes are decreasing as the credit card payments are increasing over the same period.

* A cab driver gets more tips in the afternoon than any other time of the day.

* Long distance trips (above 50 miles) increases during the holidays, surpassing the number of short distance trips (less than 10 miles).

* The number of cash transactions were high in 2010, whereas the number of credit card transactions are constantly increasing into 2013.