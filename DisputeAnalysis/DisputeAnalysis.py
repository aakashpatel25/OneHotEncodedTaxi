'''
@author:ruturajp

The purpose of this program is to find the disputed trips from the dataset which can release 
hidden patterns of the disputes happening in the Taxi Trips.
The output filtered data will be imported to Tableau inorder to find details with visualizations.
Program is divided into following parts --

Find most disputed trips on an hour
Find the disputed trips by month, year.
Find the amount of the trips which are disputed.

'''
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator
import re, string
from pyspark.sql.functions import *

conf = SparkConf().setAppName('Trip Dispute Analysis')
sc = SparkContext(conf=conf)

sqlContext= SQLContext(sc)

s3 = 's3://taxidata.com/'
year1 = s3+'2010/'
year2 = s3+'2011/'
year3 = s3+'2012/'
year4 = s3+'2013/'

#parquet file
myParquet = sqlContext.read.parquet(year1,year2,year3,year4)

#Transaction dataframe
transDF = myParquet.select(trim(upper(myParquet.paymentType)).alias('paymentType') \
                    ,year('pickupTime').alias('year') \
                    ,month('pickupTime').alias('month') \
                    ,hour('pickupTime').alias('hour') \
                    ,'totalAmount').where('totalAmount > 0')

#no of disputed trips per hour
disputed_per_hour = (transDF.filter("paymentType = 'DIS'")
     .groupBy('hour','paymentType')
     .agg(count("paymentType").alias("disputedTrips"))
     .orderBy("hour")
     .coalesce(1))

#Output to csv
disputed_per_hour.write.mode('overwrite') \
                        .format("com.databricks.spark.csv") \
                        .save('s3://taxidata.com/DisputeAnalysis/DisputePerHour')

#No of Trips with  every month
disputed_per_month = (transDF.filter("paymentType = 'DIS'")
     .groupBy('year','month','paymentType')
     .agg(count("paymentType").alias("tripsCount"))
     .orderBy("year")
     .coalesce(1))

#Output to csv
disputed_per_month.write.mode('overwrite') \
                        .format("com.databricks.spark.csv") \
                        .save('s3://taxidata.com/DisputeAnalysis/DisputesPerMonth')


#Amount of Money involved in disputed trips
disputed_amounts = (transDF.filter("paymentType = 'DIS'")
     .groupBy('year','month')
     .sum("totalAmount").alias("disputeAmount")
     .orderBy("year")
     .coalesce(1))

#Output to csv
disputed_amounts.write.mode('overwrite') \
                        .format("com.databricks.spark.csv") \
                        .save('s3://taxidata.com/DisputeAnalysis/DisputedAmount')
