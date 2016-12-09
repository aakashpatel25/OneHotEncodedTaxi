'''
    @amoghari

    Surcharge Analysis. The purpose of this analysis is to determine if there will
    be any surcharge associated with a given ride. All the analysis had to be done 
    in such fashion that it could be uploaded to Tableau for the visualization
    purpose. This analysis performes surcharge analysis based on hour, day, month,
    year and weekly basis and stores in ther result.
'''
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import dayofmonth,weekofyear,month,hour,year

conf = SparkConf().setAppName('Surcharge Stats')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

s3 = 's3://taxidata.com/'
year1 = s3+'2010/'
year2 = s3+'2011/'
year3 = s3+'2012/'
year4 = s3+'2013/'
output = "s3://taxidata.com/SurchargeAnalysis/"

data = sqlContext.read.parquet(year1,year2,year3,year4)

data = data.select('pickupTime','surcharge')

# Convert the dataframe into proper format to do analysis.
data = (data.withColumn("hour",hour('pickupTime'))
            .withColumn("day",dayofmonth('pickupTime'))
            .withColumn("month",month('pickupTime'))
            .withColumn("week",weekofyear('pickupTime'))
            .withColumn("year",year('pickupTime')).drop('pickupTime'))

# Create DF to analyse surcharge over four years of data per hour.
hourly = data.select('hour','day','month','year','surcharge')

# Creates analysis of surcharge per hour over the time period of four year.
hourly = data.groupBy('year','month','day','hour').sum('surcharge').coalesce(1)

# Save data back to S3.
(hourly.write.mode('overwrite')
       .format("com.databricks.spark.csv")
       .save(output+'HourlySurge'))

# Create DF to do weekly analysis.
weekly = data.select('week','surcharge')

# Performs the analyis of surcharge on weekly basis. 
weekly = data.groupBy('week').sum('surcharge').coalesce(1)

# Save data to S3.
(weekly.write.mode('overwrite')
       .format("com.databricks.spark.csv")
       .save(output+'WeekWiseSurge'))