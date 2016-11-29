'''
    @amoghari

    Surcharge Analysis.
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

data = (data.withColumn("hour",hour('pickupTime'))
            .withColumn("day",dayofmonth('pickupTime'))
            .withColumn("month",month('pickupTime'))
            .withColumn("week",weekofyear('pickupTime'))
            .withColumn("year",year('pickupTime')).drop('pickupTime'))

hourly = data.select('hour','day','month','year','surcharge')

hourly = data.groupBy('year','month','day','hour').sum('surcharge')

(hourly.write.mode('overwrite')
       .format("com.databricks.spark.csv")
       .save(output+'HourlySurge'))

weekly = data.select('week','surcharge')

weekly = data.groupBy('week').sum('surcharge')

(weekly.write.mode('overwrite')
       .format("com.databricks.spark.csv")
       .save(output+'WeekWiseSurge'))