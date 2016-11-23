'''
Took 3m41s on 1 month of data (30 parquet files)
'''
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator
from datetime import datetime
from pyspark.sql.functions import month, year, hour, mean
from operator import add

conf = SparkConf().setAppName('speed analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

s3 = 's3://taxidata.com/'
year1 = s3+'2010/'
year2 = s3+'2011/'
year3 = s3+'2012/'
year4 = s3+'2013/'

def parseSpeed(speed):
	'''
	Convert speed to float with 1 decimal place
	If speed is None, set it to -1
	'''
	if speed is not None:
		return float(int(speed * 10)) / 10
	else:
		return float(-1)

myParquet = sqlContext.read.parquet(year1,year2,year3,year4)

timeDist = myParquet.select(year('pickupTime').alias('year'), month('pickupTime').alias('month'), hour('pickupTime').alias('hour'),
	'tripTime', 'tripDistance')

speed = timeDist.withColumn('speed', timeDist['tripDistance']/(timeDist['tripTime']/3600)).cache()

# Metric 1: average speed by month
averageSpeedByMonth = speed.groupBy('year','month').mean('speed').coalesce(1)
    
averageSpeedByMonth.write.mode('overwrite').format("com.databricks.spark.csv").save('monthlyAvgSpeed')

# Metric 2: average speed by hour
averageSpeedByHour = speed.groupBy('hour').mean('speed').coalesce(1)

averageSpeedByHour.write.mode('overwrite').format("com.databricks.spark.csv").save('hourlyAvgSpeed')

# Metric 3: number of trips made at certain speed
# We can use this to see if there are any anomalies or general trends
speedRDD = speed.select('speed').rdd
totalBySpeedRDD = speedRDD.map(lambda s: (parseSpeed(s.speed), 1)).reduceByKey(add).sortByKey()
totalBySpeed = sqlContext.createDataFrame(totalBySpeedRDD, ['speed', 'count']).coalesce(1)

totalBySpeed.write.mode('overwrite').format("com.databricks.spark.csv").save('countBySpeed')