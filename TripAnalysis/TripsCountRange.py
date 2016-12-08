from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.sql import  Row
from pyspark.sql import SQLContext
import sys, operator
import re, string
from datetime import datetime
from pyspark.sql.functions import *

conf = SparkConf().setAppName('Trips By Distance Analysis')
sc = SparkContext(conf=conf)

sqlContext= SQLContext(sc)

s3 = 's3://taxidata.com/'
year1 = s3+'2010/'
year2 = s3+'2011/'
year3 = s3+'2012/'
year4 = s3+'2013/'

#parquet file
myParquet = sqlContext.read.parquet(year1,year2,year3,year4)

#change trips to a range
def changeToRange(trip):
    if trip > 0.0 and trip <= 10.0: return 10.0
    elif trip > 10.0 and trip <= 20.0: return 20.0
    elif trip > 20.0 and trip <= 30.0: return 30.0
    elif trip > 30.0 and trip <= 50.0: return 50.0
    elif trip > 50.0 and trip <= 70.0: return 70.0
    elif trip > 70.0 and trip <= 100.0: return 100.0
    elif trip <= 0.0: return 0.0
    else: return 200.0

#initiating range
udfChangeToRange = udf(changeToRange, FloatType())

transDF = myParquet.select(trim(upper(myParquet.paymentType)).alias('paymentType'), \
                            year('pickupTime').alias('year') \
                            ,month('pickupTime').alias('month'),\
                            'totalAmount',udfChangeToRange('tripDistance').alias('tripRange')) \
                            .where('totalAmount > 0').cache()

#Cleaning the data converting multi-values to single for labels
newtransDF = (transDF.withColumn('paymentType',
    regexp_replace('paymentType', 'CAS', 'CSH'))
    .withColumn('paymentType',
    regexp_replace('paymentType', 'CRD', 'CRE')))

#Calculating number of trips with range 10,20,30,50,70, 100 and more than 100 as 200
#based on the type of payment {cash,credit}
trips_by_range = (newtransDF
     .groupBy('year','month','paymentType','tripRange')
     .agg(count("tripRange").alias("totalTrips"))
     .where(col("paymentType").isin({"CRE", "CSH"}))
     .where('tripRange > 0.0')
     .sort("month","tripRange").coalesce(1))

#Output to csv
trips_by_range.write.mode('overwrite') \
                        .format("com.databricks.spark.csv") \
                        .save('s3://taxidata.com/TripDistanceAnalysis/tripsByRange')
    

