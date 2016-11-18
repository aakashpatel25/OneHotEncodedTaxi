from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys

conf = SparkConf().setAppName('Sample Program')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

inputs = 's3://taxidata.com/2010/1/'

myParquet = sqlContext.read.parquet(inputs)

#tipsData = myParquet.select('carId','driverId','pickupTime','paymentType','tripDistance','fareAmount','tipAmount','totalAmount')

pickupDistTip = tipsData.select('driverId','pickupTime')

pickupDistTip.show()