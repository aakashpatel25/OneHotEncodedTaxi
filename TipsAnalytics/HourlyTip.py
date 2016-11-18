from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator
from datetime import datetime
from pyspark.sql.functions import hour, avg

inputs = 'dataparquet/'

myParquet = sqlContext.read.parquet(inputs)
tipsData = myParquet.select('carId','driverId','pickupTime','paymentType','tripDistance','fareAmount','tipAmount','totalAmount')

pickupDistTip = tipsData.select('pickupTime','tipAmount')
(pickupDistTip
    .groupBy(hour("pickupTime").alias("hour"))
    .agg(avg("tipAmount").alias("totalTips"))
    .show(1000))