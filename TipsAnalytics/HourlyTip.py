from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator
from datetime import datetime
from pyspark.sql.functions import hour, avg



s3 = 's3://taxidata.com/'
year1 = s3+'2010/'
year2 = s3+'2011/'
year3 = s3+'2012/'
year4 = s3+'2013/'

myParquet = sqlContext.read.parquet(year1,year2,year3,year4)

pickupDistTip = myParquet.select('pickupTime','tipAmount')
(pickupDistTip
    .groupBy(hour("pickupTime").alias("hour"))
    .agg(avg("tipAmount").alias("totalTips"))
    
averageSalary.write.mode('overwrite').format("com.databricks.spark.csv").save('hourlytips')