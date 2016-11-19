from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import month,year

conf = SparkConf().setAppName('Sample Program')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

s3 = 's3://taxidata.com/'
year1 = s3+'2010/'
year2 = s3+'2011/'
year3 = s3+'2012/'
year4 = s3+'2013/'

myParquet = sqlContext.read.parquet(year1,year2,year3,year4)

pickupDistTip = myParquet.select('driverId','totalAmount',year('pickupTime').alias('year')\
							,month('pickupTime').alias('month')).cache()

grouppedDF = pickupDistTip.groupBy('year','month','driverId').sum('totalAmount')

averageSalary = (grouppedDF.withColumnRenamed('sum(totalAmount)','Sum')
                           .groupBy('year','month').mean('Sum')).coalesce(1)

averageSalary.write.mode('overwrite').format("com.databricks.spark.csv").save('salary')