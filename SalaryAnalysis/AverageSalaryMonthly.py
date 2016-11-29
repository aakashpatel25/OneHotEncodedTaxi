'''
	@amoghari

	Determin average salary of a driver in a given year, month and over all.
'''
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

inputData = sqlContext.read.parquet(year1,year2,year3,year4)

pickupDistSalary = inputData.select('driverId','totalAmount',\
							 year('pickupTime').alias('year'),\
							 month('pickupTime').alias('month')).cache()

yearMonthGroup = (pickupDistSalary.groupBy('year','month','driverId')
								  .sum('totalAmount'))

averageMonthYearSalary = (yearMonthGroup.withColumnRenamed('sum(totalAmount)','Sum')
                           .groupBy('year','month').mean('Sum')).coalesce(1)

pickupDistSalaryYear = pickupDistSalary.select('driverId','year','totalAmount')

yearGroup = (pickupDistSalaryYear.groupBy('year','driverId')
								 .sum('totalAmount'))

averageYearSalary = (yearGroup.withColumnRenamed('sum(totalAmount)','Sum')
                              .groupBy('year').mean('Sum')).coalesce(1)

averageMonthYearSalary = averageMonthYearSalary.withColumnRenamed('avg(Sum)','Salary')
averageYearSalary = averageYearSalary.withColumnRenamed('avg(Sum)','Salary')

(averageMonthYearSalary.write.mode('overwrite')
					   .format("com.databricks.spark.csv")
					   .save('MonthYearSalary'))

(averageYearSalary.write.mode('overwrite')
				  .format("com.databricks.spark.csv")
				  .save('YearlySalary'))