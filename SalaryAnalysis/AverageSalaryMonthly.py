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

# Convert datetime format into year and month column. Create new df for analysis.
pickupDistSalary = inputData.select('driverId','totalAmount',\
							 year('pickupTime').alias('year'),\
							 month('pickupTime').alias('month')).cache()

# Sum every driver's salary over year and month.
yearMonthGroup = (pickupDistSalary.groupBy('year','month','driverId')
								  .sum('totalAmount'))

# Performans monthly salary analysis by averaging by the number of drivers. 
averageMonthYearSalary = (yearMonthGroup.withColumnRenamed('sum(totalAmount)','Sum')
                           .groupBy('year','month').mean('Sum')).coalesce(1)

# Creates new dataframe to do salary analysis on yearly basis.
pickupDistSalaryYear = pickupDistSalary.select('driverId','year','totalAmount')

# Sum every driver's yearly salary.
yearGroup = (pickupDistSalaryYear.groupBy('year','driverId')
								 .sum('totalAmount'))

# Average the yearly salary of a driver by taking average of all driver's salary.
averageYearSalary = (yearGroup.withColumnRenamed('sum(totalAmount)','Sum')
                              .groupBy('year').mean('Sum')).coalesce(1)

# Rename the columns to save it into the CSV file.
averageMonthYearSalary = averageMonthYearSalary.withColumnRenamed('avg(Sum)','Salary')
averageYearSalary = averageYearSalary.withColumnRenamed('avg(Sum)','Salary')

(averageMonthYearSalary.write.mode('overwrite')
					   .format("com.databricks.spark.csv")
					   .save('MonthYearSalary'))

(averageYearSalary.write.mode('overwrite')
				  .format("com.databricks.spark.csv")
				  .save('YearlySalary'))