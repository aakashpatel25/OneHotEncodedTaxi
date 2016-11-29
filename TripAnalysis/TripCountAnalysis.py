'''
	@amoghari

	To obtain statistics regarding number of trips that are being took over the four
	year in the city of New York. 

	Pupose of this program is to get trip stats about hourly, daily, monthly, yearly 
	and weekly. 

	There are other analysis as well which are to be considered, but later was
	discarded to create one single file that could then be used in Tableau to plot
	various visualizations. 
'''
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import month,year,hour,dayofmonth,dayofyear,weekofyear

conf = SparkConf().setAppName('Sample Program')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

s3 = 's3://taxidata.com/'
year1 = s3+'2010/'
year2 = s3+'2011/'
year3 = s3+'2012/'
year4 = s3+'2013/'
output = s3+'TripCount/'

taxidata = sqlContext.read.parquet(year1,year2,year3,year4)

tripCount = (taxidata.select(year('pickupTime').alias('year'),\
							 month('pickupTime').alias('month'),\
							 dayofmonth('pickupTime').alias('day'),\
							 hour('pickupTime').alias('hour'),
							 'driverId'))

tripCount = tripCount.groupBy('year','month','day','hour').count().coalesce(1)

(tripCount.write.mode('overwrite')
				.format("com.databricks.spark.csv")
				.save(output+'TripCount'))

tripCountWeek = (taxidata.select(weekofyear('pickupTime').alias('week'),\
							    'driverId'))

tripCountWeek = tripCountWeek.groupBy('week').count().coalesce(1)

(tripCountWeek.write.mode('overwrite')
					.format("com.databricks.spark.csv")
				 	.save(output+'WeekWiseTrips'))

# tripCountHour = (taxidata.select(hour('pickupTime').alias('hour'),\
# 							    'driverId'))

# tripCountHour = tripCountHour.groupBy('hour').count().coalesce(1)

# tripCountMonth = (taxidata.select(month('pickupTime').alias('month'),\
# 							    'driverId'))

# tripCountMonth = tripCountMonth.groupBy('month').count().coalesce(1)

# tripCountYear = (taxidata.select(year('pickupTime').alias('year'),\
# 							    'driverId'))

# tripCountYear = tripCountYear.groupBy('year').count().coalesce(1)

# tripCountDayMonth = (taxidata.select(dayofmonth('pickupTime').alias('day'),\
# 							    'driverId'))

# tripCountDayMonth = tripCountDayMonth.groupBy('day').count().coalesce(1)

# tripCountMonthYear = (taxidata.select(year('pickupTime').alias('year'),\
# 							    month('pickupTime').alias('month'),\
# 							    'driverId'))

# tripCountMonthYear = tripCountMonthYear.groupBy('year','month').count().coalesce(1)

# tripCountWeek = (taxidata.select(weekofyear('pickupTime').alias('week'),\
# 							    'driverId'))

# tripCountWeek = tripCountWeek.groupBy('week').count().coalesce(1)

# tripCountDayMonthYear = (taxidata.select(year('pickupTime').alias('year'),\
# 							    month('pickupTime').alias('month'),\
# 							    dayofyear('pickupTime').alias('day'),\
# 							    'driverId'))

# tripCountDayMonthYear = tripCountMonthYear.groupBy('year','month','day').count().coalesce(1)

# (tripCountHour.write.mode('overwrite')
# 					  .format("com.databricks.spark.csv")
# 					  .save(output+'HourWiseTrips'))

# (tripCountMonth.write.mode('overwrite')
# 					 .format("com.databricks.spark.csv")
# 					 .save(output+'MonthWiseTrips'))

# (tripCountYear.write.mode('overwrite')
# 					.format("com.databricks.spark.csv")
# 					.save(output+'YearWiseTrips'))

# (tripCountDayMonth.write.mode('overwrite')
# 				  .format("com.databricks.spark.csv")
# 				  .save(output+'DayWiseTrips'))

# (tripCountMonthYear.write.mode('overwrite')
# 				   .format("com.databricks.spark.csv")
# 				   .save(output+'MonthYearTrips'))

# (tripCountWeek.write.mode('overwrite')
# 					.format("com.databricks.spark.csv")
# 				 	.save(output+'WeekWiseTrips'))

# (tripCountDayMonthYear.write.mode('overwrite')
# 					  .format("com.databricks.spark.csv")
# 				 	  .save(output+'DayMonthYearWiseTrips'))
