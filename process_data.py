from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, FloatType, IntegerType, DoubleType
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import sum
import sys, datetime

fair_inputs = sys.argv[1]
trip_inputs = sys.argv[2]
output = sys.argv[3]

conf = SparkConf().setAppName('Taxi Data Processing')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

fare_schema = StructType([
    			StructField('carId', LongType(), True),
    			StructField('driverId', LongType(), True),
    			StructField('vendorType', StringType(), True),
    			StructField('pickupTime', TimestampType(), True),
    			StructField('paymentType', StringType(), True),
    			StructField('fareAmount', FloatType(), True),
    			StructField('surcharge', FloatType(), True),
    			StructField('mtaTax', FloatType(), True),
    			StructField('tipAmount', FloatType(), True),
    			StructField('tollAmount', FloatType(), True),
    			StructField('totalAmount', FloatType(), True),
])

trip_schema = StructType([
    			StructField('carId', LongType(), False),
    			StructField('driverId', LongType(), False),
    			StructField('vendorType', StringType(), False),
    			StructField('rateCode', IntegerType(), False),
    			StructField('storeFlag', StringType(), False),
    			StructField('pickupTime', TimestampType(), False),
    			StructField('dropoffTime', TimestampType(), False),
    			StructField('passengerCount', IntegerType(), False),
    			StructField('tripTime', IntegerType(), False),
    			StructField('tripDistance', FloatType(), False),
    			StructField('pickupLong', DoubleType(), False),
    			StructField('pickupLat', DoubleType(), False),
    			StructField('dropLong', DoubleType(), False),
    			StructField('dropLat', DoubleType(), False),
])

fairDF = (sqlContext.read.format('com.databricks.spark.csv')
                    .options(header='true',inferschema='false')
                    .schema(fare_schema)
                    .load(fair_inputs).repartition(75))

tripDF = (sqlContext.read.format('com.databricks.spark.csv')
                    .options(header='true',mode="DROPMALFORMED",inferschema='false')
                    .schema(trip_schema)
                    .load(trip_inputs)).drop('storeFlag').repartition(75)

#joindData = sqlContext.read.parquet(output)

#print ''
#print ''
#print ''
#print ''
#print ''
#print ''
#print ''
#print ''
#print '======================================================================================================='
#print 'Fair DF'
#print fairDF.count()
#print ''
#print ''
#print ''
#print ''
#print 'Trip DF'
#print tripDF.count()
#print ''
#print ''
#print ''
#print ''
#print 'Joined DF'
#print joindData.count()
#print '======================================================================================================='
#print ''
#print ''
#print ''
#print ''
#print ''
#print ''
#print ''
#print ''

tripData = (tripDF.join(fairDF, (tripDF['carId'] == fairDF['carId']) & \
								(tripDF['driverId'] == fairDF['driverId']) & \
								(tripDF['pickupTime'] == fairDF['pickupTime']) & \
								(tripDF['vendorType'] == fairDF['vendorType']),
								'inner')
					.drop(fairDF['carId'])
					.drop(fairDF['driverId'])
					.drop(fairDF['vendorType'])
					.drop(fairDF['pickupTime'])).coalesce(30)

tripData.write.format('parquet').save(output,mode='overwrite')