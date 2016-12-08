'''
This code takes existing parquet files for taxi data and adds the following columns to the data:
- pickupNeighborhood: neighborhood containing the pickup coordinates
- pickupBorough: borough containing the pickup coordinates
- dropNeighborhood: neighborhood containing the drop coordinates
- dropBorough: borough containing the drop coordinates
The data is then saved in parquet file format. Again, there are 30 files per month for consistency.
A neighborhood is a subset of a borough. Boroughs are more well-known areas of NYC (i.e. Queens, Manhattan).
Required setup:
- sudo apt-get install libgeos-dev
- pip install shapely
Note: this takes a _long_ time. For a single parquet file on my laptop, it took about 20 minutes. 
'''

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import json
from shapely.geometry import shape, Point

conf = SparkConf().setAppName('set borough')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

s3 = 's3://taxidata.com/'
year1 = s3+'2010/'
year2 = s3+'2011/'
year3 = s3+'2012/'
year4 = s3+'2013/'

outDir = 's3://taxidata.com/data/2010/'
geojsonFile = 'nynta.geojson'

nbhData = []
neighborhoodBoroughDict = {'None' : 'None'}
with open(geojsonFile) as f:
	jsonData = json.load(f)
for feature in jsonData['features']:
	bShape = shape(feature['geometry'])
	neighborhood = feature['properties']['neighborhood']
	neighborhoodBoroughDict[neighborhood] = feature['properties']['borough']
	nbhData.append((bShape, neighborhood))

def findNeighborhood(lon, lat):
	point = Point([lon, lat])
	neighborhood = 'None'
	for (bshape, nname) in nbhData:
		if bshape.contains(point):
			neighborhood = nname
			break
	return neighborhood

def findBorough(neighborhood):
	return neighborhoodBoroughDict[neighborhood]

neighborhoodUDF = udf(findNeighborhood, StringType())
boroughUDF = udf(findBorough, StringType())

data = sqlContext.read.parquet(year1,year2,year3,year4)
dataWithPickupNbhd = data.withColumn('pickupNeighborhood', 
	neighborhoodUDF('pickupLong', 'pickupLat'))
print 'Loaded df....'
dataWithPickup = dataWithPickupNbhd.withColumn('pickupBorough', boroughUDF('pickupNeighborhood'))
print 'Added pickup location'
dataWithDropNbhd = dataWithPickup.withColumn('dropNeighborhood', 
	neighborhoodUDF('dropLong', 'dropLat'))
print 'Added dropoff location'
dataWithBoth = dataWithDropNbhd.withColumn('dropBorough', boroughUDF('dropNeighborhood'))
#outdata.show()
print 'Will Strat saving now....'
dataWithBoth.write.format('parquet').save(outDir, mode='overwrite')
