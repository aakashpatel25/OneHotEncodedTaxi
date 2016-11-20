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

inputDir = '/home/julie/School/BDProjectData/2013/1'
outDir = '/home/julie/School/BDProjectData/2013/1/withNbhd'
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

dataAll = sqlContext.read.parquet(inputDir + '/*.parquet')
data = dataAll.repartition(100)
dataWithPickupNbhd = data.withColumn('pickupNeighborhood', 
	neighborhoodUDF('pickupLong', 'pickupLat'))
dataWithPickup = dataWithPickupNbhd.withColumn('pickupBorough', boroughUDF('pickupNeighborhood'))
dataWithDropNbhd = dataWithPickup.withColumn('dropNeighborhood', 
	neighborhoodUDF('dropLong', 'dropLat'))
dataWithBoth = dataWithDropNbhd.withColumn('dropBorough', boroughUDF('dropNeighborhood'))
outdata = dataWithBoth.coalesce(30)
#outdata.show()
outdata.write.format('parquet').save(outDir, mode='overwrite')
