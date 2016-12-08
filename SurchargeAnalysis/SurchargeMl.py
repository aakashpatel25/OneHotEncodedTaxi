'''
    @author: amoghari

    Given a vendor type, pickup time (hour, day, week and month) and pick up location 
    predict if passenger is likely to be charge with surcharge.
'''
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS,LogisticRegressionModel
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import dayofmonth,weekofyear,month,hour

conf = SparkConf().setAppName('Surcharge Prediction')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

s3 = 's3://taxidata.com/'
year1 = s3+'2010/'
year2 = s3+'2011/'
year3 = s3+'2012/'
year4 = s3+'2013/'

data = sqlContext.read.parquet(year1,year2,year3,year4)

data = data.select('vendorType','pickupTime','surcharge','pickupLong','pickupLat')

data = (data.withColumn("hour",hour('pickupTime'))
            .withColumn("day",dayofmonth('pickupTime'))
            .withColumn("month",month('pickupTime'))
            .withColumn("week",weekofyear('pickupTime')))

data = data.drop('pickupTime').rdd

def vendorType(feature):
    if feature[0]=='VTS':
        feature[0]= 1
    else:
        feature[0]= 0
    return feature

def convertSurcharge(sur):
    if sur>0.0:
        return 1.0
    return 0.0

testRDD, trainRDD = data.randomSplit([0.2,0.8],25)

trainRDD = (trainRDD.map(lambda x:(x['surcharge'],[ele for ele in x if x!=x['surcharge']]))
                    .map(lambda (lab,feat):(convertSurcharge(lab),vendorType(feat)))
                    .map(lambda (lab,feat):LabeledPoint(lab,feat)))
     
testRDD = (testRDD.map(lambda x:(x['surcharge'],[ele for ele in x if x!=x['surcharge']]))
                  .map(lambda (lab,feat):(convertSurcharge(lab),vendorType(feat))))

model = LogisticRegressionWithLBFGS.train(trainRDD, iterations=10,numClasses=2)
model.save(sc, "s3://taxidata.com/SurchargeAnalysis/ML/lrm_model.model")

# model= LogisticRegressionModel.load(sc, "lrm_model.model")

predictedRDD = testRDD.map(lambda (lab,feat): (float(model.predict(feat)),lab))

metrics = MulticlassMetrics(predictedRDD)

# def predictAcc(a,b):
#     if a==b:
#         return 1
#     return 0

# total = predictedRDD.map(lambda (a,b):(1,predictAcc(a,b))).reduceByKey(lambda a,b:a+b)
# x = total.collect()[0]
# print x[1]/float(testRDD.count())

precision = metrics.precision()
recall = metrics.recall()
f1Score = metrics.fMeasure()

print f1Score
print recall
print precision