# OneHotEncodedTaxi
Big Data Project

### How to read Parquet file

inputs = 'parquetfiledirectory/'

myParquet = sqlContext.read.parquet(inputs)
tipsData = myParquet.select(column names)

tipsData.show()


