#!/bin/bash

data_dir="2013"
hdfs_fare="fare/"
hdfs_trip="trip/"
output="TaxiData/"
year="2013"
echo "Enter Data Directory: "
read data_dir
echo "Enter Year of Data: "
read year

#mkdir $output
output="$output$year"
mkdir $output

for i in {1..12}
do
	echo "Removing fare hdfs directory ... "
	hdfs dfs -rm -r $hdfs_fare
	echo "Removing fare hdfs directory ... "
	hdfs dfs -rm -r $hdfs_trip
	echo "Removing files from HDFS complete ... "
	echo " "
	
	hdfs dfs -mkdir $hdfs_fare
	hdfs dfs -mkdir $hdfs_trip

	trip_file="$data_dir/trip_data_$i.csv"
	fare_file="$data_dir/trip_fare_$i.csv"
	
	echo "Copying Trip file to HDFS ... "
	hdfs dfs -copyFromLocal $trip_file $hdfs_trip
	echo "Copying Fare file to HDFS ... "
	hdfs dfs -copyFromLocal $fare_file $hdfs_fare
	echo "Copying to HDFS Operation complete ... "
	echo " "

	echo "Running Spark Job ... "
	path="$output/$i/"
	spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 --master yarn-clinet process_data.py $hdfs_fare $hdfs_trip $path
	echo "Spark Job Complete ... "
	echo " "

	echo "Copying File to Local Directory ... "
	mkdir $path
	hdfs dfs -copyToLocal $path $path
	echo "Copying File to Local Directory Complete ... "
	echo " "
	echo "Data Processing complete for $year month $i"
	echo " "
done