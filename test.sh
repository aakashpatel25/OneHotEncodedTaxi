#!/bin/bash

data_dir="2013"
hdfs_fare="fare/"
hdfs_trip="trip/"
output="TaxiData/"
year="2014"
echo "Enter Data Directory: "
read data_dir
echo "Enter Year of Data: "
read year

echo "Removing fare hdfs directory ... "
hdfs dfs -rm -r $hdfs_fare
echo "Removing fare hdfs directory ... "
hdfs dfs -rm -r $hdfs_trip
echo "Removing files from HDFS complete ... "
echo " "

hdfs dfs -mkdir $hdfs_fare
hdfs dfs -mkdir $hdfs_trip

trip_file="$data_dir/trip_data_2.csv"
fare_file="$data_dir/trip_data_2.csv"

echo "Copying Trip file to HDFS ... "
hdfs dfs -copyFromLocal $trip_file $hdfs_trip
echo "Copying Fare file to HDFS ... "
hdfs dfs -copyFromLocal $fare_file $hdfs_fare
echo "Copying to HDFS Operation complete ... "
echo " "