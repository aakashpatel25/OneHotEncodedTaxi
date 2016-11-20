#!/bin/bash

echo "Enter S3 Directory Location: "
read s3_location
echo "Enter Input Path: "
read input_path

s3_location="s3://$s3_location/$input_path/"

for i in {1..12}
do
	loc="$s3_location$i/"
	aws s3 mv $loc $s3_location --recursive
	echo "File Transfer complete for month of $i"
done