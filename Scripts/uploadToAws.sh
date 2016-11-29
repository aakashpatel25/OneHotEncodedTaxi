#!/bin/bash

#Upload to AWS S3 bucket from local directory.

echo "Enter S3 Directory Location: "
read s3_location
echo "Enter Input Path: "
read input_path

s3_location="s3://$s3_location/$input_path/"

for i in {1..12}
do
	loc="$s3_location$i/"
	directory="$input_path/$i"
	com="$directory/_comm*"
	met="$directory/_meta*"
	aws s3 cp $com $loc
	aws s3 cp $met $loc
	for j in {0..29}
	do
		if [ "$j" -lt "10" ];
		then
			filename="$directory/part-r-0000$j*"
			aws s3 cp $filename $loc
		else
			filename="$directory/part-r-000$j*"
			aws s3 cp $filename $loc
		fi
	done
done