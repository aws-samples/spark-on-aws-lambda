#!/bin/bash

# S3 paths
SOURCE_S3_PATH="s3://your-source-bucket/path/to/largefile.dat"
DEST_S3_BUCKET="s3://your-destination-bucket/path/to/chunks/"

# Temporary file for download
TEMP_FILE="/tmp/largefile.csv"

# Prefix for the output files
OUTPUT_PREFIX="/tmp/chunk"

# Download the file from S3
aws s3 cp $SOURCE_S3_PATH $TEMP_FILE

# Split the file into 128MB chunks
split -b 128M $TEMP_FILE $OUTPUT_PREFIX

# Upload chunks to the destination S3 bucket
for file in $OUTPUT_PREFIX*; do
    aws s3 cp $file $DEST_S3_BUCKET
done

# Clean up temporary files
rm $TEMP_FILE
rm $OUTPUT_PREFIX*

echo "File has been split into 128MB chunks and uploaded to $DEST_S3_BUCKET"
