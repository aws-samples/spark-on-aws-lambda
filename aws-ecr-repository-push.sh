#!/usr/bin/env bash

#Script used to push the image to ECR repository. Please ensure that the role associated with the instance has access to AWS ECR

echo "Starting the PUSH to AWS ECR...."



if [ $# -eq 0 ]
  then
    echo "Please provide the image name"
fi

Dockerimage=$1

# Fetch the AWS account number
aws_account=$(aws sts get-caller-identity --query Account --output text)

if [ $? -ne 0 ]
then
    exit 255
fi


# Get the region defined in the current configuration (default to us-west-2 if none defined)
aws_region=$(aws configure get region)
aws_region=${region:-us-east-1}
reponame="${aws_account}.dkr.ecr.${aws_region}.amazonaws.com/${Dockerimage}:latest"

# Creates a repo if it does not exist
echo "Create or replace if repo does not exist...."
aws ecr describe-repositories --repository-names "${Dockerimage}" > /dev/null 2>&1

if [ $? -ne 0 ]
then
    aws ecr create-repository --repository-name "${Dockerimage}" > /dev/null
fi

# Get the AWS ECr login 
aws ecr get-login-password --region "${aws_region}" | docker login --username AWS --password-stdin "${aws_account}".dkr.ecr."${aws_region}".amazonaws.com

# Build the docker image and push to ECR
echo "Building the docker image"
docker build  -t ${Dockerimage} .


echo "Tagging the Docker image"
docker tag ${Dockerimage} ${reponame}


echo "Pushing the Docket image to AWS ECR"
docker push ${reponame}