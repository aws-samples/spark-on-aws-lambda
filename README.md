<h1><p><u>Contents</u></p></h1>


### Introduction
Apache Spark on AWS Lambda is a standalone installation of Spark running on AWS Lambda. The Spark is packaged in a Docker container, and AWS Lambda is used to execute the image along with the PySpark script. Currently, heavier engines like Amazon EMR, AWS Glue, or Amazon EMR serverless are required for event driven or streaming smaller files to use Apache Spark. When processing smaller files under 10 MB in size per payload, these engines incur resource overhead costs and operate more slowly(slower than pandas). This container-based strategy lowers overhead costs associated with spinning up numerous nodes while processing the data on a single node. Use Apache Spark on AWS Lambda for event-based pipelines with smaller files if you're seeking for a less expensive choice, according to customers.

### Current Challenge
At present, on Amazon EMR and AWS Glue, the PySpark script will need to be run on each node after a JVM spin-up, in contrast to other frameworks like Pandas, which do not incur this overhead cost. For small files, Pandas outperforms Spark, and Spark takes longer due to the JVM spin-up cost. The ACID frameworks like Apache HUDI and Apache Iceberg are only Spark compatible, and none are Pandas compatible. When files are small and there is a  requirement for the ACID framework on Amazon S3 then Spark on AWS Lambda shines. Spark on AWS Lambda will be the best option for these use scenarios.

1. When utilizing AWS Glue or EMR, the JVM spin-up cost for Spark is considerable, making it slower and more expensive than Pandas to handle smaller files. Pandas performs better on small files than Spark.  The JVM  Costs is reduced and processing is expedited using this Framework.
2. Event-driven and smaller payloads with less frequency coming from AWS Managed Kafka, and Kinesis triggers are ideal use cases. The framework is cost-effective for less frequent payloads and can load data to Apache HUDI or Iceberg tables on Amazon S3.
3. Batch processing small files saves time and money on spark on AWS Lambda in comparison with AWS EMR( with minimum of 3 nodes). The framework can be hosted on AWS Batch for longer-running batch processes or AWS Lambda for 15-minute loads.
4. Run different versions of Spark workloads in parallel. (At present, you need a separate cluster on AWS EMR and Glue, which means a minimum of 6 nodes).

This AWS sample is intended to show an example of a container-based approach. This can be executed on AWS Lambda or AWS Batch for longer-running tasks.

### <ins>**Release**</ins>
**Release 1** of the docker image will include AWS Lambda settings, Hadoop-AWS and aws-sdk-bundle libraries, and standalone Spark. To connect with Amazon S3, the docker container running on AWS Lambda is assisted by the hadoop-aws and aws-sdk-bundle libraries.

####  DockerFile 
<p>The DockerFile builds the image using an AWS based image for Python 3.8. During the build process, it installs PySpark, copies all the required files, and sets the credentials locally on the container. </p>

#### sparkOnAWSLambda.py 
<p>Python code to read the csv file from the S3 location, process the data, and write it to the Amazon S3 location. This piece of code can be updated to add data processing logic and read and write different formats like Apache HUDI, Apache Delta Table, and Apache Iceberg. </p>

#### spark-class
<p>To add the jar files to the java path, use the unix command. <br>
**Note :** Updates are required if JAVA_HOME and version have changed.

### <ins>VPC, Roles and Execution</ins>
<p>In this framework, AWS Lambda can be hosted on an AWS VPC. The input file is on Amazon S3, and the corresponding AWS Lambda role should only have access to read the file. Deploy a Amazon S3 endpoint to the VPC so that the AWS Lambda script can access the Amazon S3 location. The AmazonLambdaTaskExecutionRolePolicy is the execution role for EC2 Docker containers, and for Amazon S3 access, attached actions like Amazon S3: Get*, S3: List*, and S3: PutObject Access are available along with the resource name.  </p>


#### <p><u>High level steps to build AWS Lambda using Spark container</u></p>


1. Create a Docker file with an AWS base image, public.ecr.aws/lambda/python:3.8. The Dockerfile has the entrypoint to the Lambda_Hnadler and the command to execute the script when triggered.
2. Locally create a Docker image and container. Use AWS cloud9, AWS workspace, or a local PC for this step.
3. Create an Amazon ECR Repository and push the container to the repository. Manually upload to the AWS ECR repository or use the script. aws-ecr-repository-push.sh.
4. Create an AWS Lambda function with the role AmazonLambdaTaskExecutionRolePolicy. Increase the memory and timeout settings to suit the file size. The environmental variable can be set if dynamic input is required.
5. Choose the option to use a container image for AWS Lambda.
6. Create a sample event and trigger the AWS Lambda script.


### Build, test, and deploy containers to the Amazon ECR repository.

Building a docker and pushing the image to the Amazon ECR registry. You can use the aws-ecr-repository-push.sh script or manually following below steps.

Browse to the Docker folder with all the required files. Build the Docker image locally by executing the Dockerfile locally.

#Browse to the local folder
```
docker build -t sparkonlambda .
```

### Run the docker 

#Authenticate the docker CLI with Amazon ECR

```
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com</code>
```

### Create the Amazon ECR repository using command line

```
aws ecr create-repository --repository-name sparkonlambda --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
```

#Tag the image and push it to AWS ECR repo


```
docker tag  sparkonlambda:latest 123456789012.dkr.ecr.us-east-1.amazonaws.com/sparkonlambda:latest
```
###  Amazon ECR push to repository

```
docker push <accountnumber>.dkr.ecr.us-east-1.amazonaws.com/sparkonlambda
```


#### Required Permission pushing image in Amazon ECR
Before pushing the Docker image to the repository, ensure that the IAM role permission must allow you to list, view, and push or pull images from only one AWS ECR repository in your AWS account. Below is a custom policy.
<em>**Note** : Access is limited to one repository on the Amazon ECR. The policy key resource tag has the name of the repository of choice.</em>
e.g.
```{
   "Version":"2012-10-17",
   "Statement":[
      {
         "Sid":"ListImagesInRepository",
         "Effect":"Allow",
         "Action":[
            "ecr:ListImages"
         ],
         "Resource":"arn:aws:ecr:us-east-1:123456789012:repository/sparkonlambda"
      },
      {
         "Sid":"GetAuthorizationToken",
         "Effect":"Allow",
         "Action":[
            "ecr:GetAuthorizationToken"
         ],
         "Resource":"*"
      },
      {
         "Sid":"ManageRepositoryContents",
         "Effect":"Allow",
         "Action":[
                "ecr:BatchCheckLayerAvailability",
                "ecr:GetDownloadUrlForLayer",
                "ecr:GetRepositoryPolicy",
                "ecr:DescribeRepositories",
                "ecr:ListImages",
                "ecr:DescribeImages",
                "ecr:BatchGetImage",
                "ecr:InitiateLayerUpload",
                "ecr:UploadLayerPart",
                "ecr:CompleteLayerUpload",
                "ecr:PutImage"
         ],
         "Resource":"arn:aws:ecr:us-east-1:123456789012:repository/sparkonlambda"
      }
   ]
}
```

### <p><u>AWS Lambda task creation</u></p>

* Create a new Lambda function: Navigate to the AWS Lambda service, and create a new function using the "Container Image" blueprint. Select the runtime that your container image is built for, and specify the repository and image name from the previous step.

* Grant permissions to the Lambda function: In the IAM service, attach the "LambdaExecutionRole" to the newly created Lambda function, granting it the necessary permissions to access the container image in ECR and Amazon S3.

* Configure the Lambda function: Set the environment variables, memory size,VPC(if required),enable AWS Cloudwatch and timeout for the AWS Lambda function, and add any necessary triggers. 

* Test the Lambda function: Use the AWS Management Console to test the Lambda function and verify that it's running as expected.



#### Required Permission for creating AWS Lambda function

###  <p><u>AWS Lambda function execution</u></p>

In order to execute the AWS Lambda task, the IAM would require the execution role for the function.

#### Required Permission
The task execution role grants the AWS Lambda permission to make AWS API calls on your behalf. Create a role and attach the following policy called AmazonLambdaTaskExecutionRolePolicy. If the AWS Lambda is deployed on a VPC then Amazon S3 VPC endpoint is required. Based on your use case, you can have an inline custom policy (if required).

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```


Amazon S3 ppolicy for read and write
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListObjectsInBucket",
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": ["arn:aws:s3:::bucket-name"]
        },
        {
            "Sid": "AllObjectActions",
            "Effect": "Allow",
            "Action": "s3:*Object",
            "Resource": ["arn:aws:s3:::bucket-name/*"]
        }
    ]
}
```


Check if the trust policy has the below configuration

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```
Future Release:

At AWS data lab team, we are working to together to add some additional features to this framework. Please let us know if you are interested in contributing.
