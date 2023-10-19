<h1><p><u>Contents</u></p></h1>

### Please refer the Wikipage for latest updates. https://github.com/aws-samples/spark-on-aws-lambda/wiki
### Disclaimer: At present AWS team is not planning to support the Github. The support model is open-source MIT-0 license.

README.MD will be updated soon by the contributors.

### Introduction
Apache Spark on AWS Lambda(SoAL) framework is a standalone installation of Spark running on AWS Lambda. The Spark is packaged in a Docker container, and AWS Lambda is used to execute the Spark image. The Spark Image pulls in a PySpark script.  When processing smaller files under 500 MB in size per payload, some of cluster based services incurs JVM startup cost. This container-based strategy lowers overhead costs associated with spinning up numerous nodes while processing the data on a single node. Use SoAL framework for event-based pipelines with smaller files if you're seeking for a less expensive choice, according to customers. 

### Architecture

The Spark on AWS Lambda(SoAL) framework feature allows you to run Spark applications on AWS Lambda in a similar way to running Spark on Amazon EMR and EMR serverless. This feature enables you to submit a Spark script stored in an Amazon S3 bucket to run on AWS Lambda, by just adjusting the AWS Lambda Environment variable.

When you submit a Spark script to AWS Lambda, an AWS Lambda function is created for the script, and a container is deployed to run the function. The container contains a version of Spark that is compatible with AWS Lambda, as well as any dependencies that your Spark script requires.

Once the container is deployed on AWS Lambda, it remains the same until the function is updated or deleted. This means that subsequent invocations of the function will use the same AWS Lambda Function, which can help improve performance by reducing the time required to start up and initialize Spark(if the Lambda is warm.

The Spark logs will be part of the AWS Lambda logs stored in AWS Cloudwatch.

![Architecture](https://github.com/aws-samples/spark-on-aws-lambda/blob/main/Architecture-Diagram.jpg)
***

### Current Challenge
At present, on Amazon EMR and AWS Glue, the PySpark script will need to be run on each node after a JVM spin-up, in contrast to other frameworks like Pandas, which do not incur this overhead cost because running on single machine. For small files under 500MB, Pandas outperforms Spark, and Spark takes longer due to the JVM spin-up cost. The ACID frameworks like Apache HUDI and Apache Iceberg are only Spark compatible, and none are Pandas compatible. When files are small and there is a  requirement for the ACID framework on Amazon S3 then SoAL framework shines. SoAL framework will be a great option for these use cases.

1. When utilizing AWS Glue or Amazon EMR, the JVM spin-up cost for Spark is considerable, making it slower and more expensive than Pandas to handle smaller files. Pandas performs better on small files than Spark.  The JVM  Cost is reduced and processing is expedited using this Framework.
2. Event-driven and smaller payloads with less frequency coming from AWS Managed Kafka, and AWS Kinesis triggers are ideal use cases. The framework is cost-effective for less frequent payloads and can load data to Apache HUDI or Iceberg tables on Amazon S3.
3. Batch processing small files saves time and money on in comparison with Amazon EMR( with minimum of 3 nodes). The framework can be hosted on AWS Lambda for 15-minute loads.
4. Run different versions of Spark workloads in parallel. (At present, you need a separate cluster on Amazon EMR and AWS Glue, which means a minimum of 6 nodes).

This AWS sample is intended to show an example of a container-based approach. 



####  DockerFile 
<p>The DockerFile builds the image using an AWS based image for Python 3.8. During the build process, it installs PySpark, copies all the required files, and sets the credentials locally on the container. </p>

#### sparkLambdaHandler.py 
<p>This script is invoked in AWS Lambda when an event is triggered. The script downloads a Spark script from an S3 bucket, sets environment variables for the Spark application, and runs the spark-submit command to execute the Spark script.
Here is a summary of the main steps in the script:
1. The lambda_handler function is the entry point for the Lambda function. It receives an event object and a context object as parameters.
2. The s3_bucket_script and input_script variables are used to specify the Amazon S3 bucket and object key where the Spark script is located.
3. The boto3 module is used to download the Spark script from Amazon S3 to a temporary file on the Lambda function's file system.
4. The os.environ dictionary is used to store any arguments passed via the lambda event.
5. The subprocess.run method is used to execute the spark-submit command, passing in the path to the temporary file where the Spark script was downloaded.The event payload recieved by the lambda is passed onto the spark application via the event arguement.
Overall, this script enables you to execute a Spark script in AWS Lambda by downloading it from an S3 bucket and running it using the spark-submit command. The script can be configured by setting environment variables, such as the PYSPARK_SUBMIT_ARGS variable, to control the behavior of the Spark application. </p>

#### spark-class
<p> spark-class is a script provided by Apache Spark that is used to launch Spark applications on a cluster or local mode. It is typically located in the bin directory of your Spark installation. The spark-class script sets up the classpath, system properties, and environment variables required to launch Spark, and then runs the specified class or script using the java command. It is designed to work with both Scala and Java applications.

The spark-class script is typically invoked whenever you need to launch a Spark application on a cluster. This can include launching Spark applications from the command line, submitting Spark jobs to a cluster using the spark-submit command, or launching Spark shell sessions for Scala, Python, or R.

In this code sample, we are substituting a local shell script for the existing spark-class shell script in order to run spark locally. <br>
**Note :** Updates are required if JAVA_HOME and version have changed.

#### spark-scripts
<p>Spark-scripts folder will contain all the pyspark scripts for various target framework integration like Apache HUDI, Apache Iceberg and Delta lake table. Note : Please specifiy S3a file path for all the input and output locations</p>

##### sample-spark-script-csv-to-hudi-table.py
<p> This script is a PySpark script that can be used to read a CSV file from an S3 location, add a timestamp column to it, and write it to a Hudi table in another Amazon S3 location. The script is designed to be run on AWS Lambda, and it includes configuration settings for running PySpark in a serverless environment. The script can be customized by setting environment variables and modifying the configuration settings to meet the specific needs of your PySpark application.</p> 

##### sample-spark-script-csv-to-iceberg-table.py
<p> This script is a PySpark script that can be used to read a CSV file from an S3 location, add a timestamp column to it, and write it to an Iceberg table in another Amazon S3 location.</p> 


##### sample-spark-script-csv-to-delta-table.py
<p> This script is a PySpark script that can be used to read a CSV file from an S3 location, add a timestamp column to it, and write it to a Delta table in another Amazon S3 location.</p> 

####download_jar.sh
<p>The shell script downloads all the required jar files for the ACID frameworks like Apache HUDI, Apache Iceberg and Apache Delta table. It is based on the FRAMEWORK argument in the docker file while building the image</p>

### <ins>VPC, Roles and Execution</ins>
<p>In this framework, AWS Lambda can be hosted on an AWS VPC. The input file is on Amazon S3, and the corresponding AWS Lambda role should only have access to read the file. Deploy an Amazon S3 endpoint to the VPC so that the AWS Lambda script can access the Amazon S3 location. The AmazonLambdaTaskExecutionRolePolicy is the execution role for EC2 Docker containers, and for Amazon S3 access, attached actions like Amazon S3: Get*, S3: List*, and S3: PutObject Access are available along with the resource name.  </p>


#### <p><u>High level steps to build AWS Lambda using Spark container</u></p>


1. Create a Docker file with an AWS base image, public.ecr.aws/lambda/python:3.8. The Dockerfile has the entrypoint to the Lambda_Hnadler and the command to execute the script when triggered.
2. Locally create a Docker image and container. Use AWS Cloud9, AWS workspace, or a local PC for this step.
3. Create an Amazon ECR Repository and push the container to the repository. Manually upload to the AWS ECR repository or use the shell script aws-ecr-repository-push.sh or SAM template build and upload.
4. Create an AWS Lambda function with the role AmazonLambdaTaskExecutionRolePolicy. Increase the memory and timeout settings to suit the file size. The environmental variable can be set if dynamic input is required.
5. Choose the option to use a container image for AWS Lambda.
6. Create a sample event and trigger the AWS Lambda script.


### Build, test, and deploy containers to the Amazon ECR repository.

Building a docker and pushing the image to the Amazon ECR registry. You can use the aws-ecr-repository-push.sh script or manually following below steps.

Browse to the Docker folder with all the required files. Build the Docker image locally by executing the Dockerfile locally.

#Browse to the local folder and run the docker build along with the desired FRAMEWORK, HUDI, DELTA and ICEBERG
```
docker build --build-arg FRAMEWORK=DELTA -t sparkonlambda .
```

### Run the docker 

#Authenticate the docker CLI with Amazon ECR

```
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <accountnumber>.dkr.ecr.us-east-1.amazonaws.com
```

### Create the Amazon ECR repository using command line

```
aws ecr create-repository --repository-name sparkonlambda --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
```

#Tag the image and push it to AWS ECR repo


```
docker tag  sparkonlambda:latest <accountnumber>.dkr.ecr.us-east-1.amazonaws.com/sparkonlambda:latest
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
### License:

License-https://github.com/aws-samples/spark-on-aws-lambda/blob/main/LICENSE

