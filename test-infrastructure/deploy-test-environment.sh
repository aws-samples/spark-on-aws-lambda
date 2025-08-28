#!/bin/bash

# Deploy Test Environment for Iceberg Glue Catalog Integration
# This script sets up the complete test environment

set -e

# Configuration
STACK_NAME="spark-lambda-iceberg-test"
AWS_REGION="us-east-1"
BUCKET_PREFIX="spark-lambda-iceberg-test"
DATABASE_NAME="iceberg_test_db"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting Iceberg Test Environment Deployment${NC}"
echo "=================================================="

# Check if AWS CLI is configured
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo -e "${RED}❌ AWS CLI not configured. Please run 'aws configure' first.${NC}"
    exit 1
fi

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="${BUCKET_PREFIX}-${AWS_ACCOUNT_ID}"

echo -e "${BLUE}📋 Configuration:${NC}"
echo "  Stack Name: $STACK_NAME"
echo "  AWS Region: $AWS_REGION"
echo "  AWS Account: $AWS_ACCOUNT_ID"
echo "  S3 Bucket: $BUCKET_NAME"
echo "  Database: $DATABASE_NAME"
echo ""

# Step 1: Deploy CloudFormation stack
echo -e "${YELLOW}📦 Step 1: Deploying CloudFormation stack...${NC}"
aws cloudformation deploy \
    --template-file test-infrastructure/iceberg-test-setup.yaml \
    --stack-name $STACK_NAME \
    --parameter-overrides \
        BucketName=$BUCKET_PREFIX \
        DatabaseName=$DATABASE_NAME \
    --capabilities CAPABILITY_NAMED_IAM \
    --region $AWS_REGION

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ CloudFormation stack deployed successfully${NC}"
else
    echo -e "${RED}❌ CloudFormation deployment failed${NC}"
    exit 1
fi

# Step 2: Upload test scripts to S3
echo -e "${YELLOW}📤 Step 2: Uploading test scripts to S3...${NC}"

# Create scripts directory in S3
aws s3 cp spark-scripts/test-iceberg-integration.py s3://$BUCKET_NAME/scripts/ --region $AWS_REGION
aws s3 cp spark-scripts/simple-iceberg-reader.py s3://$BUCKET_NAME/scripts/ --region $AWS_REGION

echo -e "${GREEN}✅ Test scripts uploaded to S3${NC}"

# Step 3: Build and push Docker image (if ECR repo exists)
echo -e "${YELLOW}🐳 Step 3: Checking for Docker image...${NC}"

# Check if ECR repository exists
ECR_REPO_NAME="sparkonlambda-iceberg"
if aws ecr describe-repositories --repository-names $ECR_REPO_NAME --region $AWS_REGION > /dev/null 2>&1; then
    echo -e "${GREEN}✅ ECR repository exists: $ECR_REPO_NAME${NC}"
    
    # Get ECR login
    aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
    
    # Build and push image
    echo -e "${YELLOW}🔨 Building Docker image with Iceberg support...${NC}"
    docker build --build-arg FRAMEWORK=ICEBERG -t $ECR_REPO_NAME .
    
    docker tag $ECR_REPO_NAME:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$ECR_REPO_NAME:latest
    docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$ECR_REPO_NAME:latest
    
    echo -e "${GREEN}✅ Docker image built and pushed${NC}"
else
    echo -e "${YELLOW}⚠️ ECR repository not found. Creating it...${NC}"
    aws ecr create-repository --repository-name $ECR_REPO_NAME --region $AWS_REGION
    
    # Get ECR login
    aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
    
    # Build and push image
    echo -e "${YELLOW}🔨 Building Docker image with Iceberg support...${NC}"
    docker build --build-arg FRAMEWORK=ICEBERG -t $ECR_REPO_NAME .
    
    docker tag $ECR_REPO_NAME:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$ECR_REPO_NAME:latest
    docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$ECR_REPO_NAME:latest
    
    echo -e "${GREEN}✅ ECR repository created and Docker image pushed${NC}"
fi

# Step 4: Get outputs from CloudFormation
echo -e "${YELLOW}📋 Step 4: Getting CloudFormation outputs...${NC}"

LAMBDA_ROLE_ARN=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs[?OutputKey==`LambdaRoleArn`].OutputValue' \
    --output text \
    --region $AWS_REGION)

echo -e "${GREEN}✅ Lambda Role ARN: $LAMBDA_ROLE_ARN${NC}"

# Step 5: Create Lambda function
echo -e "${YELLOW}⚡ Step 5: Creating Lambda function...${NC}"

LAMBDA_FUNCTION_NAME="spark-iceberg-test"
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$ECR_REPO_NAME:latest"

# Check if Lambda function exists
if aws lambda get-function --function-name $LAMBDA_FUNCTION_NAME --region $AWS_REGION > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️ Lambda function exists. Updating...${NC}"
    aws lambda update-function-code \
        --function-name $LAMBDA_FUNCTION_NAME \
        --image-uri $IMAGE_URI \
        --region $AWS_REGION
    
    aws lambda update-function-configuration \
        --function-name $LAMBDA_FUNCTION_NAME \
        --role $LAMBDA_ROLE_ARN \
        --timeout 900 \
        --memory-size 3008 \
        --environment Variables="{
            SCRIPT_BUCKET=$BUCKET_NAME,
            SPARK_SCRIPT=test-iceberg-integration.py,
            DATABASE_NAME=$DATABASE_NAME,
            TABLE_NAME=sample_customers,
            AWS_REGION=$AWS_REGION
        }" \
        --region $AWS_REGION
else
    echo -e "${YELLOW}🆕 Creating new Lambda function...${NC}"
    aws lambda create-function \
        --function-name $LAMBDA_FUNCTION_NAME \
        --role $LAMBDA_ROLE_ARN \
        --code ImageUri=$IMAGE_URI \
        --package-type Image \
        --timeout 900 \
        --memory-size 3008 \
        --environment Variables="{
            SCRIPT_BUCKET=$BUCKET_NAME,
            SPARK_SCRIPT=test-iceberg-integration.py,
            DATABASE_NAME=$DATABASE_NAME,
            TABLE_NAME=sample_customers,
            AWS_REGION=$AWS_REGION
        }" \
        --region $AWS_REGION
fi

echo -e "${GREEN}✅ Lambda function created/updated: $LAMBDA_FUNCTION_NAME${NC}"

# Step 6: Display next steps
echo ""
echo -e "${BLUE}🎉 Test Environment Deployed Successfully!${NC}"
echo "=================================================="
echo ""
echo -e "${YELLOW}📋 Next Steps:${NC}"
echo ""
echo "1. Create sample Iceberg table (run on EC2 or local machine with Spark):"
echo "   python test-infrastructure/create-sample-iceberg-table.py $BUCKET_NAME $DATABASE_NAME"
echo ""
echo "2. Test the Lambda function:"
echo "   aws lambda invoke \\"
echo "     --function-name $LAMBDA_FUNCTION_NAME \\"
echo "     --payload '{\"DATABASE_NAME\":\"$DATABASE_NAME\",\"TABLE_NAME\":\"sample_customers\",\"TEST_TYPE\":\"comprehensive\"}' \\"
echo "     --region $AWS_REGION \\"
echo "     response.json"
echo ""
echo "3. View the results:"
echo "   cat response.json"
echo ""
echo -e "${YELLOW}📊 Resources Created:${NC}"
echo "  • S3 Bucket: $BUCKET_NAME"
echo "  • Glue Database: $DATABASE_NAME"
echo "  • Lambda Function: $LAMBDA_FUNCTION_NAME"
echo "  • ECR Repository: $ECR_REPO_NAME"
echo "  • IAM Role: SparkLambdaIcebergRole-$STACK_NAME"
echo ""
echo -e "${YELLOW}🔗 Useful Commands:${NC}"
echo "  • View CloudWatch logs:"
echo "    aws logs tail /aws/lambda/$LAMBDA_FUNCTION_NAME --follow --region $AWS_REGION"
echo ""
echo "  • Clean up resources:"
echo "    ./test-infrastructure/cleanup-test-environment.sh"
echo ""
echo -e "${GREEN}✅ Deployment Complete!${NC}"