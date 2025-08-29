#!/bin/bash

# Cleanup Test Environment for Iceberg Glue Catalog Integration

set -e

# Configuration
STACK_NAME="spark-lambda-iceberg-test"
AWS_REGION="us-east-1"
LAMBDA_FUNCTION_NAME="spark-iceberg-test"
ECR_REPO_NAME="sparkonlambda-iceberg"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🧹 Starting Test Environment Cleanup${NC}"
echo "========================================"

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="spark-lambda-iceberg-test-${AWS_ACCOUNT_ID}"

echo -e "${BLUE}📋 Configuration:${NC}"
echo "  Stack Name: $STACK_NAME"
echo "  AWS Region: $AWS_REGION"
echo "  AWS Account: $AWS_ACCOUNT_ID"
echo "  S3 Bucket: $BUCKET_NAME"
echo "  Lambda Function: $LAMBDA_FUNCTION_NAME"
echo "  ECR Repository: $ECR_REPO_NAME"
echo ""

# Step 1: Delete Lambda function
echo -e "${YELLOW}⚡ Step 1: Deleting Lambda function...${NC}"
if aws lambda get-function --function-name $LAMBDA_FUNCTION_NAME --region $AWS_REGION > /dev/null 2>&1; then
    aws lambda delete-function --function-name $LAMBDA_FUNCTION_NAME --region $AWS_REGION
    echo -e "${GREEN}✅ Lambda function deleted${NC}"
else
    echo -e "${YELLOW}⚠️ Lambda function not found${NC}"
fi

# Step 2: Empty and delete S3 bucket contents
echo -e "${YELLOW}🗑️ Step 2: Emptying S3 bucket...${NC}"
if aws s3 ls s3://$BUCKET_NAME --region $AWS_REGION > /dev/null 2>&1; then
    aws s3 rm s3://$BUCKET_NAME --recursive --region $AWS_REGION
    echo -e "${GREEN}✅ S3 bucket emptied${NC}"
else
    echo -e "${YELLOW}⚠️ S3 bucket not found or already empty${NC}"
fi

# Step 3: Delete ECR repository
echo -e "${YELLOW}🐳 Step 3: Deleting ECR repository...${NC}"
if aws ecr describe-repositories --repository-names $ECR_REPO_NAME --region $AWS_REGION > /dev/null 2>&1; then
    aws ecr delete-repository --repository-name $ECR_REPO_NAME --force --region $AWS_REGION
    echo -e "${GREEN}✅ ECR repository deleted${NC}"
else
    echo -e "${YELLOW}⚠️ ECR repository not found${NC}"
fi

# Step 4: Delete CloudFormation stack
echo -e "${YELLOW}📦 Step 4: Deleting CloudFormation stack...${NC}"
if aws cloudformation describe-stacks --stack-name $STACK_NAME --region $AWS_REGION > /dev/null 2>&1; then
    aws cloudformation delete-stack --stack-name $STACK_NAME --region $AWS_REGION
    
    echo -e "${YELLOW}⏳ Waiting for stack deletion to complete...${NC}"
    aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME --region $AWS_REGION
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ CloudFormation stack deleted successfully${NC}"
    else
        echo -e "${RED}❌ CloudFormation stack deletion failed or timed out${NC}"
        echo -e "${YELLOW}⚠️ Please check the AWS Console for stack status${NC}"
    fi
else
    echo -e "${YELLOW}⚠️ CloudFormation stack not found${NC}"
fi

# Step 5: Clean up local Docker images (optional)
echo -e "${YELLOW}🐳 Step 5: Cleaning up local Docker images...${NC}"
if docker images | grep -q $ECR_REPO_NAME; then
    docker rmi $(docker images | grep $ECR_REPO_NAME | awk '{print $3}') 2>/dev/null || true
    echo -e "${GREEN}✅ Local Docker images cleaned up${NC}"
else
    echo -e "${YELLOW}⚠️ No local Docker images found${NC}"
fi

echo ""
echo -e "${BLUE}🎉 Cleanup Complete!${NC}"
echo "===================="
echo ""
echo -e "${GREEN}✅ All test resources have been cleaned up${NC}"
echo ""
echo -e "${YELLOW}📋 Resources Removed:${NC}"
echo "  • Lambda Function: $LAMBDA_FUNCTION_NAME"
echo "  • S3 Bucket: $BUCKET_NAME (emptied)"
echo "  • ECR Repository: $ECR_REPO_NAME"
echo "  • CloudFormation Stack: $STACK_NAME"
echo "  • IAM Role: SparkLambdaIcebergRole-$STACK_NAME"
echo "  • Glue Database: iceberg_test_db"
echo "  • Glue Table: sample_customers"
echo ""
echo -e "${BLUE}💡 Note: The S3 bucket itself will be deleted by CloudFormation${NC}"
echo -e "${BLUE}💡 Note: Check AWS Console to verify all resources are removed${NC}"