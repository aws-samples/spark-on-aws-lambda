#!/bin/bash

# Deploy Production Iceberg Lambda Function
set -e

# Configuration
FUNCTION_NAME="spark-iceberg-production"
AWS_REGION="us-east-1"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO_NAME="sparkonlambda-iceberg"
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:latest"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🚀 Deploying Production Iceberg Lambda Function${NC}"
echo "=================================================="

# Get Lambda role ARN from CloudFormation
LAMBDA_ROLE_ARN=$(aws cloudformation describe-stacks \
    --stack-name spark-lambda-iceberg-test \
    --query 'Stacks[0].Outputs[?OutputKey==`LambdaRoleArn`].OutputValue' \
    --output text \
    --region $AWS_REGION)

echo -e "${BLUE}📋 Configuration:${NC}"
echo "  Function Name: $FUNCTION_NAME"
echo "  AWS Region: $AWS_REGION"
echo "  AWS Account: $AWS_ACCOUNT_ID"
echo "  Image URI: $IMAGE_URI"
echo "  Lambda Role: $LAMBDA_ROLE_ARN"
echo ""

# Step 1: Update the Lambda handler in the Docker image
echo -e "${YELLOW}📦 Step 1: Updating Lambda handler in container...${NC}"

# Copy the new handler to the project
cp lambda-deployment/spark-iceberg-reader.py sparkLambdaHandler.py

echo -e "${GREEN}✅ Updated Lambda handler${NC}"

# Step 2: Rebuild and push Docker image
echo -e "${YELLOW}🐳 Step 2: Rebuilding Docker image...${NC}"

# Login to ECR
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

# Build new image
docker build --build-arg FRAMEWORK=ICEBERG -t $ECR_REPO_NAME .

# Tag and push
docker tag $ECR_REPO_NAME:latest $IMAGE_URI
docker push $IMAGE_URI

echo -e "${GREEN}✅ Docker image rebuilt and pushed${NC}"

# Step 3: Create or update Lambda function
echo -e "${YELLOW}⚡ Step 3: Creating/updating Lambda function...${NC}"

# Check if function exists
if aws lambda get-function --function-name $FUNCTION_NAME --region $AWS_REGION > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️ Function exists. Updating...${NC}"
    
    # Update function code
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --image-uri $IMAGE_URI \
        --region $AWS_REGION
    
    # Update function configuration
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --role $LAMBDA_ROLE_ARN \
        --timeout 900 \
        --memory-size 3008 \
        --environment Variables='{"DATABASE_NAME":"iceberg_test_db","TABLE_NAME":"sample_customers","AWS_REGION":"'$AWS_REGION'"}' \
        --region $AWS_REGION
        
else
    echo -e "${YELLOW}🆕 Creating new Lambda function...${NC}"
    
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --role $LAMBDA_ROLE_ARN \
        --code ImageUri=$IMAGE_URI \
        --package-type Image \
        --timeout 900 \
        --memory-size 3008 \
        --environment Variables='{"DATABASE_NAME":"iceberg_test_db","TABLE_NAME":"sample_customers","AWS_REGION":"'$AWS_REGION'"}' \
        --region $AWS_REGION
fi

echo -e "${GREEN}✅ Lambda function deployed: $FUNCTION_NAME${NC}"

# Step 4: Test the function
echo -e "${YELLOW}🧪 Step 4: Testing the Lambda function...${NC}"

# Create test event
cat > test-event.json << EOF
{
    "operation": "read_table",
    "database": "iceberg_test_db",
    "table": "sample_customers",
    "limit": 5,
    "include_analytics": true
}
EOF

# Invoke function
echo -e "${BLUE}📤 Invoking Lambda function...${NC}"
aws lambda invoke \
    --function-name $FUNCTION_NAME \
    --payload file://test-event.json \
    --region $AWS_REGION \
    response.json

# Check response
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Lambda invocation successful${NC}"
    echo -e "${BLUE}📄 Response:${NC}"
    cat response.json | jq '.' 2>/dev/null || cat response.json
    echo ""
else
    echo -e "${RED}❌ Lambda invocation failed${NC}"
fi

# Step 5: Show CloudWatch logs
echo -e "${YELLOW}📋 Step 5: Recent CloudWatch logs:${NC}"
aws logs tail /aws/lambda/$FUNCTION_NAME --since 2m --region $AWS_REGION

echo ""
echo -e "${BLUE}🎉 Production Lambda Deployment Complete!${NC}"
echo "=================================================="
echo ""
echo -e "${YELLOW}📊 Function Details:${NC}"
echo "  • Function Name: $FUNCTION_NAME"
echo "  • Memory: 3008 MB"
echo "  • Timeout: 15 minutes"
echo "  • Runtime: Container (Spark + Iceberg)"
echo ""
echo -e "${YELLOW}🔗 Useful Commands:${NC}"
echo "  • Test function:"
echo "    aws lambda invoke --function-name $FUNCTION_NAME --payload file://test-event.json response.json"
echo ""
echo "  • View logs:"
echo "    aws logs tail /aws/lambda/$FUNCTION_NAME --follow --region $AWS_REGION"
echo ""
echo "  • Update function:"
echo "    ./lambda-deployment/deploy-production-lambda.sh"
echo ""
echo -e "${GREEN}✅ Your Iceberg Lambda function is ready for production use!${NC}"

# Cleanup
rm -f test-event.json