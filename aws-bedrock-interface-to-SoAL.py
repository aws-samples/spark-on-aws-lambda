import boto3
import json
import logging
from typing import Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class BedrockPySparkAgent:
    def __init__(self, bedrock_client, lambda_client):
        self.bedrock_client = bedrock_client
        self.lambda_client = lambda_client
        self.model_id = "anthropic.claude-v2"  # Or another suitable Bedrock model

    def generate_pyspark_script(self, prompt: str) -> str:
        log.info(f"Generating PySpark script with prompt: {prompt}")
    
        # Define the PySpark session configuration to be included in the LLM prompt
        pyspark_config = f"""
        spark = SparkSession.builder \\
            .appName("Spark-on-AWS-Lambda") \\
            .master("local[1]") \\
            .config("spark.driver.bindAddress", "127.0.0.1") \\
            .config("spark.driver.memory", "5g") \\
            .config("spark.executor.memory", "5g") \\
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
            .config("spark.sql.hive.convertMetastoreParquet", "false") \\
            .enableHiveSupport().getOrCreate()
        """
    
        # Ensure the prompt starts with "Human:" as required
        full_prompt = f'''
    Human: Generate a valid, executable PySpark script that can be executed without errors. 
    Ensure all indentations are correct and the code is cleanly formatted. 
    No explanations, no comments, no prefixes like 'python'. Remove any language-specific code block formatting (such as ```python).
    Do not use the findspark() library in the script. 
    The Spark session should have the following configuration:
    {pyspark_config}
    
    Prompt: {prompt}
    
    Assistant:
        '''
    
        try:
            # Call the Bedrock model to generate the PySpark script
            response = self.bedrock_client.invoke_model(
                modelId=self.model_id,
                contentType="application/json",
                accept="application/json",
                body=json.dumps({
                    "prompt": full_prompt.strip(),  # Strip any leading or trailing whitespace
                    "max_tokens_to_sample": 1000,
                    "temperature": 0.7
                })
            )
    
            # Extract and parse the generated script from the model response
            script = json.loads(response['body'].read())['completion']
            log.info(f"Generated PySpark script:\n{script}")
            return script
    
        except Exception as e:
            log.error(f"Error generating PySpark script: {e}")
            raise



    def execute_pyspark_script(self, script: str) -> Dict[str, Any]:
        log.info(f"Executing PySpark script: {script}...")  # Log first 100 chars of the script
        try:
            log.info("Inside Try")
            
            # Clean up the script before sending
            clean_script = script.strip()  # Remove leading and trailing whitespace
    
            response = self.lambda_client.invoke(
                FunctionName='spark-on-aws-lambda-latest',
                InvocationType='RequestResponse',
                Payload=json.dumps({'script': clean_script})
            )
            
            
            result = json.loads(response['Payload'].read())
            log.info(f"Result of Lambda Execution :  {result}")
            
            log.info("PySpark script executed successfully.")
            return result
        except Exception as e:
            log.error(f"Error executing PySpark script: {e}")
            raise


    def improve_script(self, script: str, error: str) -> str:
        log.info(f"Improving script based on error: {error}")
        print(f"Error encountered: {error}")
        try:
            response = self.bedrock_client.invoke_model(
                modelId=self.model_id,
                contentType="application/json",
                accept="application/json",
                body=json.dumps({
                    "prompt": f"Human: The following PySpark script resulted in an error. Please fix the script. Provide only the executable code, no explanations, no comments, no prefixes like 'python', Remove any language-specific code block formatting (such as ```python) from the following Python code and return the plain code without syntax highlighting.. Please fix the script:\n\nScript:\n{script}\n\nError:\n{error}\n\nProvide only the corrected code, no python prefix or explanations.Remove the string 'python' frm the begining of the script\n\nAssistant: Here's the corrected PySpark script:\n\n",
                    "max_tokens_to_sample": 1000,
                    "temperature": 0.7,
                })
            )
            corrected_script = json.loads(response['body'].read())['completion']
            print("Second attempt with improved code:")
            print(corrected_script)
            log.info("Script improvement successful.")
            return corrected_script
        except Exception as e:
            log.error(f"Error improving PySpark script: {e}")
            raise

    def run_pyspark_task(self, task_description: str, max_attempts: int = 5) -> Dict[str, Any]:
        script = self.generate_pyspark_script(task_description)
        
        for attempt in range(max_attempts):
            log.info(f"Attempt {attempt + 1} to execute task: {task_description}")
            try:
                result = self.execute_pyspark_script(script)
                status_code = result.get('statusCode')
                
                if status_code != 200:
                    # Extract error from the result body, which is a stringified JSON
                    try:
                        error_body = json.loads(result['body'])  # First decode the body
                        error_message = error_body if isinstance(error_body, str) else error_body.get('Error', 'Unknown error')
                    except json.JSONDecodeError:
                        error_message = result['body']  # Use raw body if JSON decoding fails
                    
                    log.warning(f"Attempt {attempt + 1} failed with error: {error_message}")
                else:
                    log.info(f"Task completed successfully on attempt {attempt + 1}")
                    return result
            except Exception as e:
                error_message = str(e)
                log.warning(f"Attempt {attempt + 1} failed with exception: {error_message}")
            
            # If we reach here, it means the attempt failed
            if attempt < max_attempts - 1:
                script = self.improve_script(script, error_message)
                log.info("Attempting again with improved code:")
                log.info(script)  # Log the first 100 characters of the improved script
            else:
                log.error(f"Max attempts reached. Failed to complete task after {max_attempts} attempts.")
                return {'error': 'Max attempts reached'}
        
        log.error(f"Task failed after {max_attempts} attempts.")
        return {'error': 'Max attempts reached'}

# Usage example
bedrock_client = boto3.client('bedrock-runtime')
lambda_client = boto3.client('lambda')

agent = BedrockPySparkAgent(bedrock_client, lambda_client)
result = agent.run_pyspark_task("""
generate pyspark script show this data ain dataframe[
  {
    "name": "John",
    "age": 34,
    "sex": "Male"
  },
  {
    "name": "Mary",
    "age": 29,
    "sex": "Female"
  },
  {
    "name": "Michael",
    "age": 40,
    "sex": "Male"
  },
  {
    "name": "Sarah",
    "age": 22,
    "sex": "Female"
  },
  {
    "name": "David",
    "age": 31,
    "sex": "Male"
  },
  {
    "name": "Emma",
    "age": 27,
    "sex": "Female"
  },
  {
    "name": "Daniel",
    "age": 36,
    "sex": "Male"
  },
  {
    "name": "Olivia",
    "age": 24,
    "sex": "Female"
  },
  {
    "name": "Robert",
    "age": 38,
    "sex": "Male"
  },
  {
    "name": "Sophia",
    "age": 30,
    "sex": "Female"
  }
]
""")

print(result)
