import sys
import logging
import ast
import subprocess
import re
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def compile_pyspark_script(script_content):
    """
    Compiles a PySpark script to check for syntax errors using the ast module.
    """
    try:
        logger.info('Compiling the PySpark script for syntax check...')
        
        # Parse the script using ast to check for syntax errors
        ast.parse(script_content)
        
    except SyntaxError as e:
        logger.error(f'Syntax error in the PySpark script at line {e.lineno}: {e.msg}')
        raise e
    except Exception as e:
        logger.error(f'Error during the compilation of PySpark script: {e}')
        raise e
    else:
        logger.info('PySpark script compiled successfully without syntax errors.')

def search_for_errors(output, error_output):
    """
    Searches for relevant error keywords in both output and error_output.
    Captures from "Traceback" to the final error.
    Returns a filtered list of errors if found.
    """
    error_patterns = [
        r"Traceback",
        r"AttributeError",
        r"error",
        r"Exception",
        r"FileNotFound",
    ]

    combined_logs = output + error_output
    relevant_errors = []
    capture_flag = False

    for line in combined_logs.splitlines():
        if "Traceback" in line:
            capture_flag = True

        if capture_flag:
            relevant_errors.append(line)

        if any(re.search(pattern, line, re.IGNORECASE) for pattern in error_patterns):
            capture_flag = True

    if relevant_errors:
        return "\n".join(relevant_errors)
    else:
        return None

def run_spark_job(script_content):
    """
    Runs the provided Spark script as a subprocess and captures logs and errors.
    Executes Spark job locally using all available cores with local[*].
    Returns either the output of the Spark job or a success message if no errors occur.
    Filters and returns relevant error messages in case of failure.
    """
    output = ""
    error_output = ""
    MAX_LOG_LENGTH = 5000

    # Write the script content to a temporary file
    with open("/tmp/spark_script.py", "w") as f:
        f.write(script_content)

    spark_submit_cmd = [
        "spark-submit",
        "--master", "local[*]",
        "--driver-memory", "4G",
        "/tmp/spark_script.py"
    ]

    try:
        logger.info("Running the Spark job")
        
        process = subprocess.Popen(spark_submit_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        for stdout_line in iter(process.stdout.readline, ""):
            logger.info(stdout_line.strip())
            output += stdout_line.strip() + "\n"

        for stderr_line in iter(process.stderr.readline, ""):
            logger.error(stderr_line.strip())
            error_output += stderr_line.strip() + "\n"
        
        process.stdout.close()
        process.stderr.close()
        return_code = process.wait()

        filtered_errors = search_for_errors(output, error_output)

        if return_code != 0 or filtered_errors:
            full_error_message = f"Error during Spark job execution:\n{filtered_errors}" if filtered_errors else "Error during Spark job execution: No specific error message available."
            
            if len(full_error_message) > MAX_LOG_LENGTH:
                truncated_message = full_error_message[:MAX_LOG_LENGTH] + "\n... Log truncated. Check CloudWatch for full logs."
                return truncated_message

            return full_error_message

        if output.strip():
            return output
        else:
            return "Successful run"

    except Exception as e:
        logger.error(f"Error running Spark job: {e}")
        raise e

def lambda_handler(event, context):
    """
    Lambda handler that receives a PySpark script in the event payload,
    checks its syntax, runs it using Spark-submit locally, and returns the results.
    """
    logger.info("******************Start AWS Lambda Handler************")

    try:
        # Extract the PySpark script from the event payload
        pyspark_script = event.get('script')
        
        if not pyspark_script:
            raise ValueError("No PySpark script provided in the event payload")

        # Compile the script to check for syntax errors
        #compile_pyspark_script(pyspark_script)

        # Run the Spark job and capture the results or success message
        result = run_spark_job(pyspark_script)

        logger.info("******************End AWS Lambda Handler************")
        
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
