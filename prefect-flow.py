# prefect flow goes here
import httpx
from prefect import flow, task
import requests
import boto3
from prefect.logging import get_run_logger

TOTAL_MESSAGES = 21
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
SQS_CLIENT = boto3.client('sqs')

@task(name ="Fetch Messages")
def fetch_messages():
    logger = get_run_logger()
    logger.info("Fetching SQS URL from external API...retrieving 21 new messages")
    try:
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/zzz2bx"
        payload = requests.post(url).json
        sqs_url = payload.get("sqs_url")
        logger.info(f"Retrieved SQS URL: {sqs_url}")
        if not sqs_url:
            raise ValueError("API response did not contain 'sqs_url'.")
        return sqs_url
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching SQS URL: {e}")
        raise
        
