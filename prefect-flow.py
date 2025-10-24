# prefect flow goes here
import httpx
from prefect import flow, task
import requests
import boto3
from prefect.logging import get_run_logger
from typing import List, Tuple

TOTAL_MESSAGES = 21
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
SQS_CLIENT = boto3.client('sqs')

@task(name ="Fetch Messages")
def fetch_messages():
    logger = get_run_logger()
    logger.info("Fetching SQS URL from external API...retrieving 21 new messages")
    try:
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/zzz2bx"
        payload = requests.post(url).json()
        sqs_url = payload.get("sqs_url")
        logger.info(f"Retrieved SQS URL: {sqs_url}")
        if not sqs_url:
            raise ValueError("API response did not contain 'sqs_url'.")
        return sqs_url
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching SQS URL: {e}")
        raise

@task(name = "Process Current Messages")
def get_total_message_count(sqs_url: str) -> int:
    logger = get_run_logger()
    logger.info("Processing messages from SQS")
    try:
        response = SQS_CLIENT.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed'])
        attributes = response.get('Attributes', {})
        visible = int(attributes.get('ApproximateNumberOfMessages', 0))
        not_visible = int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0))
        delayed = int(attributes.get('ApproximateNumberOfMessagesDelayed', 0))
        total_count = visible + not_visible + delayed
        logger.info(f"Queue Status: Total Messages= {total_count} (Visible={visible}, Not Visible={not_visible}, Delayed={delayed})")
        return total_count
    except Exception as e:
        logger.error(f"Error processing SQS messages: {e}")

@task(name = 'Collect Messages')
def collect_messages(sqs_url: str)-> List[Tuple[int, int, str]]:
    logger = get_run_logger()
    logger.info("Collecting and parsing messages from SQS")
    
    response = SQS_CLIENT.receive_message(
        QueueUrl=sqs_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1,
        VisibilityTimeout=300,
        MessageAttributeNames=['order_no', 'word']
    )

    messages = response.get('Messages', [])
    if not messages:
        logger.info("No messages received from SQS.")
        return []
    
    parsed_messages = []

    for message in messages:
        try: 
            attributes = message.get('MessageAttributes', {})
            order_no_str = attributes['order_no']['StringValue']
            order_no = int(order_no_str)

            word = attributes['word']['StringValue']

            handle = message['ReceiptHandle']

            parsed_messages.append((order_no, word, handle))

        except KeyError as e:
            logger.error(f"Missing expected message attribute: {e}")
        except ValueError as e:
            logger.error(f"Error converting order_no to int: {e}")
    logger.info(f"Collected {len(parsed_messages)} messages from SQS.")
    return parsed_messages

#task that deletes messages from SQS with receipt handles
@task(name = "Delete Messages")
def delete_messages(sqs_url: str, messages_to_delete: List[Tuple[int, str, str]]) -> int:
    logger = get_run_logger()
    logger.info("Deleting processed messages from SQS")
    
    #creates unique id and receipt handle pairs for deletion
    receipt_handles = [
        {'Id': str(order_no), 'ReceiptHandle': handle}
        for order_no, word, handle in messages_to_delete
    ]

    deleted_messages = 0

    #stop task if no messages to delete
    if not receipt_handles:
        logger.info("No messages to delete. Stopping deletion process.")
        return 0
    
    #deletes messages in batches of 10
    for i in range(0, len(receipt_handles), 10):
        batch = receipt_handles[i:i + 10]
        try:
            response = SQS_CLIENT.delete_message_batch(
                QueueUrl=sqs_url,
                Entries=batch
            )
            #get count of successful deletions
            successful_deletions_count = len(response.get('Successful', []))
            #get count of failed deletions
            failed_deletions = len(response.get('Failed', []))
            if failed_deletions > 0:
                logger.error(f"{failed_deletions} messages failed to delete in this batch.")

            deleted_messages += successful_deletions_count
            logger.info(f"Deleted {successful_deletions_count} messages in this batch.")
        except Exception as e:
            logger.error(f"Error deleting messages: {e}")
        
    logger.info(f"Total messages deleted in this run: {deleted_messages}")
    return deleted_messages

@task(name = "Reassemble and Submit Total Messages")
def submit_total_messages(all_messages: List[Tuple[int, str]]):
    logger = get_run_logger()
    logger.info("Reassembling and submitting total messages to external endpoint")

    if len(all_messages) != TOTAL_MESSAGES:
        logger.error(f"Cannot submit: Expected {TOTAL_MESSAGES} fragments but only received {len(all_messages)}.")
        return
    
    sorted_messages = sorted(all_messages, key=lambda x: x[0])
    
    final_message = ''.join(word for order_no, word in sorted_messages)  

    logger.info(f"Final reassembled message: {final_message}")        
    
    try:
        response = SQS_CLIENT.send_message(
            QueueUrl=SUBMISSION_URL,
            MessageBody= 'Final Message to Submit from Anna Yao',
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': 'zzz2bx'
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': final_message
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': 'Prefect'
                }
            }
        )
        print(f"Response: {response}")
        
        response.raise_for_status()
        if response.status_code != 200:
            logger.error(f"Failed to submit message. Status code: {response.status_code}, Response: {response.text}")
            return
        logger.info(f"Successfully submitted message. Response: {response.text}")
    except httpx.HTTPError as e:
        logger.error(f"Error submitting final message: {e}")
        

@flow(name = "SQS Message Processing Flow")
def master_sqs_puzzle_flow(sqs_url: str, collected_fragments: List[Tuple[int, str]] = None) -> List[Tuple[int, str]]:
    logger = get_run_logger()
    if collected_fragments is None:
        collected_fragments = []

    total_in_queue = get_total_message_count(sqs_url)

    if total_in_queue > 0:
        new_messages = collect_messages(sqs_url)
        if new_messages:
            new_fragments = [(order_no, word) for order_no, word, handle in new_messages]
            
            logger.info(f"Appended {len(new_fragments)} new fragments")

            delete_messages(sqs_url, new_messages)

            return new_fragments
        else:
            logger.info("No visible messages to collect. Waiting for delayed messages to expire.")
            return []
    elif total_in_queue == 0:
        logger.info("Queue is empty. Checking for collected fragments to submit.")
        if len(collected_fragments) == TOTAL_MESSAGES:
            submit_total_messages(collected_fragments)
            return collected_fragments
        else:
            logger.warning(f"Queue is empty, but only {len(collected_fragments)} fragments were accumulated. Check previous run logs or data persistence.")
            return collected_fragments
    
    return []     

@flow(name="Initialize")
def initialize_queue_flow():
    return fetch_messages()
            

