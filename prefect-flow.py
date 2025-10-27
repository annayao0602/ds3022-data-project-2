# prefect flow goes here
import httpx
from prefect import flow, task
import requests
import boto3
from prefect.logging import get_run_logger
from typing import List, Tuple
import duckdb
import time

TOTAL_MESSAGES = 21
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
SQS_CLIENT = boto3.client('sqs')
DUCKDB_FILE = 'puzzle.db'

#initial task to receive SQS URL from external API
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
#uses get_queue_attributes to get total message count in SQS
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
        return -1

#task to collect messages from the sqs client, pulling order_no and word attributes
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

    #parse handle for deletion task later
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

#task to initialize duckdb and create fragments table
@task(name = "initialize DuckDB")
def initialize_duckdb():
    logger = get_run_logger()
    logger.info("Initializing DuckDB database for message storage")
    try:
        conn = duckdb.connect(DUCKDB_FILE)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fragments (
                order_no INTEGER PRIMARY KEY,
                word VARCHAR
            )
        """)
        conn.close()
        logger.info("DuckDB initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing DuckDB: {e}")
        raise

#task to save message fragments to duckdb, listing order no and word
@task(name = "Save fragments to DuckDB")
def save_to_duckdb(fragments: List[Tuple[int, str]]):
    logger = get_run_logger()
    logger.info("Saving message fragments to DuckDB")
    if not fragments:
        logger.info("No fragments to save. Exiting task.")
        return
    try:
        conn = duckdb.connect(DUCKDB_FILE)
        for order_no, word in fragments:
            conn.execute("""
                INSERT OR REPLACE INTO fragments (order_no, word) VALUES (?, ?)
            """, (order_no, word))
        conn.close()
        logger.info("Fragments saved to DuckDB successfully.")
    except Exception as e:
        logger.error(f"Error saving fragments to DuckDB: {e}")
        raise

#counting rows in duckdb table for monitoring progress
@task(name = "count rows in DuckDB")
def count_duckdb_rows() -> int:
    logger = get_run_logger()
    logger.info("Counting rows in DuckDB fragments table")
    try:
        with duckdb.connect(DUCKDB_FILE) as con:
            count = con.execute("SELECT COUNT(*) FROM fragments").fetchone()[0]
        return count
    except Exception as e:
        logger.error(f"Error counting rows in DuckDB: {e}")
        return 0

#final submission task, pulls fragments from duckdb, reassembles message, and submits to SQS
@task(name = "Reassemble and Submit Total Messages")
def submit_total_messages():
    logger = get_run_logger()
    logger.info("Reassembling and submitting total messages from duckdb")

    try:
        with duckdb.connect(DUCKDB_FILE) as con:
            sorted_fragments = con.execute("SELECT order_no, word FROM fragments ORDER BY order_no").fetchall()
    except Exception as e:
        logger.error(f"Error reading from DuckDB for submission: {e}")
        return
    
    if len(sorted_fragments) != TOTAL_MESSAGES:
        logger.error(f"Cannot submit: Expected {TOTAL_MESSAGES} fragments but found {len(sorted_fragments)} in database.")
        return
    
    final_message = ' '.join(word for order_no, word in sorted_fragments) 

    logger.info(f"Final reassembled message: {final_message}")

    try:
        response = SQS_CLIENT.send_message(
            QueueUrl=SUBMISSION_URL,
            MessageBody= f'Final Message to Submit from zzz2bx',
            MessageAttributes={
                'uvaid': { 'DataType': 'String', 'StringValue': 'zzz2bx' },
                'phrase': { 'DataType': 'String', 'StringValue': final_message },
                'platform': { 'DataType': 'String', 'StringValue': 'prefect' }
            }
        ) 
        logger.info(f"Submission response: {response}")
        http_status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if http_status and 200 <= http_status < 300:
            logger.info(f"Solution successfully submitted! HTTP Status: {http_status}, Message ID: {response.get('MessageId')}")
        else:
            logger.error(f"Submission failed or returned non-200 status: {http_status}. Response: {response}")

    except Exception as e:
        logger.error(f"Error submitting final message to SQS: {e}")

#flow that uses a while loop to poll SQS for new messages every minute for 20 minutes or until all messages are pulled
@flow(name = "SQS Message Processing Flow")
def puzzle_flow():
    logger = get_run_logger()
    logger.info("Starting SQS Message Processing Flow")
    try:
        sqs_url = fetch_messages()
        initialize_duckdb()
    except Exception as e:
        logger.error(f"Flow initialization failed: {e}")
        return
    
    start_time = time.time()
    max_duration_seconds = 20 * 60  # 20 minutes
    poll_interval_seconds = 60

    logger.info(f"Initialization complete. Starting 20-minute collection loop. Polling every {poll_interval_seconds}s.")

    while time.time() - start_time < max_duration_seconds:
        try:
            total_count = get_total_message_count(sqs_url)
            if total_count > 0:
                new_messages_with_handles = collect_messages(sqs_url)

                if new_messages_with_handles:
                    new_fragments = [(order_no, word) for order_no, word, handle in new_messages_with_handles]
                    save_to_duckdb(new_fragments)
                    deleted_count = delete_messages(sqs_url, new_messages_with_handles)
                    logger.info(f"Processed {len(new_messages_with_handles)} new messages, deleted {deleted_count} from SQS.")
            fragments_in_db = count_duckdb_rows()
            logger.info(f"Total fragments stored in DuckDB: {fragments_in_db}/{TOTAL_MESSAGES}")
            if fragments_in_db == TOTAL_MESSAGES:
                logger.info("All message fragments collected. Proceeding to submission.")
                break
        except Exception as e:
            logger.error(f"Error during collection loop: {e}")

        logger.info(f"Cycle complete. Sleeping for {poll_interval_seconds} seconds...")
        time.sleep(poll_interval_seconds)

    logger.info("Loop finished. Proceeding to final reassembly and submission.")
    submit_total_messages() 


if __name__ == "__main__":
    puzzle_flow()
            

