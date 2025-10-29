import httpx
import requests
import boto3
import duckdb
from typing import List, Tuple
from pendulum import datetime

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

TOTAL_MESSAGES = 21
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
SQS_CLIENT = boto3.client('sqs', region_name='us-east-1', aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"), aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"))
DUCKDB_FILE = 'puzzle_airflow.db'

#initialization of api and duckdb
@task()
def initialize_api():
    print("Initializing API and retrieving SQS URL...")
    try:
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/zzz2bx"
        payload = requests.post(url).json()
        sqs_url = payload.get("sqs_url")
        PUZZLE_URL = Variable.set("PUZZLE_URL", sqs_url)
        if not sqs_url:
            raise ValueError("API response did not contain 'sqs_url'.")
        return sqs_url
    except requests.exceptions.RequestException as e:
        print(f"Error initializing API: {e}")
        raise

@task()
def initialize_duckdb():
    print("Initializing DuckDB and creating table if not exists...")
    try:
        conn = duckdb.connect(DUCKDB_FILE)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fragments (
                order_no INTEGER PRIMARY KEY,
                word VARCHAR
            )
        """)
        conn.close()
    except Exception as e:
        print(f"Error initializing DuckDB: {e}")
        raise

#declare dag
@dag(
    dag_id="initial_setup",
    start_date=datetime(2025, 1, 1),
    description="initial setup for API and DuckDB",
)
def initial_setup():
    initialize_api()
    initialize_duckdb()

initial_setup()

#tasks for processing sqs messages
@task()
def get_total_message_count(sqs_url: str) -> int:
    try:
        response = SQS_CLIENT.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed'])
        attributes = response.get('Attributes', {})
        visible = int(attributes.get('ApproximateNumberOfMessages', 0))
        not_visible = int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0))
        delayed = int(attributes.get('ApproximateNumberOfMessagesDelayed', 0))
        total_count = visible + not_visible + delayed
        print(f"Queue Status: Total Messages= {total_count} (Visible={visible}, Not Visible={not_visible}, Delayed={delayed})")
        return total_count
    except Exception as e:
        print(f"Error processing SQS messages: {e}")
        return -1
#collect messages from sqs
@task()
def collect_messages(sqs_url: str)-> List[Tuple[int, int, str]]:
    print("Collecting messages from SQS...")
    response = SQS_CLIENT.receive_message(
        QueueUrl=sqs_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1,
        VisibilityTimeout=300,
        MessageAttributeNames=['order_no', 'word']
    )

    messages = response.get('Messages', [])
    if not messages:
        print("No messages received from SQS.")
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
            print(f"Missing expected message attribute: {e}")
        except ValueError as e:
            print(f"Error converting order_no to int: {e}")
    print(f"Collected {len(parsed_messages)} messages from SQS.")
    return parsed_messages

@task()
def delete_messages(sqs_url: str, messages_to_delete: List[Tuple[int, str, str]]) -> int:
    print("Deleting messages from SQS...")

    #creates unique id and receipt handle pairs for deletion
    receipt_handles = [
        {'Id': str(order_no), 'ReceiptHandle': handle}
        for order_no, word, handle in messages_to_delete
    ]

    deleted_messages = 0

    #stop task if no messages to delete
    if not receipt_handles:
        print("No messages to delete. Stopping deletion process.")
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
                print(f"{failed_deletions} messages failed to delete in this batch.")

            deleted_messages += successful_deletions_count
            print(f"Deleted {successful_deletions_count} messages in this batch.")
        except Exception as e:
            print(f"Error deleting messages: {e}")
        
    print(f"Total messages deleted in this run: {deleted_messages}")
    return deleted_messages
#save fragments to duckdb
@task()
def save_to_duckdb(messages_with_handles: List[Tuple[int, str]]):
    print("Saving fragments to DuckDB...")
    fragments_to_save = [(order_no, word) for order_no, word, handle in messages_with_handles]

    if not fragments_to_save:
        print("No fragments to save. Exiting task.")
        return
    try:
        with duckdb.connect(DUCKDB_FILE) as conn:
            conn.executemany("""
                INSERT INTO fragments (order_no, word) VALUES (?, ?)
                ON CONFLICT (order_no) DO NOTHING
            """, fragments_to_save) 
        
        print(f"Fragments saved to DuckDB successfully: {len(fragments_to_save)}")
    except Exception as e:
        print(f"Error saving fragments to DuckDB: {e}")
        raise
#count entries in duckdb
@task()
def count_duckdb_entries():
    print("Counting entries in DuckDB...")
    try:
        conn = duckdb.connect(DUCKDB_FILE)
        result = conn.execute("SELECT COUNT(*) FROM fragments").fetchone()
        conn.close()
        count = result[0] if result else 0
        print(f"Total entries in DuckDB: {count}")
        return count
    except Exception as e:
        print(f"Error counting entries in DuckDB: {e}")
        return 0
#submit final reassembled message
@task(task_id="submit_results")
def submit_results():
    print("Submitting final reassembled message...")
    try:
        with duckdb.connect(DUCKDB_FILE) as con:
            sorted_fragments = con.execute("SELECT order_no, word FROM fragments ORDER BY order_no").fetchall()
    except Exception as e:
        print(f"Error reading from DuckDB for submission: {e}")
        return
    
    if len(sorted_fragments) != TOTAL_MESSAGES:
        print(f"Cannot submit: Expected {TOTAL_MESSAGES} fragments but found {len(sorted_fragments)} in database.")
        return
    
    final_message = ' '.join(word for order_no, word in sorted_fragments) 

    print(f"Final reassembled message: {final_message}")

    try:
        response = SQS_CLIENT.send_message(
            QueueUrl=SUBMISSION_URL,
            MessageBody= f'Final Message to Submit from zzz2bx',
            MessageAttributes={
                'uvaid': { 'DataType': 'String', 'StringValue': 'zzz2bx' },
                'phrase': { 'DataType': 'String', 'StringValue': final_message },
                'platform': { 'DataType': 'String', 'StringValue': 'airflow' }
            }
        ) 
        print(f"Submission response: {response}")
        http_status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if http_status and 200 <= http_status < 300:
            print(f"Solution successfully submitted! HTTP Status: {http_status}, Message ID: {response.get('MessageId')}")
        else:
            print(f"Submission failed or returned non-200 status: {http_status}. Response: {response}")

    except Exception as e:
        print(f"Error submitting final message to SQS: {e}")

#declare main dag
@dag(
    dag_id="process_and_submit_puzzle_messages",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    description="DAG for processing and submitting puzzle messages",
)
def process_and_submit_puzzle_messages():
    @task()
    def get_sqs_url_from_variable():
        print("Fetching 'PUZZLE_URL' from Airflow Variables...")
        return Variable.get("PUZZLE_URL")
    
    sqs_url = get_sqs_url_from_variable()

    total_count = get_total_message_count(sqs_url)

    new_messages = collect_messages(sqs_url)

    delete_messages_task = delete_messages(sqs_url, new_messages)
    save_to_duckdb_task = save_to_duckdb(new_messages)
    count_entries = count_duckdb_entries()

    new_messages >> [save_to_duckdb_task, delete_messages_task, total_count]

    save_to_duckdb_task >> count_entries
                                          
    @task.branch()
    def decide_submission(count: int):
        if count == TOTAL_MESSAGES:
            return "submit_results"
        else:
            return "no_submission"
        
    submit_task = submit_results()
    stop_task = EmptyOperator(task_id="no_submission")

    branch = decide_submission(count_entries)
    branch >> [submit_task, stop_task]

process_and_submit_puzzle_messages()

    
    
