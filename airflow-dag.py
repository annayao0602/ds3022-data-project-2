import httpx
import requests
import boto3
import logging
import duckdb
from typing import List, Tuple
from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
