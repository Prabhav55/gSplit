"""
Main API to Call glazeDONUT Module For Extraction Or Splitting.
"""

"""
IMPORTS FOR BOTH COMPONENTS
"""
# Base Imports
import re
import os
import json
import torch
import boto3
import botocore
import uvicorn
from PIL import Image
import requests
import logging
from io import BytesIO
import asyncio
from pdf2image import convert_from_path
from ext2apflow import ext2ap, send_seg
from transformers import DonutProcessor, VisionEncoderDecoderModel

# Kafka
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError
import socket
import time
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

"""
LOGGING FOR BOTH COMPONENTS
"""
# Set Logging
logging.basicConfig(filename='/app_dev/app/logging/api.log',
                    format = '%(asctime)s %(message)s',
                    filemode = 'w',
                    level=logging.INFO)

logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)

bucket_apaas = os.environ['apaas_bucket']
azureURL = os.environ['azure.service.invoice.connectionString']

logging.info(f'Attempting to load model.')

"""
MODEL CONFIGS
"""

logging.info(f'------ EXTRACTION LOGGING -----')
logging.info(f'Model is loaded')

# Extraction Function - Calls AZURE AI Service Hosting Glazed DONUT
def extract_document(pdf_path):

    syncAnalyzeURL = azureURL + '/formrecognizer/documentModels/prebuilt-invoice:syncAnalyze'

    headers = {
        'accept': '*/*',
        'Content-Type': 'application/octet-stream',
    }

    params = {
        'api-version': '2022-08-31',
    }

    with open(pdf_path, 'rb') as f:
        data = f.read()

    internal_output = requests.post(
        syncAnalyzeURL,
        params=params,
        headers=headers,
        data=data,
    ).json()

    try:
        output = internal_output['analyzeResult']['documents'][0]['fields']
    except:
        output = {}

    try:
        key_val = internal_output['analyzeResult']['keyValuePairs']
    except:
        key_val = []
    
    try:
        pages = internal_output['analyzeResult']['pages']
        tables = internal_output['analyzeResult']['tables']
    except:
        pages = {}
        tables = []

    return output, pages, key_val, tables

# Splitter Logic
def split_document(image_path, page_to_analyze = -1):

    syncAnalyzeURL = azureURL + '/formrecognizer/documentModels/prebuilt-invoice:syncAnalyze'

    headers = {
        'accept': '*/*',
        'Content-Type': 'application/octet-stream',
    }

    if page_to_analyze != -1:
        params = {
            'api-version': '2022-08-31',
            'pages': page_to_analyze
        }
    else:
        params = {
            'api-version': '2022-08-31'
        }

    with open(image_path, 'rb') as f:
        data = f.read()

    internal_output = requests.post(
        syncAnalyzeURL,
        params=params,
        headers=headers,
        data=data,
    ).json()

    try:
        output = internal_output['analyzeResult']['documents'][0]['fields']
    except:
        output = {}

    try:
        key_val = internal_output['analyzeResult']['keyValuePairs']
    except:
        key_val = []
    
    try:
        pages = internal_output['analyzeResult']['pages']
        tables = internal_output['analyzeResult']['tables']
    except:
        pages = {}
        tables = []

    return output, pages, key_val, tables
