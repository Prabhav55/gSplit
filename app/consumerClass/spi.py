'''
Processor For Single Page Invoice
Functionality:
    - Define Class for Single Page Invoice
'''

# Base Imports
import re
import os
import json
import time
import boto3
import logging
import botocore
from io import BytesIO
from PyPDF2 import PdfReader
from PIL import Image, ImageSequence

# Helpers
from helper import ext2apflow, splitting, model, docConvert

# Set Logging
logger3 = logging.getLogger('3')
logging.basicConfig(format = '%(asctime)s %(message)s',
                    filemode = 'w',
                    level=logging.INFO)
logger3.addHandler(logging.FileHandler('/app_dev/app/logging/splitting.log'))

# Set Kafka and S3 Logger Level
logging.getLogger('kafka').setLevel(logging.ERROR)
logging.getLogger('kafka.errors').setLevel(logging.ERROR)
logging.getLogger('aws_msk_iam_sasl_signer').setLevel(logging.ERROR)
logging.getLogger('socket').setLevel(logging.ERROR)
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)

# Load Consumer Topic Names
spiConsumerTopicName = os.environ['spi.topic.consumer.name']
mdiConsumerTopicName = os.environ['mdi.topic.consumer.name']
mpiConsumerTopicName = os.environ['mpi.topic.consumer.name']
hpiConsumerTopicName = os.environ['hpi.topic.consumer.name']
imgiConsumerTopicName = os.environ['imgi.topic.consumer.name']

# Load Producer Topic Names
spiProducerTopicName = os.environ['spi.topic.producer.name']
mdiProducerTopicName = os.environ['mdi.topic.producer.name']
mpiProducerTopicName = os.environ['mpi.topic.producer.name']
hpiProducerTopicName = os.environ['hpi.topic.producer.name']
imgiProducerTopicName = os.environ['imgi.topic.producer.name']
extractionIn = os.environ['extractionin.topic.producer.name']

# Load Parition Count - Scaled Producers and Consumer for Each
spiTopicPCount = int(os.environ['spi.topic.pcount'])
mdiTopicPCount = int(os.environ['mdi.topic.pcount'])
mpiTopicPCount = int(os.environ['mpi.topic.pcount'])
hpiTopicPCount = int(os.environ['hpi.topic.pcount'])
imgiTopicPCount = int(os.environ['imgi.topic.pcount'])
extractionInPCount = os.environ['extractionin.topic.pcount']
totalThreads = sum([spiTopicPCount, mdiTopicPCount, hpiTopicPCount, imgiTopicPCount, mpiTopicPCount, extractionInPCount])

# Base Configs
bucket_apaas = os.environ['apaas_bucket']
s3_region = os.environ['s3_region']
brokers = os.environ['brokers'].split(',')

logger3.info(f'----------------------------------------------------------------')
logger3.info(f'Module: SPI | Single Page Invoice Processor Intitalized.')
logger3.info(f'----------------------------------------------------------------\n')

class SPI:

    def process(self, partitionID, producerTopic, message, producer):
        '''
        Assumption will be that it is a single page invoice.
        arguments:
            - partitionID: The partition to produce to.
            - producerTopic: The topic to produce to.
            - message: Coded message from Kafka.
            - producer: Kafka Producer Class.
        returns:
            - caseId: Case ID of the case that is processed.
            - status:
                - 200: Full Success and Message Sent.
                - 300: Partial Success & Empty UDP Sent.
                - 500: Full Error. No Message Sent.
        '''

        # GETTING BASE VALUES FROM MESSAGE
        msg = message.value.decode('utf-8')
        msg = json.loads(msg)
        
        try:
            caseId = msg['caseId']
            pdfURL = msg['invoiceUrl']
            imgname = msg['imageName'].split('.')[0]
            metadataURL = msg['metadataUrl']
        except:
            caseId = msg['caseId']
            pdfURL = msg['pdfURL']
            imgname = ''

        return caseId, status