'''
Controller For Consumers and Models
Functionality:
    - Receive request from orchestrator to start subprocess on single thread.
    - Create Consumer and Producer pair on n partitions bases on arguments.
    - Manage Classes of Consumers.
'''

# Default Imports
import os
import time
import socket
import logging
import argparse

# Kafka and S3 Imports
import boto3
import botocore
from kafka.errors import KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition

# Module Imports
from consumerClass import spi, hpi, imgi, mdi, mpi

# Set Logging
logger2 = logging.getLogger('2')
logging.basicConfig(format = '%(asctime)s %(message)s',
                    filemode = 'w',
                    level=logging.INFO)
logger2.addHandler(logging.FileHandler('/app_dev/app/logging/splitting.log'))

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

logger2.info(f'----------------------------------------------------------------')
logger2.info(f'Module: Controller | Base topics initialized and counts received from configs.')
logger2.info(f'----------------------------------------------------------------\n')
startTime = time.time()

# Classes for MSK Kafka
class MSKTokenProvider():
    def token(self):
        if s3_region == 'us-east-1':
            token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        elif s3_region == 'ap-south-1':
            token, _ = MSKAuthTokenProvider.generate_auth_token('ap-south-1')
        else:
            token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

tp = MSKTokenProvider()

# Function To Create Consumer
def createConsumerProducer(topic, partitionID):

    currConsumer = KafkaConsumer(
        group_id='gSplit',
        bootstrap_servers=brokers,
        security_protocol='SASL_SSL',
        sasl_mechanism = 'OAUTHBEARER',
        sasl_oauth_token_provider = tp,
        client_id = socket.gethostname(),
        max_poll_interval_ms = 900000000,
        auto_offset_reset='latest'
    )

    partition = TopicPartition(topic, int(partitionID))
    currConsumer.assign([partition])

    currProducer = KafkaProducer(
        bootstrap_servers=brokers,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=tp,
        client_id=socket.gethostname(),
    )

    return currConsumer, currProducer

# Function To Start Consumer
def startConsumer(args):

    partitionID = args.partition
    consumerType = args.consumerClass

    if consumerType == 'spi':

        consumer, producer = createConsumerProducer(spiConsumerTopicName, partitionID)
        producerTopic = spiProducerTopicName
        msgProcessor = spi.SPI()

        logger2.info(f'Module: Controller SPI Parition {partitionID} | Switching on Consumer!')
        for message in consumer:

            try:

                startTime = time.time()
                logger2.info(f'Module: Controller SPI Parition {partitionID} | Received Message. Processing and Decoding.')
                caseId, status = msgProcessor.process(partitionID, producerTopic, message, producer)
                curr_time = str(time.time() - startTime)
                logger2.info(f'Module: Controller SPI Parition {partitionID} | Status is {status} for message caseId {caseId}. Total time taken is {curr_time}.\n')

            except Exception as e:

                logger2.info(f'Module: Controller SPI Parition {partitionID} | Ran into unknown exception: {e} for message. Skipping.')

    if consumerType == 'imgi':

        consumer, producer = createConsumerProducer(imgiConsumerTopicName, partitionID)
        producerTopic = imgiProducerTopicName
        msgProcessor = imgi.IMGI()

        logger2.info(f'Module: Controller IMGI Parition {partitionID} | Switching on Consumer!')
        for message in consumer:

            try:

                startTime = time.time()
                logger2.info(f'Module: Controller IMGI Parition {partitionID} | Received Message. Processing and Decoding.')
                caseId, status = msgProcessor.process(partitionID, producerTopic, message, producer)
                curr_time = str(time.time() - startTime)
                logger2.info(f'Module: Controller IMGI Parition {partitionID} | Status is {status} for message caseId {caseId}. Total time taken is {curr_time}.\n')

            except Exception as e:

                logger2.info(f'Module: Controller IMGI Parition {partitionID} | Ran into unknown exception: {e} for message. Skipping.')

    if consumerType == 'mpi':

        consumer, producer = createConsumerProducer(mpiConsumerTopicName, partitionID)
        producerTopic = mpiProducerTopicName
        msgProcessor = mpi.MPI()

        logger2.info(f'Module: Controller MPI Parition {partitionID} | Switching on Consumer!')
        for message in consumer:

            try:

                startTime = time.time()
                logger2.info(f'Module: Controller MPI Parition {partitionID} | Received Message. Processing and Decoding.')
                caseId, status = msgProcessor.process(partitionID, producerTopic, message, producer)
                curr_time = str(time.time() - startTime)
                logger2.info(f'Module: Controller MPI Parition {partitionID} | Status is {status} for message caseId {caseId}. Total time taken is {curr_time}.\n')

            except Exception as e:

                logger2.info(f'Module: Controller MPI Parition {partitionID} | Ran into unknown exception: {e} for message. Skipping.')

    if consumerType == 'mdi':

        consumer, producer = createConsumerProducer(mdiConsumerTopicName, partitionID)
        producerTopic = mdiProducerTopicName
        msgProcessor = mdi.MDI()

        logger2.info(f'Module: Controller MDI Parition {partitionID} | Switching on Consumer!')
        for message in consumer:

            try:

                startTime = time.time()
                logger2.info(f'Module: Controller MDI Parition {partitionID} | Received Message. Processing and Decoding.')
                caseId, status = msgProcessor.process(partitionID, producerTopic, message, producer)
                curr_time = str(time.time() - startTime)
                logger2.info(f'Module: Controller MDI Parition {partitionID} | Status is {status} for message caseId {caseId}. Total time taken is {curr_time}.\n')

            except Exception as e:

                logger2.info(f'Module: Controller MDI Parition {partitionID} | Ran into unknown exception: {e} for message. Skipping.')

    if consumerType == 'hpi':

        consumer, producer = createConsumerProducer(hpiConsumerTopicName, partitionID)
        producerTopic = hpiProducerTopicName
        msgProcessor = hpi.HPI()

        logger2.info(f'Module: Controller HPI Parition {partitionID} | Switching on Consumer!')
        for message in consumer:

            try:

                startTime = time.time()
                logger2.info(f'Module: Controller HPI Parition {partitionID} | Received Message. Processing and Decoding.')
                caseId, status = msgProcessor.process(partitionID, producerTopic, message, producer)
                curr_time = str(time.time() - startTime)
                logger2.info(f'Module: Controller HPI Parition {partitionID} | Status is {status} for message caseId {caseId}. Total time taken is {curr_time}.\n')

            except Exception as e:

                logger2.info(f'Module: Controller HPI Parition {partitionID} | Ran into unknown exception: {e} for message. Skipping.')

# MAIN FUNCTION
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Controller For Kafka.")
    parser.add_argument('--partition', type=int, required=True, help='Number of Partions for Topic.')
    parser.add_argument('--consumerClass', type=int, required=True, help='Type of Consumer Class.')
    args = parser.parse_args()
    time.sleep(10)
    startConsumer(args)