'''
Orchestrator For Consumers and Models
Functionality:
    - Based on configs for each topic, creates a python3 process of type controller with a consumerClass module.
    - Scales partitions for the topic based on configs.
    - Manages bases logging.
'''

# Default Imports
import os
import time
import logging
import subprocess

# Set Logging
logger1 = logging.getLogger('1')
logging.basicConfig(format = '%(asctime)s %(message)s',
                    filemode = 'w',
                    level=logging.INFO)
logger1.addHandler(logging.FileHandler('/app_dev/app/logging/splitting.log'))

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

logger1.info(f'----------------------------------------------------------------')
logger1.info(f'Module: Orchestrator | Base topics initialized and counts received from configs.')
logger1.info(f'Module: Orchestrator | Total number of active python3 threads will be {totalThreads}')
logger1.info(f'Module: Orchestrator | Track module in logs before the delimiter. Starting Thread Creation!')
logger1.info(f'----------------------------------------------------------------\n')
startTime = time.time()

# Start Process for Single Page Invoices
for i in range(spiTopicPCount):
    assignPartition = i
    consumerType = 'spi'
    command = ['python3', 'controller.py', '--partition', assignPartition, '--consumerClass', consumerType]
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode == 0:
        logger1.info(f'Module: Orchestrator | Created thread {assignPartition} of type {consumerType}.')

logger1.info(f'Module: Orchestrator | All Threads Created for Consumer Type SPI!\n')

# Start Process for Multi Document Invoices
for i in range(mdiTopicPCount):
    assignPartition = i
    consumerType = 'mdi'
    command = ['python3', 'controller.py', '--partition', assignPartition, '--consumerClass', consumerType]
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode == 0:
        logger1.info(f'Module: Orchestrator | Created thread {assignPartition} of type {consumerType}.')

logger1.info(f'Module: Orchestrator | All Threads Created for Consumer Type MDI!\n')
    
# Start Process for Multi Page Invoices
for i in range(mpiTopicPCount):
    assignPartition = i
    consumerType = 'mpi'
    command = ['python3', 'controller.py', '--partition', assignPartition, '--consumerClass', consumerType]
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode == 0:
        logger1.info(f'Module: Orchestrator | Created thread {assignPartition} of type {consumerType}.')

logger1.info(f'Module: Orchestrator | All Threads Created for Consumer Type MPI!\n')

# Start Process for Image Single Page Invoices
for i in range(imgiTopicPCount):
    assignPartition = i
    consumerType = 'imgi'
    command = ['python3', 'controller.py', '--partition', assignPartition, '--consumerClass', consumerType]
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode == 0:
        logger1.info(f'Module: Orchestrator | Created thread {assignPartition} of type {consumerType}.')

logger1.info(f'Module: Orchestrator | All Threads Created for Consumer Type IMGI!\n')

# Start Process for High Priority Queue Invoices
for i in range():
    assignPartition = i
    consumerType = 'hpi'
    command = ['python3', 'controller.py', '--partition', assignPartition, '--consumerClass', consumerType]
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode == 0:
        logger1.info(f'Module: Orchestrator | Created thread {assignPartition} of type {consumerType}.')

curr_time = str(time.time() - startTime)
logger1.info(f'Module: Orchestrator | All Threads Created for Consumer Type HPI!\n')
logger1.info(f'Module: Orchestrator | ALL THREADS ARE CREATED! Time Taken is {curr_time} seconds.\n')
logger1.info(f'----------------------------------------------------------------\n')
