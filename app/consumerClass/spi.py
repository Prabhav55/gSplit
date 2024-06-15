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
            parent_caseId = msg['parentCaseId']
            imgname = msg['imageName'].split('.')[0]
            metadataURL = msg['metadataUrl']
        except:
            caseId = msg['caseId']
            parent_caseId = ""
            pdfURL = msg['pdfURL']
            imgname = ''

        logger3.info(f'Module: SPI, Parition {partitionID} | SPLITTING REQUEST ACCEPTED')
        logger3.info(f'Module: SPI, Parition {partitionID} | Case ID is {caseId} | Parent Case ID is {parent_caseId} | Single Page PDF | Document Name is {imgname}.')

        # DOWNLOAD FILES WITH TIMEOUT OR PRODUCE TO APAAS CASES
        if pdfURL.startswith('s3://'):
            bucket = pdfURL.split('s3://')[1].split('/')[0]
            path = pdfURL.split(bucket)[1][1:]
        else:
            if pdfURL.startswith('/'):
                bucket = pdfURL[1:].split('/')[0]
                path = pdfURL.split(bucket)[1][1:]
            else:
                bucket = pdfURL.split('/')[0]
                path = pdfURL.split(bucket)[1][1:]
        bucket = os.environ['apaas_bucket']

        invoiceMimetype = msg['invoiceMimeType']
        extensionType = msg['imageName'].split('.')[1]
        logger3.info(f'Module: SPI, Parition {partitionID} | {caseId} - Extension type is {extensionType}')

        try:

            if extensionType.lower() == 'tiff' or extensionType.lower() == 'tif':
                logger3.warning(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | INVOICE IS NOT A PDF - TIFF Detected. Converting!')
                s3 = boto3.client('s3')
                filename = '/app_dev/app/tempstorage/' + caseId + '.tiff'
                s3_path_to_delete = filename
                s3.download_file(bucket, path, filename)
                filename = docConvert.tiff_to_pdf(filename)
                documentMimetype = extensionType

            elif extensionType.lower() in ['jpeg', 'jpg', 'image', 'png']:
                logger3.warning(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | INVOICE IS NOT A PDF - Image Detected. Converting!')
                s3 = boto3.client('s3')
                filename = '/app_dev/app/tempstorage/' + caseId + '.' + extensionType.lower()
                s3_path_to_delete = filename
                s3.download_file(bucket, path, filename)
                filename = '/app_dev/app/tempstorage/' + caseId + '.' + extensionType.lower()
                logger3.info(f'Image Path is {filename}')
                filename = docConvert.img_to_pdf(filename)
                documentMimetype = extensionType

            else:
                s3 = boto3.client('s3')
                filename = '/app_dev/app/tempstorage/' + caseId + '.pdf'
                s3_path_to_delete = filename
                s3.download_file(bucket, path, filename)
                documentMimetype = 'PDF'

            logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | File Downloaded From S3 Successfully.')

        except Exception as e:

            try:
            
                logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | File was not available on S3. Sleeping for 10 seconds and retrying for 2 times!')
                time.sleep(10)
                
                if extensionType.lower() == 'tiff' or extensionType.lower() == 'tif':
                    logger3.warning(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | INVOICE IS NOT A PDF - TIFF Detected. Converting!')
                    s3 = boto3.client('s3')
                    filename = '/app_dev/app/tempstorage/' + caseId + '.tiff'
                    s3_path_to_delete = filename
                    s3.download_file(bucket, path, filename)
                    filename = docConvert.tiff_to_pdf(filename)
                    documentMimetype = extensionType

                elif extensionType.lower() in ['jpeg', 'jpg', 'image', 'png']:
                    logger3.warning(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | INVOICE IS NOT A PDF - Image Detected. Converting!')
                    s3 = boto3.client('s3')
                    filename = '/app_dev/app/tempstorage/' + caseId + '.' + extensionType.lower()
                    s3_path_to_delete = filename
                    s3.download_file(bucket, path, filename)
                    filename = '/app_dev/app/tempstorage/' + caseId + '.' + extensionType.lower()
                    filename = docConvert.img_to_pdf(filename)
                    documentMimetype = extensionType

                else:
                    s3 = boto3.client('s3')
                    filename = '/app_dev/app/tempstorage/' + caseId + '.pdf'
                    s3_path_to_delete = filename
                    s3.download_file(bucket, path, filename)
                    documentMimetype = 'PDF'

                logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | File Downloaded From S3 Successfully.')

            except:

                time.sleep(10)
                if extensionType.lower() == 'tiff' or extensionType.lower() == 'tif':
                    logger3.warning(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | INVOICE IS NOT A PDF - TIFF Detected. Converting!')
                    s3 = boto3.client('s3')
                    filename = '/app_dev/app/tempstorage/' + caseId + '.tiff'
                    s3_path_to_delete = filename
                    s3.download_file(bucket, path, filename)
                    filename = docConvert.tiff_to_pdf(filename)
                    documentMimetype = extensionType

                elif extensionType.lower() in ['jpeg', 'jpg', 'image', 'png']:
                    logger3.warning(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | INVOICE IS NOT A PDF - Image Detected. Converting!')
                    s3 = boto3.client('s3')
                    filename = '/app_dev/app/tempstorage/' + caseId + '.' + extensionType.lower()
                    s3_path_to_delete = filename
                    s3.download_file(bucket, path, filename)
                    filename = '/app_dev/app/tempstorage/' + caseId + '.' + extensionType.lower()
                    filename = docConvert.img_to_pdf(filename)
                    documentMimetype = extensionType

                else:
                    s3 = boto3.client('s3')
                    filename = '/app_dev/app/tempstorage/' + caseId + '.pdf'
                    s3_path_to_delete = filename
                    s3.download_file(bucket, path, filename)
                    documentMimetype = 'PDF'

                logger3.info(f'File Downloaded From S3 Successfully.')

        extracts = {}
        po_numbers = {}
        pdf = PdfReader(filename)

        # LOAD FROM CACHE IF PARENT EXISTS. OTHERWISE EXTRACT.
        if len(pdf.pages) == 1:

            if parent_caseId == '':

                # Extract PO Numbers
                logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Parent Case ID not specified. Assuming New Case. No Cache Located.')
                out, pages_out, key_val, tables = model.split_document(filename)
                extracts[0] = out
                to_save = [out, pages_out, key_val, tables]
                po_numbers[0] = ext2apflow.split_pofinder(out, key_val)

                # Save To Cache
                localPathCache = '/app_dev/app/zcache/' + caseId + '_0.json'
                s3pathCache = 'zcache/'  + caseId + '_0.json'
                with open(localPathCache, 'w') as f:
                    json.dump(to_save, f)
                s3.upload_file(s3pathCache, bucket_apaas, localPathCache)
                logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Extracted Date and cache stored with path in S3 as {s3pathCache}.')

            else:
                
                # Load From Cache.
                get_s_page = imgname.split('_')[1]
                cachePath = 'app_dev/app/zcache/'  + parent_caseId + '_' + str(get_s_page) + '.json'
                logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Parent Case ID specified. Looking In Cache Path {cachePath} - Using page number {get_s_page}.')

                if os.path.isfile(path):
                    with open(path, 'r') as f:
                        cache_ext = json.load(f)
                    extracts[0] = cache_ext[0]
                    po_numbers[0] = ext2apflow.split_pofinder(extracts[0], cache_ext[2])
                    logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | CACHE FOUND IN ZCACHE! Using Preexisting Extract.')

                else:
                    logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Cache Not Found In ZCACHE! Extracting.')
                    out, pages_out, key_val, tables = model.split_document(filename)
                    extracts[0] = out
                    po_numbers[0] = ext2apflow.split_pofinder(out, key_val)
                    to_save = [out, pages_out, key_val, tables]

                    # Save To Cache
                    localPathCache = '/app_dev/app/zcache/' + caseId + '_0.json'
                    s3pathCache = 'zcache/'  + caseId + '_0.json'
                    with open(localPathCache, 'w') as f:
                        json.dump(to_save, f)
                    s3.upload_file(s3pathCache, bucket_apaas, localPathCache)
                    logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Extracted Date and cache stored with path in S3 as {s3pathCache}.')

            # Updated Document ID
            doc_id = pdfURL.split('/')[-1]
            new_pdf_url = pdfURL.split('s3://')[1].replace(bucket, '')[1:]
            is_split = False
            splits = []

            # No Split Required. Send Message to EL
            extracts_po = extracts[0]
            logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | SPLIT IS FALSE - Single Page Invoice.')

            try:

                out_message = {
                    "bucket": bucket,
                    "caseId": caseId,
                    "pdfURL": new_pdf_url,
                    "metadataURL": metadataURL,
                    "imageName": imgname + '.' + str(extensionType),
                    "reject_reason": "",
                    "documentMimetype": documentMimetype,
                    "isMultipleDocument" : "false",
                    "documentType": "INVOICE",
                    "documentSubType": "PO",
                    "startPage": "",
                    "endPage": "",
                    "apaasPONumber": extracts_po['PurchaseOrder']['content'],
                    "sellerName": "",
                    "apaasERPName": "",
                    "invoiceNumber": "",
                    "invoiceDate": ""
                }

                logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | PO IS FOUND, SENDING FOR ERP IDENTIFICATION')

                # Write to Kafka
                try:
                    producer.send(producerTopic, json.dumps(out_message).encode('utf-8'), partition = partitionID)
                    producer.flush()
                    logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Message Sent To Producer Topic.')
                except Exception as e:
                    logger3.error(e)
                    logger3.error(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Producer Failed - Retrying')
                    time.sleep(5)
                    producer.send(producerTopic, json.dumps(out_message).encode('utf-8'), partition = partitionID)
                    producer.flush()

            except Exception as e:

                po_num = po_numbers[0]

                if po_num != '':
                    out_message = {
                        "bucket": bucket,
                        "caseId": caseId,
                        "pdfURL": new_pdf_url,
                        "metadataURL": metadataURL,
                        "imageName": imgname + '.' + str(extensionType),
                        "reject_reason": "",
                        "documentMimetype": documentMimetype,
                        "isMultipleDocument" : "false",
                        "documentType": "INVOICE",
                        "documentSubType": "PO",
                        "startPage": "",
                        "endPage": "",
                        "apaasPONumber": po_num,
                        "sellerName": "",
                        "apaasERPName": "",
                        "invoiceNumber": "",
                        "invoiceDate": ""
                    }
                    logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | PO Found - Sending to ERP Identification.')

                    # Write to Kafka
                    try:
                        producer.send(producerTopic, json.dumps(out_message).encode('utf-8'), partition = partitionID)
                        producer.flush()
                        logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Message Sent To Producer Topic.')
                    except Exception as e:
                        logger3.error(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Producer Failed - Retrying')
                        time.sleep(5)
                        producer.send(producerTopic, json.dumps(out_message).encode('utf-8'), partition = partitionID)
                        producer.flush()
                    
                else:
                    out_message = {
                        "bucket": bucket,
                        "caseId": caseId,
                        "pdfURL": new_pdf_url,
                        "metadataURL": metadataURL,
                        "imageName": imgname + '.' + str(extensionType),
                        "reject_reason": "",
                        "documentMimetype": documentMimetype,
                        "isMultipleDocument" : "false",
                        "documentType": "INVOICE",
                        "documentSubType": "PO",
                        "startPage": "",
                        "endPage": "",
                        "apaasPONumber": "",
                        "sellerName": "",
                        "apaasERPName": "",
                        "invoiceNumber": "",
                        "invoiceDate": ""
                    }
                    logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | PO EXTRACTION FAILED - Moving to direct extraction.')

                    # Write to Kafka
                    try:
                        producer.send(extractionIn, json.dumps(out_message).encode('utf-8'), partition = 0)
                        producer.flush()
                        logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Message Sent To Extraction Topic.')
                    except Exception as e:
                        logger3.error(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Producer Failed - Retrying')
                        time.sleep(5)
                        producer.send(extractionIn, json.dumps(out_message).encode('utf-8'), partition = 0)
                        producer.flush()

            logger3.info(f'Module: SPI, Parition {partitionID}, Case ID {caseId} | Completed Process For - Case ID {caseId} | Parent Case {parent_caseId} | Document Name {imgname}.')
            return caseId, 200

        else:

            return caseId, 500