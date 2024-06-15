import os
import re
import json
import logging


# Set Logging
logger3 = logging.getLogger('2')
logging.basicConfig(format = '%(asctime)s %(message)s',
                    filemode = 'w',
                    level=logging.INFO)
logger3.addHandler(logging.FileHandler('/app_dev/app/logging/other.log'))


def donut_splicer(extracts):

    splits = []
    if len(extracts.keys()) == 1:
        return False, splits
    
    c = 1
    c_page = [0, 0]
    try:
        prev_inv = extracts[c - 1]['InvoiceId']['content'].strip()
    except:
        prev_inv = ''
    try:
        prev_po = extracts[c - 1]['PurchaseOrder']['content'].strip()
    except:
        prev_po = ''
    try:
        prev_date = extracts[c - 1]['InvoiceDate']['content'].strip()
    except:
        prev_date = ''
    try:
        prev_seller = extracts[c - 1]['VendorName']['content'].strip()
        prev_seller = re.sub('[^A-Za-z0-9]+', '', prev_seller)
    except:
        prev_seller = ''

    logger3.info(f'CURRENT PAGE - {c - 1}, Invoice Number is {prev_inv}')
    
    while c < len(extracts.keys()):

        try:
            cond_inv = (extracts[c]['InvoiceId']['content'].strip() == prev_inv.strip())
        except:
            cond_inv = ("" == prev_inv)

        try:
            cond_po = (extracts[c]['PurchaseOrder']['content'].strip() == prev_po.strip())
            po_number_new = extracts[c]['PurchaseOrder']['content'].strip()
        except:
            cond_po = ("" == prev_po)
            po_number_new = ''
        
        try:
            cond_date = (extracts[c]['InvoiceDate']['content'].strip() == prev_date.strip())
        except:
            cond_date = ('' == prev_date)

        try:
            cond_seller = (re.sub('[^A-Za-z0-9]+', '', extracts[c]['VendorName']['content'].strip()) == prev_seller)
        except:
            cond_seller = ('' == prev_seller)

        logger3.info(f"CURRENT PAGE - {c}, Condition is {cond_inv}")

        if cond_inv:
            c_page[1] = c
            if c == len(extracts.keys()) - 1:
                if po_number_new == "" and prev_po != "":
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": prev_po,
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)
                elif po_number_new == "" and prev_po == "":
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'NON_PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": "",
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)
                else:
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'NON_PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": "",
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)

        elif 'InvoiceId' not in extracts[c].keys():
            c_page[1] = c
            if c == len(extracts.keys()) - 1:
                if po_number_new == "" and prev_po != "":
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": prev_po,
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)
                elif po_number_new == "" and prev_po == "":
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'NON_PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": "",
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)
                else:
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'NON_PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": "",
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)

        elif (po_number_new != '') and (cond_inv and cond_po):

            c_page[1] = c
            try:
                prev_inv = extracts[c]['InvoiceId']['content'].strip()
            except:
                prev_inv = ''
            try:
                prev_po = extracts[c]['PurchaseOrder']['content'].strip()
            except:
                prev_po = ''
            try:
                prev_date = extracts[c]['InvoiceDate']['content'].strip()
            except:
                prev_date = ''
            try:
                prev_seller = extracts[c]['VendorName']['content'].strip()
                prev_seller = re.sub('[^A-Za-z0-9]+', '', prev_seller)
            except:
                prev_seller = ''
    
            if c == len(extracts.keys()) - 1:
                if po_number_new != "" or prev_po != "":
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": prev_po,
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)
                else:
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'NON_PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": "",
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)
            
        elif (po_number_new == '') and (cond_inv):

            c_page[1] = c
            try:
                prev_inv = extracts[c]['InvoiceId']['content'].strip()
            except:
                prev_inv = ''
            try:
                prev_po = extracts[c]['PurchaseOrder']['content'].strip()
            except:
                prev_po = ''
            try:
                prev_date = extracts[c]['InvoiceDate']['content'].strip()
            except:
                prev_date = ''
            try:
                prev_seller = extracts[c]['VendorName']['content'].strip()
                prev_seller = re.sub('[^A-Za-z0-9]+', '', prev_seller)
            except:
                prev_seller = ''

            if c == len(extracts.keys()) - 1:

                if po_number_new != "" or prev_po != "":
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": prev_po,
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)

                else:
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'NON_PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": "",
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)
            
        else:
            if po_number_new != "":
                curr_item = {
                    "documentType": 'INVOICE',
                    "documentSubType": 'PO_INVOICE',
                    "invoiceNumber": prev_inv,
                    "apaasPONumber": prev_po,
                    "sellerName": prev_seller,
                    "invoiceDate": prev_date,
                    "startPage": c_page[0],
                    "endPage": c_page[1],
                    "apaasERPName": ""
                }
                splits.append(curr_item)

            else:
                curr_item = {
                    "documentType": 'INVOICE',
                    "documentSubType": 'NON_PO_INVOICE',
                    "invoiceNumber": prev_inv,
                    "apaasPONumber": "",
                    "sellerName": prev_seller,
                    "invoiceDate": prev_date,
                    "startPage": c_page[0],
                    "endPage": c_page[1],
                    "apaasERPName": ""
                }
                splits.append(curr_item)

            c_page = [c, c]
            try:
                prev_inv = extracts[c]['InvoiceId']['content'].strip()
            except:
                prev_inv = ''
            try:
                prev_po = extracts[c]['PurchaseOrder']['content'].strip()
            except:
                prev_po = ''
            try:
                prev_date = extracts[c]['InvoiceDate']['content'].strip()
            except:
                prev_date = ''
            try:
                prev_seller = extracts[c]['VendorName']['content'].strip()
                prev_seller = re.sub('[^A-Za-z0-9]+', '', prev_seller)
            except:
                prev_seller = ''


            if c == len(extracts.keys()) - 1:
                if po_number_new != "" or prev_po != "":
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": prev_po,
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)
                else:
                    curr_item = {
                        "documentType": 'INVOICE',
                        "documentSubType": 'NON_PO_INVOICE',
                        "invoiceNumber": prev_inv,
                        "apaasPONumber": "",
                        "sellerName": prev_seller,
                        "invoiceDate": prev_date,
                        "startPage": c_page[0],
                        "endPage": c_page[1],
                        "apaasERPName": ""
                    }
                    splits.append(curr_item)

        c = c + 1

    return True, splits