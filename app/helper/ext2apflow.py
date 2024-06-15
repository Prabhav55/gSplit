"""
Use only for APaaS.
Converts glazeDONUT extract to AP Flow JSON Format
"""

import os
import glob
import json
import uuid
import logging
import re
from random import randint, randrange
from dateutil import parser


with open(file='/app_dev/app/etc/listoffields.json', mode='r') as f:
    field_list = json.load(f)

with open(file='/app_dev/app/etc/currency_list.json', mode='r') as f:
    currencies = json.load(f)

with open(file='/app_dev/app/etc/default_mapping.json', mode='r') as f:
    default_values_mapping = json.load(f)

# Set Logging
logger3 = logging.getLogger('3')
logging.basicConfig(format = '%(asctime)s %(message)s',
                    filemode = 'w',
                    level=logging.INFO)
logger3.addHandler(logging.FileHandler('/app_dev/app/logging/other.log'))

"""
HELPER FUNCTIONS
"""

# SET NUMERIC FIELDS
num_fields_header = ['GST', 'HST', 'TAX_VAT', 'QST', 'pst', 
                     'Total_Amount', 'Freight', 'Weights','Fuel_Surcharge', 'Misc_Charges', 'SpecialHandingCharges',
                     'Material_Gross_Amt', 'Discount']
num_fields_list = ['Quantity_Shipped', 'Unit_Price', 'Line_Total_Amount']

# GET UOMs if in Price
def find_uom(input_string):

    input_string = re.sub(r',', '', input_string.strip())
    match = re.search(r'(?<=\d)[\s\/]*([a-zA-Z]+)\s*$', input_string)

    if not match:
        match = re.search(r'^\s*([a-zA-Z]+)\s+(?=\d)', input_string)
    if match:
        return match.group(1).strip()
    return None

# DEFAULT MAPPER
header_defaults = default_values_mapping["header_defaults"]
line_defaults = default_values_mapping["line_defaults"]

# FUNCTION TO UPDATE
def update_with_defaults(data, header_defaults, line_defaults):
    for document in data['documents']:
        for page in document['pages']:
            for header in page['header']:
                field_name = header['name']
                if header['value'] == "":
                    header['value'] = header_defaults.get(field_name, "")
                if header['value'] == "blank":
                    header['value'] = ""
            for line_item in page['lineItems']:
                for item in line_item:
                    field_name = item['name']
                    if item['value'] == "":
                        item['value'] = line_defaults.get(field_name, "")
                    if item['value'] == "blank":
                        item['value'] = ""
    return data

# GET BBOX
def get_bbox(polygon):
    return polygon

# GET PRODUCT CODES
def extract_product_code(text):

    pattern = r'\b[A-Z0-9]+(?:-[A-Z0-9]+)*\b'
    matches = re.findall(pattern, text)
    
    matches_with_numbers = [match for match in matches if any(char.isdigit() for char in match)]
    
    if matches_with_numbers:
        return matches_with_numbers[0]
    else:
        return None

# GET PO AND ZIP
def po_box_zip(address):
    
    pattern = r"(P\.?\s*O\.?\s*Box\s*\d+)|(\d{5}-\d{4}|\d{5})|([A-Z]\d[A-Z] \d[A-Z]\d)"

    extracted_data = {}
    matches = re.findall(pattern, address)
    po_box = ''
    zip_code = ''

    # Filter matches to assign to the correct category
    for match in matches:
        if "Box" in match[0]:
            po_box = match[0]
        elif match[1]:
            zip_code = match[1]
        elif match[2]:
            zip_code = match[2]

    if po_box == None or po_box == '':
        return '', zip_code
    else:
        return po_box, zip_code

# RETURN KEY VALUE PAIR FOR PO NUMBER IN SPLIT CASE
def split_pofinder(extracts, key_val):

    # SEE IF BRANCH NUMBER IS THERE
    branch = False
    for currkey in key_val:
        k = currkey['key']['content']
        try:
            v = currkey['value']
        except Exception as e:
            continue
            
        if 'branch number' in k.lower() or 'branch code' in k.lower():
            bnum = v['content']
            branch = True
            break

    if 'PurchaseOrder' in extracts.keys():
        ponum = extracts['PurchaseOrder']['content']
        if branch:
            if bnum in ponum:
                pass
            elif len(bnum) == 4:
                ponum = bnum + ponum
            elif len(bnum) == 3:
                ponum = ponum + bnum
            else:
                pass
        return ponum
    
    else:
        po_num = ''
        try:
            for currkey in key_val:

                k = currkey['key']['content']
                try:
                    v = currkey['value']
                except Exception as e:
                    continue

                for k2, v2 in field_list['available_key_value_pairs'].items():
                    if v2 == 'PO_Number' and k2.lower() in k.lower():
                        logger3.info(f"Found {v['content']}")
                        po_num = v['content']

        except Exception as e:
            logger3.info(str(e))
            logger3.error('ERROR REPORTED IN KEY VALUE PROCESSING')
            pass

        if branch:
            if bnum in po_num:
                pass
            elif len(bnum) == 4:
                po_num = bnum + po_num
            elif len(bnum) == 3:
                po_num = po_num + bnum
            else:
                pass

        return po_num
    
# CREATE EMPTY EXTRACTION JSON
def error_case(fileName, dimension, caseId):

    header = []

    for h in field_list['header_fields']:

        curr = {
            "name": h,
            "value": "",
            "confidence": 1,
            "bbox": [
                0,
                0,
                0,
                0
            ],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)

    final_line_items = [[]]

    for i in range(len(final_line_items)):
        for cell in field_list["line_item_fields"]:
            curr = {
                "name": cell,
                "value": "",
                "confidence": 1,
                "bbox": [0,0,0,0,0,0,0,0],
                "type": "table",
                "uuid_list": [
                    "1f62803c-944f-401a-b553-e74771a678c2"
                ]
            }
            final_line_items[i].append(curr)

    new_file_name = fileName.split('/')[-1] + '.pdf'

    out = {
        "documents": [
            {
                "pages": [
                    {
                        "pageNo": 1,
                        "header": header,
                        "lineItems": final_line_items,
                        "height": dimension[1],
                        "width": dimension[0],
                        "angle": dimension[2],
                        "unit": dimension[3]
                    }
                ]
            }
        ],
        "caseId": caseId,
        "invoiceFileName": new_file_name
    }

    return out

# =================================================================
# ======================== MAIN CODE ==============================
# =================================================================

def ext2ap(inp, caseId, fileName, key_val, tables, dimension, msg, page_wlist, reject_reason):

    logger3.info(f'IN UDP CONVERSION')

    logger3.info(f'INPUT KEYS ARE:')
    logger3.info(str(inp.keys()))

    # Create Header
    header = []
    added_fields = []
    added_headers = []
    invoice_total = ''
    ship_date = ''

    for k, v in inp.items():

        if k in field_list["available_header_mapping"].keys() and k not in added_fields:

            field_label = field_list["available_header_mapping"][k]

            if k == 'InvoiceDate':
                try:
                    value_field = v['valueDate']
                    logger3.info(f'Date is {value_field}. Changing to standard.')
                    try:
                        parsed_date = parser.parse(value_field, dayfirst=False, yearfirst=False)
                        formatted_date = parsed_date.strftime('%Y-%m-%d')
                        value_field = formatted_date
                        logger3.info(f'New Date is {value_field}')
                    except Exception as e2:
                        logger3.error(e2)
                        pass
                except Exception as e1:
                    logger3.error(e1)
                    value_field = v['content']
                    logger3.info(f'Date is {value_field}. Changing to standard.')
                    try:
                        parsed_date = parser.parse(value_field, dayfirst=False, yearfirst=False)
                        formatted_date = parsed_date.strftime('%Y-%m-%d')
                        value_field = formatted_date
                        logger3.info(f'New Date is {value_field}')
                    except Exception as e2:
                        logger3.error(e2)
                        pass
                ship_date = value_field

            elif k in ['SubTotal', 'TotalDiscount', 'InvoiceTotal', 'TotalTax']:

                if 'valueCurrency' in v.keys():
                    value_field = v['valueCurrency']['amount']
                    try:
                        non_decimal = re.compile(r'[^\d.,]+')
                        value_field = non_decimal.sub('', value_field)
                        value_field = value_field.replace(',', '')
                    except:
                        pass
                    if 'CurrencyCode' in added_fields:
                        pass
                    else:
                        try:
                            try:
                                currency = v['valueCurrency']['currency']
                            except:
                                currency = v['valueCurrency']['currencySymbol']

                            if currency == '$':
                                currency = 'USD'
                            
                            curr2 = {
                                "name": 'Currency',
                                "value": currency,
                                "confidence": 1,
                                "bbox": get_bbox(v['boundingRegions'][0]['polygon']),
                                "type": "form",
                                "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                            }
                            header.append(curr2)
                            added_fields.append('CurrencyCode')
                            added_headers.append('Currency')
                        except:
                            pass
                
                else:
                    value_field = v['content']

            if k == 'InvoiceTotal':
                invoice_total = value_field

            elif k == 'RemittanceAddress':

                try:
                    value_field = inp['RemittanceAddressRecipient']['content'] + ' ' + v['content']
                except Exception as e:
                    value_field = v['content']

                if 'Zip_Code' not in added_headers:
                    try: 
                        pcode, zcode = po_box_zip(v['content'])
                        curr2 = {
                            "name": 'Zip_Code',
                            "value": zcode,
                            "confidence": 1,
                            "bbox": [0,0,0,0,0,0,0,0],
                            "type": "form",
                            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                        }
                        header.append(curr2)
                        added_headers.append('Zip_Code')
                        curr2 = {
                            "name": 'Po_Box',
                            "value": pcode,
                            "confidence": 1,
                            "bbox": [0,0,0,0,0,0,0,0],
                            "type": "form",
                            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                        }
                        header.append(curr2)
                        added_headers.append('Po_Box')
                    except Exception as e:
                        logger3.error(str(e))
                        pass

            elif k == 'ShippingAddress':
                try:
                    value_field = inp['ShippingAddressRecipient']['content'] + ' ' + v['content']
                except:
                    value_field = v['content']

                if 'Zip_Code' not in added_headers:
                    try: 
                        pcode, zcode = po_box_zip(v['content'])
                        curr2 = {
                            "name": 'Zip_Code',
                            "value": zcode,
                            "confidence": 1,
                            "bbox": [0,0,0,0,0,0,0,0],
                            "type": "form",
                            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                        }
                        header.append(curr2)
                        added_headers.append('Zip_Code')
                        curr2 = {
                            "name": 'Po_Box',
                            "value": pcode,
                            "confidence": 1,
                            "bbox": [0,0,0,0,0,0,0,0],
                            "type": "form",
                            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                        }
                        header.append(curr2)
                        added_headers.append('Po_Box')
                    except Exception as e:
                        logger3.error(str(e))
                        pass

            elif k == 'BillingAddress':

                try:
                    value_field = inp['BillingAddressRecipient']['content'] + ' ' + v['content']
                except Exception as e:
                    value_field = v['content']

                logger3.info(f'Billing Address - {value_field}')

                curr2 = {
                    "name": 'BillToAddress',
                    "value": value_field,
                    "confidence": 1,
                    "bbox": [0,0,0,0,0,0,0,0],
                    "type": "form",
                    "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                }
                header.append(curr2)
                added_headers.append('BillToAddress')

                if 'RemittanceAddress' not in inp.keys():
                    curr3 = {
                        "name": 'Remit_To',
                        "value": value_field,
                        "confidence": 1,
                        "bbox": [0,0,0,0,0,0,0,0],
                        "type": "form",
                        "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                    }
                    header.append(curr3)
                    added_headers.append('Remit_To')

                if 'Zip_Code' not in added_headers:
                    try: 
                        pcode, zcode = po_box_zip(v['content'])
                        curr2 = {
                            "name": 'Zip_Code',
                            "value": zcode,
                            "confidence": 1,
                            "bbox": [0,0,0,0,0,0,0,0],
                            "type": "form",
                            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                        }
                        header.append(curr2)
                        added_headers.append('Zip_Code')
                        curr2 = {
                            "name": 'Po_Box',
                            "value": pcode,
                            "confidence": 1,
                            "bbox": [0,0,0,0,0,0,0,0],
                            "type": "form",
                            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                        }
                        header.append(curr2)
                        added_headers.append('Po_Box')
                    except Exception as e:
                        logger3.error(str(e))
                        pass

                continue

            elif k == 'PaymentDetails':
                
                payment_details = ''
                value_field_array = v['valueArray']
                for pd in value_field_array:
                    for pd_item, pd_value in pd['valueObject'].items():
                        payment_details = payment_details + ' ' + pd_value['content']

                value_field = payment_details

            elif k == 'PurchaseOrder':

                value_field = v['content'].split()[0]

                if msg['apaasERPName'] == 'RUBICON' and len(value_field) != 8:
                    needed_zeros = 8 - len(value_field)
                    for x in range(needed_zeros):
                        value_field = '0' + value_field

            else:
                value_field = v['content']


            if k == 'InvoiceDate':
                logger3.info(f'DATE IS {value_field}')

            try:
                curr = {
                    "name": field_list["available_header_mapping"][k],
                    "value": value_field,
                    "confidence": 1,
                    "bbox": get_bbox(v['boundingRegions'][0]['polygon']),
                    "type": "form",
                    "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                }
            except:
                curr = {
                    "name": field_list["available_header_mapping"][k],
                    "value": value_field,
                    "confidence": 1,
                    "bbox": [0,0,0,0,0,0,0,0],
                    "type": "form",
                    "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                }
            header.append(curr)
            added_fields.append(k)
            added_headers.append(field_list["available_header_mapping"][k])

    # isPORestricted
    try:
        curr = {
            "name": 'isPORestricted',
            "value": msg['isPORestricted'],
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
        added_headers.append('isPORestricted')
    except:
        pass

    try:
        # ADDED Reject Reason
        curr = {
            "name": 'Reject_Reason',
            "value": reject_reason,
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
    except:
        curr = {
            "name": 'Reject_Reason',
            "value": "",
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
        logger3.error('No RR Found')
        pass

    try:
        # ADDED ERP NAME
        curr = {
            "name": 'ERP_Name',
            "value": msg['apaasERPName'],
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
        erp_sent = msg['apaasERPName']
    except:
        curr = {
            "name": 'ERP_Name',
            "value": "",
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
        logger3.error('No ERP NAME FOUND')
        erp_sent = ''
        pass

    try:
        # Add PO Type
        curr = {
            "name": 'Po_Type',
            "value": msg['POType'],
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
    except:
        # Add PO Type
        curr = {
            "name": 'Po_Type',
            "value": "",
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
        logger3.error('No PO TYPE FOUND')
        pass

    try:
        # Add VendorName Type
        curr = {
            "name": 'Vendor_Number',
            "value": msg['SupplierNumber'],
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
    except:
        # Add VendorName Type
        curr = {
            "name": 'Vendor_Number',
            "value": "",
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
        logger3.error('No Vendor Number FOUND')
        pass

    try:
        # Add VendorName Type
        curr = {
            "name": 'Reject_Reason',
            "value": msg['exceptions'],
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
    except:
        # Add VendorName Type
        curr = {
            "name": 'Reject_Reason',
            "value": "",
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
        logger3.error('No Reject Reasons FOUND')
        pass

    try:
        # Add Supplier Name Type
        curr = {
            "name": 'Vendor_Name',
            "value": msg['SupplierName'],
            "confidence": 1,
            "bbox": [0,0,0,0,0,0,0,0],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
    except:
        logger3.error('No Vendor Name FOUND')
        pass

    try:
        # Add from Key Value Pairs
        for currkey in key_val:

            k = currkey['key']['content']
            try:
                v = currkey['value']
            except Exception as e:
                continue

            for k2, v2 in field_list['available_key_value_pairs'].items():

                if k2 == 'Currency' and 'Currency' in added_headers:
                    continue
                if k2 == 'PO_Number' and 'PO_Number' in added_headers:
                    continue

                if k2.lower() in k.lower():

                    check = False

                    if k2 in ['GST', 'HST', 'QST', 'PST', 'Harmonized']:
                        value = v['content']
                        check = True
                        if any(c.isalpha() for c in value):
                            continue
                        if '-' in value[1:]:
                            continue
                        try:
                            value = re.findall('[0-9\.]+', value)[0]
                        except:
                            value = 0.00

                    # Hello World
                    if v2 in ['Freight', 'Misc_Charges', 'SpecialHandingCharges', 'Material_Gross_Amt', 'Disc_Amount']:
                        check = True
                        value = v['content']
                        try:
                            value = re.findall('[0-9\.]+', value)[0]
                        except:
                            value = '0.00'

                    logger3.info(f"Key Value: {k2, v2, v['content']} :in invoice!")

                    if v2 == 'PO_Number':
                        value = v['content']
                        check = True
                        if len(value) > 14:
                            value = v['content'][:14]

                    if v2 in ['Currency']:
                        if v['content'] == '$':
                            check = True
                            value = v['content']
                            value = 'USD'
                        elif v['content'] in currencies:
                            pass
                        elif v['content'] not in currencies:
                            for page in page_wlist:
                                for w in page['words']:
                                    if w['content'] in currencies:
                                        check = True
                                        value = w['content']
                                        break
                        else:
                            v['content'] = 'USD'

                    if check:
                        curr = {
                            "name": v2,
                            "value": value,
                            "confidence": 1,
                            "bbox": [0,0,0,0,0,0,0,0],
                            "type": "form",
                            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                        }
                    else:
                        curr = {
                            "name": v2,
                            "value": v['content'],
                            "confidence": 1,
                            "bbox": [0,0,0,0,0,0,0,0],
                            "type": "form",
                            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                        }
                    header.append(curr)
                    added_headers.append(v2)

    except Exception as e:
        logger3.info(str(e))
        logger3.error('ERROR REPORTED IN KEY VALUE PROCESSING')
        pass

    for h in field_list['header_fields']:

        if h in num_fields_header:
            if h in added_headers:
                continue
            else:
                curr = {
                    "name": h,
                    "value": "0.00",
                    "confidence": 1,
                    "bbox": [
                        0,
                        0,
                        0,
                        0
                    ],
                    "type": "form",
                    "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
                }
                header.append(curr)
                added_headers.append(h)

        if (h == 'Remit_To') and ('Remit_To' not in added_headers):
            logger3.info(f"Did not extract remit to headers. Changing to new keys.")
            logger3.info(f"Keys are {inp.keys()}")
            if 'BillingAddress' in inp.keys():
                logger3.info(f"Billing Address found - {inp['BillingAddressRecipient']['content']} + {inp['BillingAddress']['content']}")
                try:
                    new_address = inp['BillingAddressRecipient']['content'] + ' ' + inp['BillingAddress']['content']
                except Exception as e:
                    logger3.error(str(e))
                    logger3.error('Error in getting address from Bill To.')
                    new_address = inp['BillingAddress']['content']
            elif 'VendorAddress' in inp.keys():
                logger3.error('Billing Also Failed. Now moving to Vendor Address.')
                if 'VendorAddress' in inp.keys():
                    try:
                        new_address = inp['VendorName']['content'] + ' ' + inp['VendorAddress']['content']
                    except:
                        new_address = inp['VendorAddress']['content']
                    finally:
                        new_address = ''

            curr = {
                "name": 'Remit_To',
                "value": new_address,
                "confidence": 1,
                "bbox": [
                    0,
                    0,
                    0,
                    0
                ],
                "type": "form",
                "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
            }

            header.append(curr)
            added_headers.append(h)
            continue

        if h == 'OCR_ID':
            curr = {
                "name": 'OCR_ID',
                "value": 'APAAS' + '_' + str(randint(1000000, 9999999)),
                "confidence": 1,
                "bbox": [
                    0,
                    0,
                    0,
                    0
                ],
                "type": "form",
                "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
            }
            header.append(curr)
            added_headers.append(h)
            continue

        if (h == 'Currency' and h not in added_headers):
            curr = {
                "name": 'Currency',
                "value": "USD",
                "confidence": 1,
                "bbox": [
                    0,
                    0,
                    0,
                    0
                ],
                "type": "form",
                "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
            }
            header.append(curr)
            continue

        if h in added_headers or h in ['ERP_Name', 'Po_Type', 'Vendor_Number']:
            continue

        curr = {
            "name": h,
            "value": "",
            "confidence": 1,
            "bbox": [
                0,
                0,
                0,
                0
            ],
            "type": "form",
            "uuid_list": ["9b0256b0-0546-42d0-92b5-d427c5efb7df"]
        }
        header.append(curr)
        added_headers.append(h)

    # REMOVE IF GST/HST/PST Present
    check_vat = False
    for i in header:
        if i['name'] in ['GST', 'HST', 'pst', 'QST'] and i['value'] != '':
            logger3.info('Found GST or other tax types. Keeping VAT Empty.')
            check_vat = True
            break

    if check_vat:
        for i in header:
            if i['name'] == 'TAX_VAT':
                logger3.info(f"TAX VALUE = {i['value']}")
                i['value'] = ''

    for i in header:
        if i['name'] == 'Sub_Total' and i['value'] == '':
            logger3.info(f"Sub Total is empty. Changing value to {invoice_total}")
            i['value'] = invoice_total 
        if i['name'] == 'Ship_Date' and i['value'] != '':
            logger3.info(f"Ship Date is not empty")
            try:
                parsed_date = parser.parse(i['value'], dayfirst=False, yearfirst=False)
                formatted_date = parsed_date.strftime('%Y-%m-%d')
                i['value'] = formatted_date
            except Exception as e:
                logger3.error(e)
                pass
        if i['name'] == 'Ship_Date' and i['value'] == '':
            logger3.info(f"Ship Date is empty. Changing value to {ship_date}")
            i['value'] = ship_date
        if i['name'] == 'Invoice_Date' and i['value'] != '':
            logger3.info(f"Invoice Date is not empty")
            try:
                parsed_date = parser.parse(i['value'], dayfirst=False, yearfirst=False)
                formatted_date = parsed_date.strftime('%Y-%m-%d')
                i['value'] = formatted_date
            except Exception as e:
                logger3.error(e)
                pass
    
    # =============================================================================

    try:

        # Create Line Items
        final_line_items = []
        lineitems = []
        added_fields = []

        # Check for UOM False Item Code False
        uom_present = False
        code_present = False

        for item in inp['Items']['valueArray']:
            for k, v in item['valueObject'].items():
                if k == 'Unit':
                    uom_present = True
                if k == 'ProductCode':
                    code_present = True

        for item in inp['Items']['valueArray']:

            for k, v in item['valueObject'].items():

                if k in field_list["available_line_mapping"].keys():
                    content_line_item = v['content']
                    if k in ['UnitPrice', 'Quantity', 'Amount']:
                        try:
                            non_decimal = re.compile(r'[^\d.,]+')
                            content_line_item = non_decimal.sub('', content_line_item)
                            content_line_item = content_line_item.replace(',', '')
                        except:
                            pass

                    try:
                        if k == 'UnitPrice':
                            value = float(content_line_item)
                            val = f"{value:.9f}"
                            content_line_item = str(val)
                    except:
                        pass

                    if uom_present == False:
                        if k == 'Quantity' or k == 'UnitPrice':
                            logger3.info(f"Quantity or Price is {v['content']}")
                            uom = find_uom(v['content'].strip())
                            logger3.info(f"UOM is found - {uom}")
                            if uom is not None and uom.strip() != '':
                                curr = {
                                    "name": 'Unit_Of_Measure',
                                    "value": uom,
                                    "confidence": 1,
                                    "bbox": get_bbox(v['boundingRegions'][0]['polygon']),
                                    "type": "table",
                                    "uuid_list": [
                                        "1f62803c-944f-401a-b553-e74771a678c2"
                                    ]
                                }
                                added_fields.append('UOM')
                                lineitems.append(curr)
                            else:
                                pass

                    if code_present == False:
                        if k == 'Description':
                            product_code = extract_product_code(content_line_item)
                            if product_code:
                                curr = {
                                    "name": 'ITEM_CODE',
                                    "value": product_code,
                                    "confidence": 1,
                                    "bbox": get_bbox(v['boundingRegions'][0]['polygon']),
                                    "type": "table",
                                    "uuid_list": [
                                        "1f62803c-944f-401a-b553-e74771a678c2"
                                    ]
                                }
                                added_fields.append('ITEM_CODE')
                                lineitems.append(curr)
                            else:
                                pass

                    curr = {
                        "name": field_list["available_line_mapping"][k],
                        "value": content_line_item,
                        "confidence": 1,
                        "bbox": get_bbox(v['boundingRegions'][0]['polygon']),
                        "type": "table",
                        "uuid_list": [
                            "1f62803c-944f-401a-b553-e74771a678c2"
                        ]
                    }
                    added_fields.append(k)
                    lineitems.append(curr)

            final_line_items.append(lineitems)
            lineitems = []

        # ADD LINE NUMBER IF AVAILABLE
        line_number_assigned = []
        indexes = {}

        for ind_table, t in enumerate(tables):
            for cell in t['cells']:
                if 'kind' in cell.keys() and cell['kind'] == 'columnHeader' and ('line' in cell['content'].lower()):
                    if ind_table in indexes.keys():
                        continue
                    indexes[ind_table] = cell['columnIndex']
                if 'kind' in cell.keys() and cell['kind'] == 'columnHeader' and ('Line No.'.lower() in cell['content'].lower() or 'Line Num'.lower() in cell['content'].lower() or 'LineNo'.lower() in cell['content'].lower()):
                    if ind_table in indexes.keys():
                        continue
                    indexes[ind_table] = cell['columnIndex']
                elif 'kind' in cell.keys() and cell['kind'] == 'columnHeader' and ('LN#'.lower() in cell['content'].lower() or 'Line'.lower() in cell['content'].lower() or 'Line No'.lower() in cell['content'].lower()):
                    if ind_table in indexes.keys():
                        continue
                    indexes[ind_table] = cell['columnIndex']
                elif 'kind' in cell.keys() and cell['kind'] == 'columnHeader' and ('ITEM'.lower() in cell['content'].lower() or 'line'.lower() in cell['content'].lower()):
                    if ind_table in indexes.keys():
                        continue
                    indexes[ind_table] = cell['columnIndex']

        for index_of_table, index_of_column in indexes.items():
            for cell in tables[index_of_table]['cells']:
                if 'kind' in cell.keys():
                    continue
                else:
                    if cell['columnIndex'] == index_of_column:
                        if any(c.isalpha() for c in cell['content'].strip()):
                            continue
                        elif '.' in cell['content'].strip() or cell['content'].strip() == '':
                            continue
                        elif '-' in cell['content'].strip() or cell['content'].strip() == '':
                            continue
                        else:
                            line_number_assigned.append(cell['content'])

        # Append Line Numbers in the table
        index_of_line_item = 0
        for index, i in enumerate(final_line_items):
            if index_of_line_item == len(line_number_assigned):
                break
            curr = {
                "name": 'Invoice_Line_Number',
                "value": line_number_assigned[index_of_line_item],
                "confidence": 1,
                "bbox": [0,0,0,0,0,0,0,0],
                "type": "table",
                "uuid_list": [
                    "1f62803c-944f-401a-b553-e74771a678c2"
                ]
            }
            final_line_items[index].append(curr)
            index_of_line_item = index_of_line_item + 1

        # Remove 0 Amount Line items
        final_line_items_new = []
        for i in range(len(final_line_items)):
            include = True
            for cell in final_line_items[i]:
                if cell['name'] == 'Line_Total_Amount' and (cell['value'] == '' or cell['value'] == 0.00):
                    include = False
                    break
            if include:
                final_line_items_new.append(final_line_items[i])
                     
    except Exception as e:
        final_line_items_new = [[]]
        logger3.info(str(e))
        logger3.error('ERROR REPORTED IN TABLE PROCESSING')
        pass

    for i in range(len(final_line_items)):

        added_fields = []

        for cell in final_line_items[i]:
            added_fields.append(cell['name'])

        logger3.info(f"EXTRACTED LINE ITEMS: {added_fields}")

        for cell_name in field_list["line_item_fields"]:
            if cell_name not in added_fields:
                if cell_name in num_fields_list:
                    curr = {
                        "name": cell_name,
                        "value": 0.00,
                        "confidence": 1,
                        "bbox": [0,0,0,0,0,0,0,0],
                        "type": "table",
                        "uuid_list": [
                            "1f62803c-944f-401a-b553-e74771a678c2"
                        ]
                    }
                    added_fields.append(cell_name)
                else:
                    curr = {
                        "name": cell_name,
                        "value": "",
                        "confidence": 1,
                        "bbox": [0,0,0,0,0,0,0,0],
                        "type": "table",
                        "uuid_list": [
                            "1f62803c-944f-401a-b553-e74771a678c2"
                        ]
                    }
                final_line_items[i].append(curr)

    new_file_name = fileName.split('/')[-1] + '.pdf'

    out = {
        "documents": [
            {
                "pages": [
                    {
                        "pageNo": 1,
                        "header": header,
                        "lineItems": final_line_items_new,
                        "height": dimension[1],
                        "width": dimension[0],
                        "angle": dimension[2],
                        "unit": dimension[3]
                    }
                ]
            }
        ],
        "caseId": caseId,
        "invoiceFileName": new_file_name
    }

    out = update_with_defaults(out, header_defaults, line_defaults)

    return out


def send_seg(inp, caseId, fileName):

    with open('/app_dev/app/etc/segments.json', 'r') as f:
        seg = json.load(f)

    return seg
