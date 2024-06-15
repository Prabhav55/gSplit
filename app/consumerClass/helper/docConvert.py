'''
Helper To Convert TIFF and PDF
Functionality:
    - Convert TIFF/Image to PDF.
'''

# Base Imports
import os
from PIL import Image, ImageSequence

def tiff_to_pdf(tiff_path: str) -> str:
 
    pdf_path = tiff_path.replace('.tiff', '.pdf')
    if not os.path.exists(tiff_path): raise Exception(f'{tiff_path} does not find.')
    image = Image.open(tiff_path)

    images = []
    for i, page in enumerate(ImageSequence.Iterator(image)):
        page = page.convert("RGB")
        images.append(page)
    if len(images) == 1:
        images[0].save(pdf_path)
    else:
        images[0].save(pdf_path, save_all=True,append_images=images[1:])

    return pdf_path

def img_to_pdf(img_path):

    pdf_path = img_path.split('.')[0] + '.pdf'
    curr_image = Image.open(img_path)
    try:
        curr_image.save(pdf_path, "PDF" ,resolution=100.0)
    except Exception as e:
        curr_image = curr_image.convert('RGB')
        curr_image.save(pdf_path, "PDF" ,resolution=100.0)

    return pdf_path
