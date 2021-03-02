import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
import os
import logging
import boto3
from botocore.exceptions import ClientError
import json


link = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/" \
       "select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&" \
       "wt=xml&indent=true&start=0&rows=100"


def download_extract_zip(zip_file_url, zip_folder):
    r = requests.get(zip_file_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(zip_folder)


def download():
    import urllib.request
    with urllib.request.urlopen(link) as f:
        data = f.read()
        with open('/tmp/source.xml', 'wb') as xml:
            xml.write(data)


def clean_folder(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            os.remove(os.path.join(root, file))


def read_source_xml(file_path, zip_folder, csv_folder):
    tree = ET.parse(file_path)
    root = tree.getroot()
    result = root.findall('result')[0] # refactor

    f1 = list(filter(lambda doc: filter(lambda x: x.attrib['name']=='file_type' and x.text=='DLTINS', doc), result))
    for doc in f1:
        for x in doc:
            if x.attrib['name'] == 'download_link':
                # if x.text !='DLTINS_20210118_01of01.xml':
                #     continue
                download_extract_zip(x.text, zip_folder)
                print("zip folder", os.listdir(zip_folder))
                logging.info("zip_folder", os.listdir(zip_folder))
                for file in list(filter(lambda x: x.endswith('.xml'), os.listdir(zip_folder))):
                    # if file !='DLTINS_20210118_01of01.xml':
                    #     continue
                    convert_xml2csv(f'{zip_folder}/{file}', f"{csv_folder}{file.split('.')[0]}.csv")
                print("csv foder", os.listdir(csv_folder))
                logging.info("csv_folder", os.listdir(csv_folder))
                for file in list(filter(lambda x: x.endswith('.csv'), os.listdir(csv_folder))):
                    x = upload_file(f"{csv_folder}/{file}", 'assignment-bucket-2', file)
                    print('after s3 upload', x)
            # delete files
            clean_folder(zip_folder)
            clean_folder(csv_folder)


def convert_xml2csv(source, dest):
    logging.info('convert_xml2csv')
    tree = ET.parse(source)
    clean_folder('/tmp/zip/')
    data = []
    for i in tree.iter():
        if 'FinInstrmGnlAttrbts' in i.tag:
            d = {}
            for element in i.iter():
                if 'Id' in element.tag:
                    d['Id'] = element.text
                if 'FullNm' in element.tag:
                    d['FullNm'] = element.text
                if 'ClssfctnTp' in element.tag:
                    d['ClssfctnTp'] = element.text
                if 'NtnlCcy' in element.tag:
                    d['NtnlCcy'] = element.text
                if 'CmmdtyDerivInd' in element.tag:
                    d['CmmdtyDerivInd'] = element.text
            data.append(d)
    df = pd.DataFrame(data)
    df.to_csv(dest)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def run_assignment():
    zip_folder = '/tmp/zip/'
    csv_folder = '/tmp/csv/'
    Path(csv_folder).mkdir(parents=True, exist_ok=True)
    Path(zip_folder).mkdir(parents=True, exist_ok=True)
    download()
    read_source_xml('/tmp/source.xml', zip_folder, csv_folder)


if __name__ == "__main__":
    run_assignment()




