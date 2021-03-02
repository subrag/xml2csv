import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path

import logging
import boto3
from botocore.exceptions import ClientError


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
        with open('source.xml', 'wb') as xml:
            xml.write(data)


def read_source_xml(file_path, zip_folder):
    tree = ET.parse(file_path)
    root = tree.getroot()
    result = root.findall('result')[0] # refactor

    f1 = list(filter(lambda doc: filter(lambda x: x.attrib['name']=='file_type' and x.text=='DLTINS', doc), result))
    for doc in f1:
        for x in doc:
            if x.attrib['name'] == 'download_link':
                download_extract_zip(x.text, zip_folder)


def convert_xml2csv(source, dest):
    tree = ET.parse(source)
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
    zip_folder = './zip/'
    csv_folder = './csv'
    Path(csv_folder).mkdir(parents=True, exist_ok=True)
    Path(zip_folder).mkdir(parents=True, exist_ok=True)
    download()
    read_source_xml('./source.xml', zip_folder)
    # convert_xml2csv(f'{zip_folder}/DLTINS_20210118_01of01.xml', 'DLTINS_20210118_01of01.csv')
    for file in list(filter(lambda x: x.endswith('.xml'), os.listdir(zip_folder))):
        convert_xml2csv(f'{zip_folder}/{file}', f"{csv_folder}{file.split('.')[0]}.csv")
    for file in list(filter(lambda x: x.endswith('.csv'), os.listdir(csv_folder))):
        upload_file(f"{csv_folder}/{file}", 'assignment-bucket-2')


if __name__ == "__main__":
    run_assignment()




