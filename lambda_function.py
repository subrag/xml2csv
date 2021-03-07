import requests
import zipfile
import io
import xml.etree.ElementTree as ElementTree
import pandas as pd
from pathlib import Path
import os
import logging
import boto3
from botocore.exceptions import ClientError
import urllib.request

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', logging.INFO))

link = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/" \
       "select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&" \
       "wt=xml&indent=true&start=0&rows=100"


def download_extract_zip(zip_file_url, zip_folder):
    """
    Download all files from zip link
    :param zip_file_url: url to get zip file
    :param zip_folder: folder to store unzipped files
    :return:
    """
    r = requests.get(zip_file_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(zip_folder)


def download(path='/tmp/source.xml'):
    """
    Download source.xml file from the link and stores
    in tmp or specified path
    """
    with urllib.request.urlopen(link) as f:
        data = f.read()
        with open(path, 'wb') as xml:
            xml.write(data)


def clean_folder(path):
    """
    deletes all the files inside specified path
    :param path: folder path to delete files
    :return:
    """
    for root, dirs, files in os.walk(path):
        for file in files:
            os.remove(os.path.join(root, file))


def read_xml2csv_upload(file_path, zip_folder, csv_folder, s3_bucket):
    """
    Function to fetch all DLTINS files and convert to xml2csv
    :param file_path: source xml file(source.xml)
    :param zip_folder: folder to store unzipped files
    :param csv_folder: folder to store converted csv files
    :param s3_bucket: s3 bucket to store csv files
    :return:
    """
    tree = ElementTree.parse(file_path)
    root = tree.getroot()
    result = root.findall('result')[0]

    filter_doc = list(filter(lambda document: filter(
        lambda element: element.attrib['name'] == 'file_type' and
                        element.text == 'DLTINS', document), result))
    for doc in filter_doc:
        for x in doc:
            if x.attrib['name'] == 'download_link':
                download_extract_zip(x.text, zip_folder)
                for file in list(filter(lambda f: f.endswith('.xml'), os.listdir(zip_folder))):
                    convert_xml2csv(f'{zip_folder}/{file}',
                                    f"{csv_folder}{file.split('.')[0]}.csv")
                for file in list(filter(lambda x: x.endswith('.csv'), os.listdir(csv_folder))):
                    upload_file(f"{csv_folder}{file}", s3_bucket, file)
            # delete processed files
            clean_folder(zip_folder)
            clean_folder(csv_folder)


def convert_xml2csv(source, destination):
    """
    transformer function to convert source xml to csv in the
    specified format
    :param source: source xml data path
    :param destination: convert csv storage path
    :return: None
    """
    logging.info(f"converting xml2csv - {source}")
    try:
        tree = ElementTree.parse(source)
        os.remove(source)
        data = []
        for i in tree.iter():
            if 'FinInstrmGnlAttrbts' in i.tag:
                d = {}
                for element in i.iter():
                    if 'Id' in element.tag:
                        d['FinInstrmGnlAttrbts.Id'] = element.text
                    if 'FullNm' in element.tag:
                        d['FinInstrmGnlAttrbts.FullNm'] = element.text
                    if 'ClssfctnTp' in element.tag:
                        d['FinInstrmGnlAttrbts.ClssfctnTp'] = element.text
                    if 'NtnlCcy' in element.tag:
                        d['FinInstrmGnlAttrbts.NtnlCcy'] = element.text
                    if 'CmmdtyDerivInd' in element.tag:
                        d['FinInstrmGnlAttrbts.CmmdtyDerivInd'] = element.text
                data.append(d)
    except Exception as ex:
        logger.error(f"Exception Occurred: {ex}")
        logger.debug(f"xml2csv failed for {source}")
    df = pd.DataFrame(data)
    df.to_csv(destination)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then
    file_name is used
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
        logger.error(e)


def lambda_handler(event, context):
    """
    Lambda function handler
    :param event:
    :param context:
    :return:
    """
    zip_folder = '/tmp/zip/'
    csv_folder = '/tmp/csv/'
    s3_bucket = os.environ.get('s3_bucket', 'assignment-bucket-2')
    Path(csv_folder).mkdir(parents=True, exist_ok=True)
    Path(zip_folder).mkdir(parents=True, exist_ok=True)
    download('/tmp/source.xml')
    read_xml2csv_upload('/tmp/source.xml', zip_folder, csv_folder, s3_bucket)


if __name__ == "__main__":
    lambda_handler(None, None)



