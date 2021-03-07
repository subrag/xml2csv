import unittest
from pathlib import Path

import boto3
import mock
import os
from moto import mock_s3
from lambda_function import upload_file, convert_xml2csv, read_xml2csv_upload
from mock import patch
import filecmp


class TestCase(unittest.TestCase):
    def setUp(self) -> None:
        Path('/tmp/test/zip').mkdir(parents=True, exist_ok=True)
        Path('/tmp/test/csv').mkdir(parents=True, exist_ok=True)

    @mock_s3
    def test_s3_file_upload(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test-bucket')
        upload_file('sample.txt', 'test-bucket', object_name=None)
        body = conn.Object('test-bucket', 'sample.txt').get()['Body'].read().decode("utf-8")
        self.assertTrue(body, 'sample file')

    def test_xml2csv(self):
        with patch('os.remove'):
            convert_xml2csv('./test.xml', './test.csv')
            self.assertTrue(filecmp.cmp('expected.csv', 'test.csv'),
                            'generated file is not matching')

    @mock_s3
    def test_read_and_upload(self):
        s3_bucket = 'assignment-bucket-2'
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=s3_bucket)
        with patch('os.remove'):
            read_xml2csv_upload('./source.xml', '/tmp/test/zip', '/tmp/test/csv', s3_bucket)
        s3 = boto3.client("s3")
        self.assertTrue(s3.list_objects(Bucket='assignment-bucket-2')['Contents'][0]['Key'],
                        'DLTINS_20210118_01of01.csv')


if __name__ == '__main__':
    unittest.main()
