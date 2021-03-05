import unittest

import boto3
import mock
from moto import mock_s3
from main import download, upload_file


class TestCase(unittest.TestCase):
    @mock_s3
    def test_s3_file_upload(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test-bucket')
        upload_file('sample.txt', 'test-bucket', object_name=None)
        body = conn.Object('test-bucket', 'sample.txt').get()['Body'].read().decode("utf-8")
        assert body == 'sample file'



if __name__ == '__main__':
    unittest.main()
