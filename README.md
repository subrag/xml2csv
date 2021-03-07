# **Assignment**
- Downloads source xml files
- Unzip files from download-link based on file type
- Convert xml to csv specific columns
- Upload to S3 bucket


Running testcase
`python -m unittest discover -s .`


**Note:**
- Code uses `python3`:3.6+
- PEP8 convention: Allowed Max line 100


**Running in Lambda**
- Role based access being used with s3 put access policy along with default Lambda related policies.
- Set Memory (MB) >= 4096
- Set Timeout >=75s