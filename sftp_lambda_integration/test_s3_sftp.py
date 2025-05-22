import os

# Update environment variables for testing before importing the module
os.environ["SSH_HOST"] = "[Redacted]"
os.environ["SSH_USERNAME"] = "[Redacted]"
ssh_file_path = os.path.expanduser("~/path/to/ssh_key/id_rsa")
with open(ssh_file_path, "r") as f:
    os.environ["SSH_PRIVATE_KEY"] = f.read()
os.environ["SSH_DIR"] = "/home/qa-midrc/"

# import the module to be tested
from regenstrief_s3_sftp import lambda_handler


test_event = {
    "Records": [
        {
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {
                    "name": "replace_with_your_bucket_name",
                },
                "object": {
                    "key": "replace_with_your_object_key",
                },
            },
        }
    ]
}

# Call the lambda_handler function with the test event
lambda_handler(test_event, None)


# Happy path!
# The lambda_handler function should pull the file from S3 and send it to the SFTP server.
