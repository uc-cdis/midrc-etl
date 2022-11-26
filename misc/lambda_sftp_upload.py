import json
import urllib.parse
import boto3
import paramiko

paramiko.util.log_to_file("paramiko.log")


def sftp_connect(host, port, username, password):
    # open a transport
    transport = paramiko.Transport((host, port))

    # authenticate
    transport.connect(None, username, password)

    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp


print("Loading function")

s3 = boto3.client("s3")


def lambda_handler(event, context):
    sftp = sftp_connect(host, port, username, password)
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    try:
        # response = s3.get_object(Bucket=bucket, Key=key)
        # print("CONTENT TYPE: " + response["ContentType"])

        with sftp.open(remote_path, "wb", 32768) as f:
            s3.download_fileobj(bucket, key, f)

        # return response["ContentType"]
    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                key, bucket
            )
        )
        raise e
