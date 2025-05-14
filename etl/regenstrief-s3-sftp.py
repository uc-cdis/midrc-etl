import boto3
import paramiko
import logging

import botocore.exceptions
import datetime
import io
import os

logger = logging.getLogger()
logger.setLevel(os.getenv("LOGGING_LEVEL", "INFO"))

print("Loading function")

s3 = boto3.client("s3")

# read in shared properties on module load - will fail hard if any are missing
SSH_HOST = os.environ["SSH_HOST"]
SSH_USERNAME = os.environ["SSH_USERNAME"]
# must have one of pwd / key - fail hard if both are missing
SSH_PASSWORD = os.getenv("SSH_PASSWORD")
# path to a private key file on S3 in 'bucket:key' format.
SSH_PRIVATE_KEY = os.getenv("SSH_PRIVATE_KEY")
assert SSH_PASSWORD or SSH_PRIVATE_KEY, "Missing SSH_PASSWORD or SSH_PRIVATE_KEY"
# optional
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_DIR = os.getenv("SSH_DIR")
# filename mask used for the remote file
SSH_FILENAME = os.getenv("SSH_FILENAME", "data_{current_date}")


def connect_to_sftp(hostname, port, username, password, pkey):
    """Connect to SFTP server and return client object."""
    transport = paramiko.Transport((hostname, port))
    k = paramiko.RSAKey.from_private_key_file(pkey) if pkey else None
    transport.connect(username=username, password=password, pkey=k)
    client = paramiko.SFTPClient.from_transport(transport)
    logger.debug(f"S3-SFTP: Connected to remote SFTP server")
    return client, transport


def lambda_handler(event, context):
    """
    Move uploaded S3 files to SFTP endpoint, then delete.
    This is the Lambda entry point. It receives the event payload and
    processes it. In this case it receives a set of 'Record' dicts which
    should contain details of an S3 file create event. The contents of this
    dict can be found in the tests.py::TEST_RECORD - the example comes from
    the Lambda test event rig.
    The only important information we process in this function are the
    `eventName`, which must start with ObjectCreated, and then the bucket name
    and object key.
    This function then connects to the SFTP server, copies the files over.
    Args:
        event: dict, the event payload delivered by Lambda.
        context: a LambdaContext object - unused.
    """
    private_key_path = "/tmp/id_rsa"
    if SSH_PRIVATE_KEY:
        f = open(private_key_path, "w")
        f.write(SSH_PRIVATE_KEY)
        f.close()

    # prefix all logging statements - otherwise impossible to filter out in
    # Cloudwatch
    logger.info(f"S3-SFTP: received trigger event")

    sftp_client, transport = connect_to_sftp(
        hostname=SSH_HOST,
        port=SSH_PORT,
        username=SSH_USERNAME,
        password=SSH_PASSWORD,
        pkey=private_key_path,
    )
    if SSH_DIR:
        sftp_client.chdir(SSH_DIR)
        logger.debug(f"S3-SFTP: Switched into remote SFTP upload directory")

    with transport:
        for s3_file in s3_files(event):
            filename = s3_file.key
            bucket = s3_file.bucket_name
            contents = ""
            try:
                logger.info(f"S3-SFTP: Transferring S3 file '{s3_file.key}'")
                transfer_file(sftp_client, s3_file, filename)
            except botocore.exceptions.BotoCoreError as ex:
                logger.exception(
                    f"S3-SFTP: Error transferring S3 file '{s3_file.key}'."
                )
                contents = str(ex)
                filename = filename + ".x"


def s3_files(event):
    """
    Iterate through event and yield boto3.Object for each S3 file created.
    This function loops through all the records in the payload,
    checks that the event is a file creation, and if so, yields a
    boto3.Object that represents the file.
    NB Redshift will trigger an `ObjectCreated:CompleteMultipartUpload` event
    will UNLOADing the data; if you select to dump a manifest file as well,
    then this will trigger `ObjectCreated:Put`
    Args:
        event: dict, the payload received from the Lambda trigger.
            See tests.py::TEST_RECORD for a sample.
    """
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        event_category, event_subcat = record["eventName"].split(":")
        if event_category == "ObjectCreated":
            logger.info(f"S3-SFTP: Received '{ event_subcat }' trigger on '{ key }'")
            yield boto3.resource("s3").Object(bucket, key)
        else:
            logger.warning(f"S3-SFTP: Ignoring invalid event: { record }")


def sftp_filename(file_mask, s3_file):
    """Create destination SFTP filename."""
    return file_mask.format(
        bucket=s3_file.bucket_name,
        key=s3_file.key.replace("_000", ""),
        current_date=datetime.date.today().isoformat(),
    )


def transfer_file(sftp_client, s3_file, filename):
    """
    Transfer S3 file to SFTP server.

    Args:
        sftp_client: paramiko.SFTPClient, connected to SFTP endpoint
        s3_file: boto3.Object representing the S3 file
        filename: string, the remote filename to use

    Returns a 2-tuple containing the name of the remote file as transferred,
        and any status message to be written to the archive file.

    """
    with sftp_client.file(filename, "w") as sftp_file:
        s3_file.download_fileobj(Fileobj=sftp_file)
    logger.info(
        f"S3-SFTP: Transferred '{ s3_file.key }' from S3 to SFTP as '{ filename }'"
    )
