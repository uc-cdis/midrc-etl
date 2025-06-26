import os
import boto3
import botocore
import json
import logging
import paramiko

from time import strftime
from botocore.exceptions import BotoCoreError


logger = logging.getLogger()
logger.setLevel(os.getenv("LOGGING_LEVEL", "INFO"))


def get_rsa_private_key_from_secrets(
    secret_name="sftp-ri-rsa-key", region_name="us-east-1"
):
    """
    Retrieve a specific RSA private key from AWS Secrets Manager.

    :param secret_name: Name of the secret in AWS Secrets Manager
    :param region_name: AWS region where the secret is stored
    :return: RSA private key string
    """
    try:
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        secret_string = client.get_secret_value(SecretId=secret_name)["SecretString"]
    except botocore.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            logger.error(
                "The secret '{}' was not found in AWS Secrets Manager.".format(
                    secret_name
                )
            )
        else:
            logger.error(
                "A client error occurred while retrieving the secret: {}".format(e)
            )
        raise
    try:
        # To parse multi-line text from a json formatted string, escape all `\n` with `\\n`
        parsed_secret = json.loads(secret_string.replace("\n", "\\n"))
    except json.JSONDecodeError:
        raise ValueError("SecretString -- {} is not valid JSON.".format(secret_string))

    logger.info("Fetching 'rsa_private_key' value from '{}'".format(secret_name))
    rsa_private_key = parsed_secret.get("rsa_private_key")
    return rsa_private_key


# read in shared properties on module load - will fail hard if any are missing
SSH_HOST = os.environ["SSH_HOST"]
SSH_USERNAME = os.environ["SSH_USERNAME"]
# must have one of pwd / key - fail hard if both are missing
SSH_PASSWORD = os.getenv("SSH_PASSWORD")
# path to a private key file on S3 in 'bucket:key' format.
SSH_PRIVATE_KEY = get_rsa_private_key_from_secrets()
assert SSH_PASSWORD or SSH_PRIVATE_KEY, "Missing SSH_PASSWORD or SSH_PRIVATE_KEY"
# optional
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_DIR = os.getenv("SSH_DIR")
# filename mask used for the remote file
SSH_FILENAME = os.getenv("SSH_FILENAME", "data_{current_date}")
private_key_path = None  # used to store the private key file temporarily


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
    Note: RSA key to connect to the server, if required, must be present in secrets manager with key name `sftp-ri-rsa-key`,
    Args:
        event: dict, the event payload delivered by Lambda.
        context: a LambdaContext object - unused.
    """
    if SSH_PRIVATE_KEY:
        private_key_path = "/tmp/id_rsa"
        f = open(private_key_path, "w")
        f.write(SSH_PRIVATE_KEY)
        f.close()

    # prefix all logging statements - otherwise impossible to filter out in
    # Cloudwatch
    logger.info("S3-SFTP: received trigger event")

    sftp_client, transport = connect_to_sftp(
        hostname=SSH_HOST,
        port=SSH_PORT,
        username=SSH_USERNAME,
        password=SSH_PASSWORD,
        pkey=private_key_path,
    )
    if SSH_DIR:
        sftp_client.chdir(SSH_DIR)
        logger.debug("S3-SFTP: Switched into remote SFTP upload directory")

    with transport:
        transfer_failure = False
        for s3_file in s3_files(event):
            # Files are uploaded to the SSH_DIR (if set), or to the root directory of the SFTP server
            filename = s3_file.key

            # Regenstrief limitations prevented creating date-based directories.
            # Uncomment below to enable date-based folder structure if supported.

            # current_date = strftime("%Y_%m_%d")
            # filename = "midrc_input/{}/{}".format(current_date, s3_file.key)

            try:
                logger.info(
                    "S3-SFTP: Transferring S3 file '{}' to SFTP".format(filename)
                )
                transfer_file(sftp_client, s3_file, filename)
            except BotoCoreError as ex:
                logger.exception(
                    "S3-SFTP: Error transferring S3 file '{}'.\nException: {}".format(
                        filename, ex
                    )
                )
                transfer_failure = True

    if transfer_failure:
        raise Exception(
            "S3-SFTP: Transferring one or more files failed. Look the logs above for more information."
        )


def connect_to_sftp(hostname, port, username, password, pkey):
    """Connect to SFTP server and return client object."""
    transport = paramiko.Transport((hostname, port))

    k = paramiko.RSAKey.from_private_key_file(pkey) if pkey else None
    transport.connect(username=username, password=password, pkey=k)
    client = paramiko.SFTPClient.from_transport(transport)
    logger.debug("S3-SFTP: Connected to remote SFTP server")
    return client, transport


def s3_files(event):
    """
    Yields a boto3 S3 Object for each file created in an S3-triggered Lambda event.

    This function loops through all the records in the payload,
    checks that the event is a file creation, and if so, yields a
    boto3.Object that represents the file.

    Note:
    * Redshift will trigger an `ObjectCreated:CompleteMultipartUpload` event
    while UNLOADing the data;
    * if you upload to S3 directly, then this will trigger `ObjectCreated:Put`

    Args:
        event: dict, the payload received from the Lambda trigger.
            See tests.py::TEST_RECORD for a sample.
    """
    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]

        event_type, event_subcategory = record["eventName"].split(":")
        if event_type == "ObjectCreated":
            logger.info(
                "S3-SFTP: Received '{}' trigger on '{}'".format(
                    event_subcategory, object_key
                )
            )
            yield boto3.resource("s3").Object(bucket_name, object_key)
        else:
            logger.warning(
                "S3-SFTP: Ignoring invalid event: {} for {}. record: {}".format(
                    event_type, object_key, record
                )
            )


def create_dir_with_parents(sftp_client, remote_path):
    """
    Creates a directory on the SFTP server, including all parent directories.

    This function creates the specified directory and all its parent directories
    on the SFTP server. If the directory already exists, it will not raise an error.

    Args:
        sftp_client (paramiko.SFTPClient): An active SFTP client connected to the target server.
        remote_path (str): The full path of the directory to create on the SFTP server.

    """
    parts = remote_path.strip("/").split("/")
    path = ""
    for part in parts:
        path = os.path.join(path, part)
        try:
            sftp_client.mkdir(path)
            logger.debug("S3-SFTP: Created directory '{}' on SFTP server".format(path))
        except OSError as ex:
            logger.debug(
                "S3-SFTP: Exception: {}. Assuming directory '{}' already exists on SFTP server and proceeding.".format(
                    ex, path
                )
            )


def transfer_file(sftp_client, s3_file, remote_filename):
    """
    Transfers an S3 file to a remote SFTP server.

    This function downloads the specified S3 object and writes it directly to
    the target path on the SFTP server using the provided SFTP client.

    Args:
        sftp_client (paramiko.SFTPClient): An active SFTP client connected to the target server.
        s3_file (boto3.Object): The S3 file to transfer.
        remote_filename (str): The full path (including filename) on the SFTP server.

    """
    remote_file_dir = os.path.dirname(remote_filename)

    if remote_file_dir:
        create_dir_with_parents(sftp_client, remote_file_dir)

    with sftp_client.file(remote_filename, "w") as sftp_file:
        s3_file.download_fileobj(Fileobj=sftp_file)
    logger.info(
        "S3-SFTP: Transferred '{}' from S3 to SFTP as '{}'".format(
            s3_file.key, remote_filename
        )
    )
