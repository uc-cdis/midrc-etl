from io import BytesIO
import zipfile, boto3
import botocore
import hashlib
from base import logger, cfg

AWS_ACCESS_KEY_ID = cfg["aws"]["access_key"]
AWS_ACCESS_SECRET_ACCESS_KEY = cfg["aws"]["secret_key"]
AWS_STORAGE_BUCKET_NAME = cfg["aws"]["bucket"]


def getPackageSize(package_name):
    try:
        package = boto3.resource("s3").Object(AWS_STORAGE_BUCKET_NAME, package_name)
        package_size = package.content_length
    except botocore.exceptions.ClientError as err:
        logger.error(err)
        raise
    return package_size


def getPackageUrl(package_name):
    return "s3://" + AWS_STORAGE_BUCKET_NAME + "/" + package_name


# opens a folder in s3 and package its contents into zip file
def createZipFileStream(bucket_name, directory_name, jobKey, fileExt):
    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_ACCESS_SECRET_ACCESS_KEY,
    )
    bucket = s3_resource.Bucket(bucket_name)
    filesCollection = bucket.objects.filter(Prefix=directory_name).all()
    archive = BytesIO()

    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zip_archive:
        for file in filesCollection:
            if file.key.endswith("." + fileExt):
                with zip_archive.open(file.key, "w") as package_file:
                    package_file.write(file.get()["Body"].read())

    # moves the pointer to head so its not empty when the zip file created
    archive.seek(0)
    # to calculate md5sum of package
    filehash = hashlib.md5()
    filehash.update(open(archive).read())
    md5 = filehash.hexdigest()
    package_name = directory_name + "/" + jobKey + ".zip"
    s3_resource.Object(bucket_name, package_name).upload_fileobj(archive)
    archive.close()
    return package_name, md5
