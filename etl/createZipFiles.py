from io import BytesIO
import zipfile, boto3
import botocore
import yaml
import hashlib
from base import s3_resource, AWS_STORAGE_BUCKET_NAME, logger


with open("config.yml", "rt", encoding="utf8") as ymlfile:
    cfg = yaml.safe_load(ymlfile)


def getPackageSize(package_name):
    try:
        package = boto3.resource("s3").Object(AWS_STORAGE_BUCKET_NAME, package_name)
        package_size = package.content_length
    except botocore.exceptions.ClientError:
        package_size = None
        pass
    return package_size


def getPackagemd5(package_name):
    try:
        package = (
            boto3.client("s3")
            .object(Bucket=AWS_STORAGE_BUCKET_NAME, Key=package_name)
            .get()["Body"]
            .read()
        )
        filehash = hashlib.md5()
        filehash.update(package)
    except botocore.exceptions.ClientError as err:
        logger.error(
            "the following occured while processing md5sum for the %s", package_name
        )
        logger.error(err)
    return filehash.hexdigest()


def getPackageUrl(package_name):
    return "s3://" + AWS_STORAGE_BUCKET_NAME + "/" + package_name


# opens a folder in s3 and package its contents into zip file
def createZipFileStream(bucket_name, directory_name, jobKey, fileExt):
    bucket = s3_resource.Bucket(bucket_name)
    filesCollection = bucket.objects.filter(Prefix=directory_name).all()
    archive = BytesIO()

    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zip_archive:
        for file in filesCollection:
            if file.key.endswith("." + fileExt):
                with zip_archive.open(file.key, "w") as package_file:
                    package_file.write(file.get()["Body"].read())

    archive.seek(
        0
    )  # moves the pointer to head so its not empty when the zip file created
    s3_resource.Object(
        bucket_name, directory_name + "/" + jobKey + ".zip"
    ).upload_fileobj(archive)
    package_name = directory_name + "/" + jobKey + ".zip"
    archive.close()
    return package_name
