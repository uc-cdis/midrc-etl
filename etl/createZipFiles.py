from io import BytesIO
import zipfile, boto3
import botocore
import yaml


with open("config.yml", "rt", encoding="utf8") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

AWS_ACCESS_KEY_ID = cfg["aws"]["access_key"]
AWS_ACCESS_SECRET_ACCESS_KEY = cfg["aws"]["secret_key"]
AWS_STORAGE_BUCKET_NAME = cfg["aws"]["bucket"]

aws_session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_ACCESS_SECRET_ACCESS_KEY,
)

s3 = aws_session.resource("s3")
s3 = boto3.client("s3", region_name="us-east-1")
s3_resource = boto3.resource(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_ACCESS_SECRET_ACCESS_KEY,
)


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
        md5sum = boto3.client("s3").head_object(
            Bucket=AWS_STORAGE_BUCKET_NAME, Key=package_name
        )["ETag"][1:-1]
    except botocore.exceptions.ClientError:
        md5sum = None
        pass
    return md5sum


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
    )  # moves the pointer to head s its not empty when the zip file created
    s3_resource.Object(
        bucket_name, directory_name + "/" + jobKey + ".zip"
    ).upload_fileobj(archive)
    package_name = directory_name + "/" + jobKey + ".zip"
    archive.close()
    return package_name
