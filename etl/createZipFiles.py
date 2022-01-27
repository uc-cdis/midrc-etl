from io import BytesIO
import  zipfile, boto3
import botocore
import yaml
with open("config.yml", "rt", encoding='utf8') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

AWS_ACCESS_KEY_ID = cfg["aws"]["access_key"]
AWS_ACCESS_SECRET_ACCESS_KEY = cfg["aws"]["secret_key"]
AWS_STORAGE_BUCKET_NAME = cfg["aws"]["bucket"]


aws_session = boto3.Session(aws_access_key_id = AWS_ACCESS_KEY_ID,
                   aws_secret_access_key = AWS_ACCESS_SECRET_ACCESS_KEY)

s3 = aws_session.resource("s3")




s3 = boto3.client("s3", region_name = "us-east-1")
s3_resource = boto3.resource("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
         aws_secret_access_key= AWS_ACCESS_SECRET_ACCESS_KEY)

def getPackageSize(package_name):
    try:
        package = boto3.resource("s3").Object(AWS_STORAGE_BUCKET_NAME,package_name)
        package_size = package.content_length
    except botocore.exceptions.ClientError:
        package_size = None
        pass
    print("package size is: "+package_size)
    return package_size

def getPackagemd5(package_name):
    try:
        md5sum = boto3.client('s3').head_object(
            Bucket=AWS_STORAGE_BUCKET_NAME,
            Key=package_name
        )['ETag'][1:-1]
    except botocore.exceptions.ClientError:
        md5sum = None
        pass
    print("md5sum is: "+md5sum)
    return md5sum

# def getPackageUrl(package_name):
#     return "s3://"+AWS_STORAGE_BUCKET_NAME+"/"+package_name



def createZipFileStream(bucketName, bucketFilePath, jobKey, fileExt, createUrl=False):
    response = {} 
    bucket = s3_resource.Bucket(bucketName)
    filesCollection = bucket.objects.filter(Prefix=bucketFilePath).all() 
    archive = BytesIO()

    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as zip_archive:
        for file in filesCollection:
            if file.key.endswith('.' + fileExt):   
                with zip_archive.open(file.key, 'w') as file1:
                    file1.write(file.get()['Body'].read())  

    archive.seek(0)
    s3_resource.Object(bucketName, bucketFilePath + "/" + jobKey + ".zip").upload_fileobj(archive)
    package_name = bucketFilePath + "/" + jobKey + ".zip"
    archive.close()

    response['fileUrl'] = None

    if createUrl is True:
        s3Client = boto3.client('s3')
        response['fileUrl'] = s3Client.generate_presigned_url('get_object', Params={'Bucket': bucketName,
                                                                                    'Key': '' + bucketFilePath + '/' + jobKey + '.zip'},
                                                              ExpiresIn=3600)

    
    return package_name, response['fileUrl']


