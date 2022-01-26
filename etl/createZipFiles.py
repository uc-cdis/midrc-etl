from io import BytesIO
import  zipfile, boto3
AWS_ACCESS_KEY_ID = "AKIAJPNBVVE3SUVO3SAA"
AWS_ACCESS_SECRET_ACCESS_KEY = "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"
AWS_STORAGE_BUCKET_NAME = "qaplanetv2-qa-midrc-707767160287-upload"
folder = "05539ef4-137e-47c4-a807-a7be0eb9ec40/test"
aws_session = boto3.Session(aws_access_key_id = AWS_ACCESS_KEY_ID,
                   aws_secret_access_key = AWS_ACCESS_SECRET_ACCESS_KEY)

s3 = aws_session.resource("s3")




s3 = boto3.client("s3", region_name = "us-east-1")
s3_resource = boto3.resource("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
         aws_secret_access_key= AWS_ACCESS_SECRET_ACCESS_KEY)

def createZipFileStream(bucketName, bucketFilePath, jobKey, fileExt, createUrl=False):
    response = {} 
    bucket = s3_resource.Bucket(bucketName)
    filesCollection = bucket.objects.filter(Prefix=bucketFilePath).all() 
    archive = BytesIO()

    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as zip_archive:
        for file in filesCollection:
            #if file.key.endswith('.' + fileExt):   
                with zip_archive.open(file.key, 'w') as file1:
                    file1.write(file.get()['Body'].read())  

    archive.seek(0)
    s3_resource.Object(bucketName, bucketFilePath + '/' + jobKey + '.zip').upload_fileobj(archive)
    archive.close()

    response['fileUrl'] = None

    if createUrl is True:
        s3Client = boto3.client('s3')
        response['fileUrl'] = s3Client.generate_presigned_url('get_object', Params={'Bucket': bucketName,
                                                                                    'Key': '' + bucketFilePath + '/' + jobKey + '.zip'},
                                                              ExpiresIn=3600)

    


createZipFileStream(AWS_STORAGE_BUCKET_NAME,folder,"test",".dcm")