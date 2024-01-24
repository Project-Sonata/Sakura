import boto3

# Upload the content(bytes) to AWS S3 storage, the bucket name is provided in constructor
class S3Uploader:

    def __init__(self, bucket_name, bucket_region, client_id, client_secret):
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.client_id = client_id
        self.client_secret = client_secret
        self.S3resource = boto3.resource(
            "s3",
            region_name=bucket_region,
            aws_access_key_id=client_id,
            aws_secret_access_key=client_secret
        )

    def uploadFile(self, key, byte_reader, content_type):
        self.S3resource.Bucket(self.bucket_name).upload_fileobj(
            Key=key,
            ExtraArgs={"ContentType": content_type,
                       "ContentDisposition": "inline"},
            Fileobj=byte_reader
        )
