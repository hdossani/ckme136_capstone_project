import boto3
import json

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

rawbucket = s3_resource.Bucket('ckme136.twitter.raw')

for file in rawbucket.objects.all():
    print(file.key, sep='\n')






