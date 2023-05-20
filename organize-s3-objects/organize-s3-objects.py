
import boto3
from datetime import datetime


def lambda_handler(events, context):

    # let's create s3 client
    s3_client = boto3.client("s3")

    # get the date and folder name
    folder_name = datetime.now().strftime('%Y%m%d') + "/"

    # let's list objets of bucket 
    bucket_name = "hiroyito-organized-s3-objects"
    list_s3_objects = s3_client.list_objects_v2(Bucket=bucket_name)
    s3_files_name = list_s3_objects.get("Contents")
    s3_files_name_list = [ item.get("Key") for item in s3_files_name]

    # create a folder to s3
    if folder_name not in s3_files_name_list:
        s3_client.put_object(Bucket=bucket_name, Key=folder_name)

    # organize files into folder
    for item in s3_files_name:
        object_creation_date = item.get('LastModified').strftime('%Y%m%d') + '/'
        object_name = item.get('Key')
    
        if object_creation_date == folder_name and "/" not in object_name:
            s3_client.copy_object(
                Bucket = bucket_name,
                CopySource = f"{bucket_name}/{object_name}",
                Key =f"{folder_name}{object_name}"
            )

            s3_client.delete_object(
                Bucket = bucket_name,
                Key = object_name
            )
    print(f"Please check your s3 bucket: {bucket_name}")




