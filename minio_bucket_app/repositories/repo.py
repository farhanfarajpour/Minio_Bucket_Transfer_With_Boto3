import boto3
from django.conf import settings
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig,S3Transfer
from django.http import JsonResponse


def s3_client():
    return boto3.client(
        's3',
        endpoint_url=settings.AWS_STORAGE_ENDPOINT,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION_NAME
    )

def list_txt_files(bucket_name):
    s3 = s3_client()
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        contents = response.get('Contents', [])
        return [obj['Key'] for obj in contents if obj['Key'].endswith('.txt')]
    except Exception as e:
        print("Error in list_txt_files:", e)
        return []

def copy_file_between_buckets(source_bucket, destination_bucket, key):
    s3 = s3_client()
    try:
        copy_source = {'Bucket': source_bucket, 'Key': key}
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=key)
        return {'key': key, 'status': 'success'}
    except ClientError as e:
        print(f"Error copying {key}: {e}")
        return {'key': key, 'status': 'failed', 'error': str(e)}

def transfer_txt_files(request):
    txt_files = list_txt_files(settings.SOURCE_BUCKET_NAME)
    transferred_files = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_file = {
            executor.submit(copy_file_between_buckets, settings.SOURCE_BUCKET_NAME, settings.DESTINATION_BUCKET_NAME, key): key
            for key in txt_files
        }
        for future in as_completed(future_to_file):
            result = future.result()
            transferred_files.append(result)

    return JsonResponse({
        'status': 'done',
        'files_transferred': transferred_files,
        'total_files': len(txt_files)
    })