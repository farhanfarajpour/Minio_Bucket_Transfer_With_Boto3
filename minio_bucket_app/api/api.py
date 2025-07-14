from concurrent.futures import ThreadPoolExecutor, as_completed

from django.http import JsonResponse
from minio_bucket_app.repositories.repo import list_txt_files, copy_file_between_buckets
from django.conf import settings
from botocore.exceptions import ClientError

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
