from django.urls import path
from minio_bucket_app.api.api import transfer_txt_files

urlpatterns = [

    path('transfer/', transfer_txt_files, name='transfer_txt_files'),
]
