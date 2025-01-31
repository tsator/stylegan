import boto3
import os
from pathlib import Path

def subir_s3(version):
    s3_client = boto3.client('s3')
    folder_path = 'salida'
    bucket_name = 'avedian-ml'
    s3_prefix = f'stylegan/resultados/{version}'  # opcional
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            local_file = os.path.join(subdir, file)
            # Crear la ruta completa en S3
            relative_path = os.path.relpath(local_file, folder_path)
            s3_path = os.path.join(s3_prefix, relative_path)
            # Subir el archivo
            print(f"Subiendo {local_file} a {s3_path}")
            s3_client.upload_file(local_file, bucket_name, s3_path)



