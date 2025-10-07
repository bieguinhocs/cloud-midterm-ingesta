import requests
import pandas as pd
import boto3
import os
from datetime import datetime

API_URL = "http://3.89.118.40:3000/users"
S3_BUCKET = "midterm-data-ingesta"
RAW_PATH = "raw/mongo"

def fetch_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()

def save_local(data):
    os.makedirs(RAW_PATH, exist_ok=True)
    filename = f"{RAW_PATH}/node_users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return filename

def upload_s3(file_path):
    s3 = boto3.client("s3")
    s3.upload_file(file_path, S3_BUCKET, os.path.basename(file_path))

def main():
    print("Obteniendo datos desde Node API...")
    data = fetch_data()
    print(f"{len(data)} registros obtenidos")
    file_path = save_local(data)
    print(f"Archivo guardado: {file_path}")
    upload_s3(file_path)
    print("Archivo cargado a S3 correctamente")

if __name__ == "__main__":
    main()