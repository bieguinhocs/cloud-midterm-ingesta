import requests
import pandas as pd
import boto3
from io import StringIO
import os

# Configuración
API_URL = os.getenv("FLASK_API_URL", "http://3.88.184.165:5000/users")
S3_BUCKET = os.getenv("S3_BUCKET", "midterm-data-ingesta")
S3_PATH = "raw/mysql/flask_users.csv"

def main():
    print(f"Obteniendo datos desde {API_URL}...")
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()

    print(f"{len(data)} registros obtenidos. Transformando a CSV...")
    df = pd.DataFrame(data)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    print(f"Subiendo a S3 bucket '{S3_BUCKET}' como '{S3_PATH}'...")
    s3 = boto3.client("s3")
    s3.put_object(Bucket=S3_BUCKET, Key=S3_PATH, Body=csv_buffer.getvalue())

    print("Ingesta completada correctamente.")

if __name__ == "__main__":
    main()