FROM python:3.14-slim-bookworm

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flask_ingesta.py .

CMD ["python", "flask_ingesta.py"]