FROM python:3.13-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spring_ingesta.py .

CMD ["python", "spring_ingesta.py"]