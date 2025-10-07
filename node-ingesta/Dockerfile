FROM python:3.13-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY node_ingesta.py .

CMD ["python", "node_ingesta.py"]