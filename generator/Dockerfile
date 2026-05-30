FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir boto3

COPY generate_json.py .

CMD ["python", "generate_json.py"]
