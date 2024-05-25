FROM python:3.9-slim

WORKDIR opt/app

COPY requirements.txt .
COPY socket_connect.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "socket_connect.py"]
