FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    unixodbc \
    unixodbc-dev \
    python3-tk tk \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000 8888

CMD ["bash", "-lc", "python app.py & jupyter notebook --ip=0.0.0.0 --port=8888 \
  --ServerApp.open_browser=False --ServerApp.allow_root=True --ServerApp.token='' --ServerApp.password=''"]
