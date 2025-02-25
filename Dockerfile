FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    rclone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY inputs/level_1a.json ./inputs/level_1a.json
COPY inputs/level_2.json ./inputs/level_2.json
COPY inputs/m49.csv ./inputs/m49.csv
COPY outputs/.gitignore ./outputs/.gitignore

CMD ["python", "-m", "app"]
