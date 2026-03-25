FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends chrony \
    && rm -rf /var/lib/apt/lists/*

COPY collector.py /usr/local/bin/collector.py

ENTRYPOINT ["python3", "/usr/local/bin/collector.py"]