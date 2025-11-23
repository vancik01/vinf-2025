FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN echo '#!/bin/bash' > /entrypoint.sh && \
    echo 'export ARCH=$(dpkg --print-architecture)' >> /entrypoint.sh && \
    echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${ARCH}' >> /entrypoint.sh && \
    echo 'export PATH="${JAVA_HOME}/bin:${PATH}"' >> /entrypoint.sh && \
    echo 'echo "Java Home: $JAVA_HOME"' >> /entrypoint.sh && \
    echo 'echo "Java Version:"' >> /entrypoint.sh && \
    echo 'java -version' >> /entrypoint.sh && \
    echo 'echo "Starting application..."' >> /entrypoint.sh && \
    echo 'exec "$@"' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p data/parsed data/wiki_extracted logs

ENV PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "pipeline.py"]
