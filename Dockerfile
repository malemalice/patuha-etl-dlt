# Use a glibc-based Python image (Debian slim) instead of Alpine
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the dependency file and install Python dependencies
COPY source/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install pymysql && \
    pip install "dlt[sqlalchemy]" && \
    pip install mysqlclient
    

# Copy your Python script into the container
COPY source/dlt_pipeline.py .

# Use a command that keeps the container running for manual execution
CMD ["tail", "-f", "/dev/null"]
