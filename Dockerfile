# Use a glibc-based Python image (Debian slim) instead of Alpine
FROM python:3.11-slim

# Set environment variable for timezone
ENV TZ=Asia/Jakarta

RUN apt-get update && apt-get install -y \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    dlt[sqlalchemy]>=1.15.0 \
    sqlalchemy>=1.4.0 \
    mysqlclient>=2.1.0 \
    pandas>=1.5.0 \
    pyarrow>=10.0.0 \
    watchdog>=3.0.0

# Create staging directory for Parquet files
RUN mkdir -p /app/staging && \
    chmod 755 /app/staging

# Create log directory
RUN mkdir -p /var/log/app && \
    chmod 755 /var/log/app

COPY source/ .

# Use a command that keeps the container running for manual execution
# CMD ["tail", "-f", "/dev/null"]

# Expose the port the app runs on
EXPOSE 8089

# Run the script and capture logs
CMD sh -c "python db_pipeline.py 2>&1 | tee /var/log/app/output.log"

# Copy the entrypoint script
# COPY ../entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh

# Set entrypoint
# ENTRYPOINT ["/entrypoint.sh"]
