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

# Copy the dependency file and install Python dependencies
COPY source/requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install pymysql && \
    pip install "dlt[sqlalchemy]==1.8.1" && \
    pip install mysqlclient && \
    pip install python-dotenv
    
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
