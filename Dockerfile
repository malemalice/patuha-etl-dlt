# Use a glibc-based Python image (Debian slim) instead of Alpine
FROM python:3.11-slim

# Set environment variable for timezone
ENV TZ=Asia/Jakarta

# Install only the absolutely necessary packages
# Note: Using pymysql (pure Python) instead of mysqlclient, so no MySQL dev headers needed
RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set the working directory
WORKDIR /app

# Copy the dependency file and install Python dependencies
COPY source/requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Create staging directory for Parquet files
RUN mkdir -p /app/staging && \
    chmod 755 /app/staging

# Create log directory
RUN mkdir -p /var/log/app && \
    chmod 755 /var/log/app

COPY source/ .

# Clean up Python cache files to ensure fresh code execution
RUN find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true && \
    find . -name "*.pyc" -delete 2>/dev/null || true && \
    find . -name "*.pyo" -delete 2>/dev/null || true

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
