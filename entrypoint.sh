#!/bin/sh

# Ensure the env variable is set, otherwise use a default value
CRON_SCHEDULE=${CRON_SCHEDULE:-*/5 * * * *}

# Write the cron job to a file
echo "$CRON_SCHEDULE /usr/local/bin/python /app/db_pipeline.py >> /var/log/cron.log 2>&1" > /etc/cron.d/custom-cron

# Apply correct permissions
chmod 0644 /etc/cron.d/custom-cron

# Apply the crontab and start cron
crontab /etc/cron.d/custom-cron
cron -f
