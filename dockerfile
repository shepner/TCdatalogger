FROM python:3.13-slim-bullseye

# Install Python dependencies
RUN apt-get update && apt-get install -y python3-pip
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt


# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Set the working directory
WORKDIR /app

# Copy application into the image
WORKDIR /app/config
COPY config ./
WORKDIR /app/TCdatalogger
COPY TCdatalogger ./

# Install the dependencies
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Copy the crontab file
COPY config/crontab /etc/cron.d/my-cron-job

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/my-cron-job

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Apply cron job
RUN crontab /etc/cron.d/my-cron-job

# Start the cron service and the application
CMD cron && tail -f /var/log/cron.log

CMD ["python", "app.py"]