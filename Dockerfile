# Use Python 3.10 as base image
FROM python:3.10

# Set working directory
WORKDIR /museums_etl

# Copy requirements.txt to working directory
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy all files to working directory
COPY . .

ENTRYPOINT ["/bin/bash"]