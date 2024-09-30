# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script into the container
COPY /src/stock_price/extraction_from_yahoo.py .

# Set the environment variable for Python to run in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Run the script when the container launches
CMD ["python", "extraction_from_yahoo.py"]
