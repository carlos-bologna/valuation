# Use an official Python runtime as the base image
FROM python:3.9-slim

# Create the /workspaces/valuation/ directory
RUN mkdir -p /workspaces/valuation/

# Set the working directory in the container
WORKDIR /workspaces/valuation/

# Create a folder called common
RUN mkdir common

# Copy the requirements file into the container
COPY api/predict/requirements.txt .

# Copy the script into the container
COPY src/stock_price/*.py .

# Copy all files inside the common folder to the image
COPY src/common/ ./common/

# Copy the config file into the container
COPY config.yaml .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt


# Set the environment variable for Python to run in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Expose port 5000 for the Flask application
EXPOSE 5000

# Run the script when the container launches
CMD ["python", "predict.py"]
