FROM python:3.11-slim

# Create and set the working directory
RUN mkdir /code
WORKDIR /code

# Install system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install Python dependencies
COPY requirements.txt /code/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project into the container
COPY . /code/

# Expose the port the application will run on
EXPOSE 8800

# Use Gunicorn to run the application in development mode
CMD ["python", "manage.py", "runserver", "0.0.0.0:8800"]
