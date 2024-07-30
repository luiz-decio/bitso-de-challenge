FROM quay.io/astronomer/astro-runtime:11.7.0

# Copy the src and data folders from Challenge_1 to the Docker container
COPY ../src /usr/local/airflow/src
COPY ../data /usr/local/airflow/data

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt