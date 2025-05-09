# Use latest airflow image
FROM apache/airflow:3.0.0

# Install packages in requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
