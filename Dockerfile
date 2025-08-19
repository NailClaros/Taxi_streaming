FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY producer/requirements3.10.txt .
RUN pip install --no-cache-dir -r requirements3.10.txt

# Copy producer code
COPY producer/ /app/producer/

# Default command
CMD ["python", "producer/producer.py"]
