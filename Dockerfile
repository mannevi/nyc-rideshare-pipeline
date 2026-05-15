# Base image — Python 3.11 on Linux
FROM python:3.11-slim

# Install Java for PySpark
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Set working directory
WORKDIR /app

# Copy requirements first (for caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY scripts/ ./scripts/
COPY data/ ./data/
COPY .env .

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-m", "scripts.process_with_spark"]