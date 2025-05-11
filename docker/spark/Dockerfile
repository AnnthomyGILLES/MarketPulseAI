FROM bitnami/spark:3.5.1

USER root

# Set environment variables
ENV HOME=/root \
    HF_HOME=/opt/bitnami/spark/models/cache \
    PYTHONIOENCODING=utf8 \
    PYTHONUNBUFFERED=1

# Install dependencies
WORKDIR /opt/bitnami/spark

# Copy requirements.txt file
COPY docker/spark/requirements.txt /opt/bitnami/spark/

# Create necessary directories
RUN mkdir -p /root/.local && chmod -R 777 /root/.local && \
    mkdir -p /opt/bitnami/spark/models/cache && \
    mkdir -p /opt/bitnami/spark/logs && \
    chmod -R 777 /opt/bitnami/spark/models

# Install uv and use it to install Python dependencies
RUN curl -sSf https://astral.sh/uv/install.sh | sh && \
    uv pip install -r requirements.txt

# Pre-download and cache FinBERT model
RUN python -c "from transformers import AutoModelForSequenceClassification, AutoTokenizer; AutoModelForSequenceClassification.from_pretrained('ProsusAI/finbert'); AutoTokenizer.from_pretrained('ProsusAI/finbert')"

# Create a directory for scripts
RUN mkdir -p /opt/bitnami/spark/scripts

# Copy entrypoint script
COPY docker/spark/entrypoint.sh /opt/bitnami/spark/scripts/
RUN chmod +x /opt/bitnami/spark/scripts/entrypoint.sh

# Keep as root user for now due to permissions requirements
USER root

# Set the default command to the entrypoint script
CMD ["/opt/bitnami/spark/scripts/entrypoint.sh"] 