# Use the official Apache Beam Python SDK base image
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Set environment variables
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="requirements.txt"

# Set working directory
WORKDIR /template

# Copy application files
COPY requirements.txt .
COPY main.py .
COPY setup.py .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy any additional configuration files (optional)
# COPY config/ ./config/

# Set the entrypoint for the Dataflow template
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"] 