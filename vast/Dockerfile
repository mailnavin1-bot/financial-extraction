FROM pytorch/pytorch:2.1.2-cuda12.1-cudnn8-devel

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Install system tools
RUN apt-get update && apt-get install -y \
    git \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /workspace

# Install Python dependencies
COPY requirements_vast.txt .
RUN pip install --no-cache-dir -r requirements_vast.txt

# Copy server scripts
COPY entrypoint.sh .
COPY extraction_server.py .
COPY verification_server.py .
COPY llama_server.py .

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Expose the API port
EXPOSE 8000

# Start the server
ENTRYPOINT ["./entrypoint.sh"]