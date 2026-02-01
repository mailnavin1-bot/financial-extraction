#!/bin/bash

# Entrypoint script for Vast.ai container
# Determines which server to run based on environment variable

echo "========================================="
echo "Financial Extraction - Vast.ai Container"
echo "========================================="

# Check which mode to run
MODE=${EXTRACTION_MODE:-extraction}

echo "Mode: $MODE"
echo "GPU Info:"
nvidia-smi --query-gpu=name,memory.total --format=csv

# Determine which model to download
if [ "$MODE" == "page_selection" ]; then
    MODEL_NAME="meta-llama/Llama-3.2-3B-Instruct"
    SERVER_SCRIPT="llama_server.py"
elif [ "$MODE" == "extraction" ] || [ "$MODE" == "verification" ]; then
    MODEL_NAME="Qwen/Qwen2-VL-72B-Instruct"
    if [ "$MODE" == "extraction" ]; then
        SERVER_SCRIPT="extraction_server.py"
    else
        SERVER_SCRIPT="verification_server.py"
    fi
else
    echo "Unknown mode: $MODE"
    exit 1
fi

# Download model if not cached
echo "Checking for model: $MODEL_NAME"
if [ ! -d "/workspace/.cache/huggingface/hub/models--${MODEL_NAME//\//_}" ]; then
    echo "Downloading $MODEL_NAME (this may take 5-15 minutes)..."
    python3 -c "from transformers import AutoModelForCausalLM, AutoTokenizer; \
                model = AutoModelForCausalLM.from_pretrained('$MODEL_NAME', device_map='auto'); \
                tokenizer = AutoTokenizer.from_pretrained('$MODEL_NAME')"
    echo "Model downloaded successfully"
else
    echo "Model already cached"
fi

# Start appropriate server
echo "Starting $MODE server on port 8000..."
python3 /workspace/$SERVER_SCRIPT