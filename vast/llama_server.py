"""
FastAPI server for page validation using Llama 3.2 3B
Runs on Vast.ai RTX 3090 instances for Stage 1
"""

import os
import json
from typing import Dict
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

app = FastAPI(title="Page Validation Server")

model = None
tokenizer = None

def load_model():
    """
    Load Llama 3.2 3B model
    """
    global model, tokenizer
    
    print("Loading Llama 3.2 3B model...")
    
    model_name = "meta-llama/Llama-3.2-3B-Instruct"
    
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype=torch.float16,
        device_map="auto"
    )
    
    model.eval()
    print("Model loaded successfully")

@app.on_event("startup")
async def startup_event():
    load_model()

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "mode": "page_validation"
    }

@app.post("/validate")
async def validate_pages(request: dict):
    """
    Validate pages for operational KPIs
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        page_texts = request.get('page_texts', {})
        prompt = request.get('prompt', '')
        
        # Build full prompt
        full_prompt = f"{prompt}\n\nRespond with valid JSON only."
        
        # Tokenize
        inputs = tokenizer(full_prompt, return_tensors="pt").to(model.device)
        
        # Generate
        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=2048,
                do_sample=False,
                pad_token_id=tokenizer.eos_token_id
            )
        
        # Decode
        response_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        # Extract JSON
        if '```json' in response_text:
            json_text = response_text.split('```json')[1].split('```')[0].strip()
        elif '```' in response_text:
            json_text = response_text.split('```')[1].split('```')[0].strip()
        else:
            # Try to find JSON in response
            start = response_text.find('{')
            end = response_text.rfind('}') + 1
            if start != -1 and end > start:
                json_text = response_text[start:end]
            else:
                json_text = response_text
        
        result = json.loads(json_text)
        
        return JSONResponse(content=result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)