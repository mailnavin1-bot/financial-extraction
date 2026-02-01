"""
FastAPI server for self-verification using Qwen2.5-VL-72B
Similar to extraction but optimized for verification tasks
"""

import os
import json
from io import BytesIO
from typing import List, Dict
import torch
from PIL import Image
from transformers import Qwen2VLForConditionalGeneration, AutoTokenizer, AutoProcessor
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

app = FastAPI(title="KPI Verification Server")

model = None
tokenizer = None
processor = None

def load_model():
    global model, tokenizer, processor
    
    print("Loading Qwen2.5-VL-72B model for verification...")
    
    model_name = "Qwen/Qwen2-VL-72B-Instruct"
    
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    processor = AutoProcessor.from_pretrained(model_name)
    
    model = Qwen2VLForConditionalGeneration.from_pretrained(
        model_name,
        torch_dtype=torch.float16,
        device_map="auto",
        load_in_8bit=True
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
        "mode": "verification"
    }

@app.post("/verify")
async def verify_extraction(
    image: UploadFile = File(...),
    prompt: str = Form(...),
    extractions: str = Form(...)  # JSON string of previous extractions
):
    """
    Verify previous extractions
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        # Read image
        image_bytes = await image.read()
        pil_image = Image.open(BytesIO(image_bytes)).convert('RGB')
        
        # Build verification prompt
        extractions_data = json.loads(extractions)
        
        verification_prompt = f"{prompt}\n\nPREVIOUS EXTRACTIONS:\n{json.dumps(extractions_data, indent=2)}"
        
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image", "image": pil_image},
                    {"type": "text", "text": verification_prompt}
                ]
            }
        ]
        
        text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        inputs = processor(
            text=[text],
            images=[pil_image],
            return_tensors="pt"
        ).to(model.device)
        
        with torch.no_grad():
            output_ids = model.generate(
                **inputs,
                max_new_tokens=2048,
                do_sample=False
            )
        
        generated_text = processor.batch_decode(
            output_ids,
            skip_special_tokens=True,
            clean_up_tokenization_spaces=False
        )[0]
        
        # Extract JSON
        if '```json' in generated_text:
            json_text = generated_text.split('```json')[1].split('```')[0].strip()
        elif '```' in generated_text:
            json_text = generated_text.split('```')[1].split('```')[0].strip()
        else:
            json_text = generated_text
        
        result = json.loads(json_text)
        
        return JSONResponse(content=result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Verification error: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)