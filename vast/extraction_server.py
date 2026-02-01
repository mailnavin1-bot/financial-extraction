"""
FastAPI server for KPI extraction using Qwen2.5-VL-72B
Runs on Vast.ai GPU instances
"""

import os
import json
import base64
from io import BytesIO
from typing import List, Dict, Any
import torch
from PIL import Image
from transformers import Qwen2VLForConditionalGeneration, AutoTokenizer, AutoProcessor
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

# Initialize FastAPI
app = FastAPI(title="KPI Extraction Server")

# Global model variables
model = None
tokenizer = None
processor = None

def load_model():
    """
    Load Qwen2.5-VL-72B model
    """
    global model, tokenizer, processor
    
    print("Loading Qwen2.5-VL-72B model...")
    
    model_name = "Qwen/Qwen2-VL-72B-Instruct"
    
    # Load tokenizer and processor
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    processor = AutoProcessor.from_pretrained(model_name)
    
    # Load model with optimizations
    model = Qwen2VLForConditionalGeneration.from_pretrained(
        model_name,
        torch_dtype=torch.float16,
        device_map="auto",
        load_in_8bit=True  # Use 8-bit quantization to fit in 80GB
    )
    
    model.eval()
    
    print("Model loaded successfully")

@app.on_event("startup")
async def startup_event():
    """
    Load model on startup
    """
    load_model()

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "gpu_available": torch.cuda.is_available(),
        "gpu_count": torch.cuda.device_count()
    }

@app.post("/extract")
async def extract_kpis(
    image: UploadFile = File(...),
    prompt: str = Form(...)
):
    """
    Extract KPIs from a single image
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        # Read image
        image_bytes = await image.read()
        pil_image = Image.open(BytesIO(image_bytes)).convert('RGB')
        
        # Prepare input
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image", "image": pil_image},
                    {"type": "text", "text": prompt}
                ]
            }
        ]
        
        # Process
        text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        inputs = processor(
            text=[text],
            images=[pil_image],
            return_tensors="pt"
        ).to(model.device)
        
        # Generate
        with torch.no_grad():
            output_ids = model.generate(
                **inputs,
                max_new_tokens=2048,
                do_sample=False
            )
        
        # Decode
        generated_text = processor.batch_decode(
            output_ids,
            skip_special_tokens=True,
            clean_up_tokenization_spaces=False
        )[0]
        
        # Extract JSON from response
        if '```json' in generated_text:
            json_text = generated_text.split('```json')[1].split('```')[0].strip()
        elif '```' in generated_text:
            json_text = generated_text.split('```')[1].split('```')[0].strip()
        else:
            json_text = generated_text
        
        # Parse JSON
        result = json.loads(json_text)
        
        return JSONResponse(content=result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Extraction error: {str(e)}")

@app.post("/extract_batch")
async def extract_batch(
    images: List[UploadFile] = File(...),
    prompts: str = Form(...)  # JSON string of prompts
):
    """
    Extract KPIs from multiple images in batch
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        prompts_list = json.loads(prompts)
        
        if len(images) != len(prompts_list):
            raise HTTPException(status_code=400, detail="Number of images must match prompts")
        
        results = []
        
        # Process images in batches of 20
        batch_size = 20
        for i in range(0, len(images), batch_size):
            batch_images = images[i:i+batch_size]
            batch_prompts = prompts_list[i:i+batch_size]
            
            # Process batch
            for img, prompt in zip(batch_images, batch_prompts):
                image_bytes = await img.read()
                pil_image = Image.open(BytesIO(image_bytes)).convert('RGB')
                
                messages = [
                    {
                        "role": "user",
                        "content": [
                            {"type": "image", "image": pil_image},
                            {"type": "text", "text": prompt}
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
                results.append(result)
        
        return JSONResponse(content={"results": results})
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch extraction error: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)