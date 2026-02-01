"""
Test all API connections and credentials
Run this before processing any documents
"""

import os
import sys
import json
import requests

# Add scripts folder to path so we can import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import load_settings

def print_header(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)

def print_success(text):
    print(f"✓ {text}")

def print_error(text):
    print(f"✗ {text}")

def print_info(text):
    print(f"  {text}")

def test_gemini_api():
    """Test Gemini API connection"""
    print_header("TESTING GEMINI API")
    try:
        settings = load_settings()
        api_key = settings.get('gemini_api_key')
        if not api_key:
            print_error("Gemini API key not configured")
            return False
        
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')
        response = model.generate_content("Ping")
        
        if response:
            print_success("Gemini API connection successful")
            return True
    except Exception as e:
        print_error(f"Gemini API test failed: {e}")
        return False

def test_vast_api():
    """Test Vast.ai API connection and check balance"""
    print_header("TESTING VAST.AI API")
    try:
        settings = load_settings()
        api_key = settings.get('vast_api_key')
        if not api_key:
            print_error("Vast.ai API key not configured")
            return False
            
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get("https://console.vast.ai/api/v0/users/current/", headers=headers, timeout=10)
        
        if response.status_code == 200:
            user_data = response.json()
            balance = user_data.get('credit', 0)
            print_success("Vast.ai API connection successful")
            print_info(f"Account balance: ${balance:.2f}")
            return True
        else:
            print_error(f"Vast.ai API error: {response.status_code}")
            return False
    except Exception as e:
        print_error(f"Vast.ai API test failed: {e}")
        return False

def test_docker_image():
    """Test if Docker image is configured"""
    print_header("TESTING DOCKER IMAGE")
    settings = load_settings()
    docker_image = settings.get('docker_image')
    
    print_info(f"Image: {docker_image}")
    
    if "pkg.dev" in docker_image:
        print_success("Google Artifact Registry URL detected")
        print_info("⚠️  Ensure this repository is PUBLIC or 'docker_auth' is set in settings.json")
        return True
    
    if "/" not in docker_image:
        print_error("Invalid image format")
        return False
    return True

def search_gpu(gpu_list, num_gpus, min_ram, label):
    """Helper to search for any GPU in a list using relaxed criteria"""
    settings = load_settings()
    api_key = settings.get('vast_api_key')
    headers = {"Authorization": f"Bearer {api_key}"}

    print_info(f"Searching for {label} candidates...")
    print_info(f"Types: {', '.join(gpu_list)}")
    
    # We loop through candidates until we find ONE available type
    found = False
    
    for gpu_name in gpu_list:
        # CLEAN NAME: Vast API needs spaces (RTX 3090), not underscores (RTX_3090)
        clean_name = gpu_name.replace('_', ' ')
        
        # EXACT SAME RELAXED CRITERIA AS VAST MANAGER
        params = {"q": json.dumps({
            "rentable": {"eq": True},
            "gpu_name": {"eq": clean_name},
            "num_gpus": {"eq": num_gpus},
            "gpu_ram": {"gte": min_ram},
            "disk_space": {"gte": 15},       # Match relaxed manager (15GB)
            "inet_down": {"gte": 20},        # Match relaxed manager (20Mbps)
            "reliability2": {"gte": 0.75}    # Match relaxed manager (75%)
        })}
        
        try:
            response = requests.get(
                "https://console.vast.ai/api/v0/bundles/",
                headers=headers,
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                offers = data.get('offers', []) if isinstance(data, dict) else data
                
                # Filter valid offers
                valid_offers = [o for o in offers if isinstance(o, dict)]
                
                if valid_offers:
                    cheapest = min(valid_offers, key=lambda x: x.get('dph_total', 999))
                    price = cheapest.get('dph_total')
                    print_success(f"✓ Found {clean_name} (from ${price:.3f}/hr)")
                    found = True
                    break # Stop looking if we found a good candidate
        except Exception:
            pass
            
    if not found:
        print_error(f"No suitable GPUs found for {label}")
        return False
    return True

def test_vast_gpu_availability():
    """Test if required GPUs are available on Vast.ai using Relaxed Search"""
    print_header("TESTING GPU AVAILABILITY (RELAXED SEARCH)")
    
    # Stage 1 Candidates (Expanded List)
    stage1_gpus = ['RTX_3090', 'RTX_4090', 'RTX_3080_Ti', 'RTX_3080', 'RTX_4080', 'RTX_A5000', 'RTX_A6000', 'RTX_5000_Ada']
    s1 = search_gpu(stage1_gpus, 1, 10, "Stage 1 (Page Selection)")
    
    # Stage 3 Candidates (Expanded List)
    stage3_gpus = ['A100_80GB', 'A100_SXM4_80GB', 'H100', 'RTX_A6000', 'RTX_6000_Ada']
    s3 = search_gpu(stage3_gpus, 2, 40, "Stage 3 (Extraction)")
    
    return s1 and s3

if __name__ == "__main__":
    t1 = test_gemini_api()
    t2 = test_vast_api()
    t3 = test_docker_image()
    t4 = test_vast_gpu_availability()
    
    if t1 and t2 and t3 and t4:
        print("\n✅ ALL SYSTEMS GO - Ready for Pilot Run")
    else:
        print("\n⚠️  ISSUES DETECTED - Review errors above")