"""
Vast.ai Instance Manager
Handles launching, monitoring, and shutting down GPU instances via direct REST API
Implements 'Fast Start' logic using public base images and runtime git cloning
"""

import os
import sys
import json
import time
import requests
import urllib.parse
from typing import Optional, Dict, Any, List

# Add parent directory to path to allow imports from root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import load_settings

import logging
logger = logging.getLogger(__name__)

class VastManager:
    API_BASE = "https://console.vast.ai/api/v0"

    def __init__(self, use_spot: bool = False):
        settings = load_settings()
        self.api_key = settings.get('vast_api_key')
        if not self.api_key:
            raise ValueError("Vast.ai API key not found in config/settings.json")
        
        self.use_spot = use_spot
        # Public Base Image (Cached on 90% of hosts)
        self.base_image = "pytorch/pytorch:2.1.2-cuda12.1-cudnn8-runtime"
        
        self._verify_auth()

    def _verify_auth(self):
        """Verify we can connect to Vast API"""
        try:
            url = f"{self.API_BASE}/users/current/"
            headers = {"Authorization": f"Bearer {self.api_key}"}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                logger.info("Vast API authentication successful")
            else:
                logger.warning(f"Vast API authentication check failed: {response.status_code}")
        except Exception as e:
            logger.warning(f"Could not verify Vast API connection: {e}")

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def _get_onstart_script(self, mode: str) -> str:
        """
        Generates the bash script to run on instance boot.
        1. Clones the code from GitHub
        2. Injects the keys from YOUR LOCAL computer
        3. Installs SERVER-SPECIFIC requirements
        """
        settings = load_settings()
        repo_url = settings.get('git_repo_url')
        git_token = settings.get('git_token', '')
        
        if not repo_url or "YOUR_USERNAME" in repo_url:
            raise ValueError(
                "\n\n!!! CONFIGURATION ERROR !!!\n"
                "You must set 'git_repo_url' in config/settings.json.\n"
            )

        if git_token and "https://" in repo_url:
            clean_url = repo_url.replace("https://", "")
            auth_url = f"https://{git_token}@{clean_url}"
        else:
            auth_url = repo_url
            
        json_content = json.dumps(settings).replace("'", "'\\''")

        # UPDATED SCRIPT LOGIC
        script = f"""#!/bin/bash
set -e
echo "--- FAST START INIT ---"

# 1. Install System Deps
apt-get update -y
apt-get install -y git libgl1-mesa-glx libglib2.0-0

# 2. Clone Repository
echo "Cloning code..."
rm -rf /workspace/app
if git clone {auth_url} /workspace/app; then
    echo "Code cloned successfully"
else
    echo "Git clone failed. Check repo URL."
    exit 1
fi

# 3. Inject Config
echo "Injecting secure configuration..."
mkdir -p /workspace/app/config
echo '{json_content}' > /workspace/app/config/settings.json

# 4. Install Python Deps (SMART CHECK)
echo "Installing dependencies..."
cd /workspace/app

# CHECK FOR SERVER-SPECIFIC REQUIREMENTS FIRST
if [ -f "requirements-server.txt" ]; then
    echo "Found requirements-server.txt - Installing Server Deps Only..."
    pip install --no-cache-dir -r requirements-server.txt
elif [ -f "requirements.txt" ]; then
    echo "Using standard requirements.txt..."
    pip install --no-cache-dir -r requirements.txt
else
    echo "No requirements found, installing defaults..."
    pip install --no-cache-dir fastapi uvicorn python-multipart transformers accelerate qwen_vl_utils tiktoken einops scipy matplotlib
fi

# 5. Start Application
echo "Starting application in mode: {mode}..."
export EXTRACTION_MODE={mode}
export PYTHONPATH=$PYTHONPATH:/workspace/app

if [ -f "entrypoint.sh" ]; then
    bash entrypoint.sh
else
    echo "No entrypoint.sh found. Sleeping to keep container alive."
    sleep infinity
fi
"""
        return script

    def search_instances(self, gpu_type: str, gpu_count: int = 1, min_gpu_ram: int = 20):
        clean_gpu_name = gpu_type.replace('_', ' ')
        logger.info(f"Searching for {gpu_count}x {clean_gpu_name}...")
        
        query = {
            "verified": {"eq": True},
            "rentable": {"eq": True},
            "gpu_name": {"eq": clean_gpu_name},
            "num_gpus": {"eq": gpu_count},
            "gpu_ram": {"gte": min_gpu_ram * 1024}, 
            "disk_space": {"gte": 20},
            "inet_down": {"gte": 200},
            "reliability2": {"gte": 0.85}
        }
        
        query_json = json.dumps(query)
        encoded_query = urllib.parse.quote(query_json)
        url = f"{self.API_BASE}/bundles?q={encoded_query}"
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=30)
            response.raise_for_status()
            offers = response.json().get('offers', [])
            
            filtered = []
            for o in offers:
                if o.get('gpu_name') != clean_gpu_name: continue
                if o.get('num_gpus') != gpu_count: continue
                if (o.get('gpu_ram', 0) / 1024) < min_gpu_ram: continue
                filtered.append(o)

            filtered.sort(key=lambda x: x['dph_total'])
            logger.info(f"Found {len(filtered)} matching offers for {clean_gpu_name}")
            return filtered
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
    
    def launch_instance(self, offer_id: int, mode: str, max_price: Optional[float] = None) -> Dict[str, Any]:
        logger.info(f"Launching instance for {mode} (Fast Start)...")
        
        url = f"{self.API_BASE}/asks/{offer_id}/"
        
        try:
            onstart_cmd = self._get_onstart_script(mode)
        except ValueError as e:
            logger.error(str(e))
            raise
        
        payload = {
            "client_id": "me",
            "image": self.base_image,
            "env": {}, 
            "disk": 40.0,
            "onstart": onstart_cmd,
            "runtype": "ssh",
            "use_jupyter_lab": False
        }

        try:
            response = requests.put(url, headers=self._get_headers(), json=payload, timeout=30)
            
            if response.status_code >= 400:
                raise Exception(f"API Error {response.status_code}: {response.text}")
                
            result = response.json()
            if not result.get('success'):
                raise Exception(f"Vast failure: {result}")
            
            instance_id = result.get('new_contract') or result.get('id')
            logger.info(f"Instance {instance_id} launched. Bootstrapping environment...")
            return self.wait_for_instance(instance_id)
            
        except Exception as e:
            raise Exception(f"Instance launch failed: {e}")

    def wait_for_instance(self, instance_id: int, timeout: int = 1200) -> Dict[str, Any]:
        logger.info(f"Waiting for instance {instance_id} to bootstrap...")
        start_time = time.time()
        url = f"{self.API_BASE}/instances"
        
        while time.time() - start_time < timeout:
            try:
                response = requests.get(url, headers=self._get_headers(), timeout=30)
                if response.status_code == 200:
                    instances = response.json().get('instances', [])
                    target = next((i for i in instances if i['id'] == instance_id), None)
                    
                    if target and target.get('actual_status') == 'running':
                        ssh_host = target.get('public_ipaddr')
                        ports = target.get('ports', {})
                        
                        api_port = None
                        if isinstance(ports, dict):
                            mapping = ports.get('8000/tcp')
                            if mapping and isinstance(mapping, list):
                                api_port = mapping[0].get('HostPort')
                        
                        if api_port:
                            api_url = f"http://{ssh_host}:{api_port}"
                            if self._wait_for_health_check(api_url):
                                return {
                                    'instance_id': instance_id,
                                    'api_url': api_url,
                                    'price_per_hour': target.get('dph_total'),
                                    'gpu_name': target.get('gpu_name'),
                                    'gpu_count': target.get('num_gpus')
                                }
            except Exception:
                pass
            time.sleep(15)
            
        raise TimeoutError(f"Instance {instance_id} failed to start within {timeout}s")
    
    def _wait_for_health_check(self, api_url: str, timeout: int = 600) -> bool:
        logger.info("  ...waiting for application server to start...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                if requests.get(f"{api_url}/health", timeout=5).status_code == 200:
                    return True
            except:
                pass
            time.sleep(10)
        return False
    
    def destroy_instance(self, instance_id: int):
        url = f"{self.API_BASE}/instances/{instance_id}/"
        try:
            requests.delete(url, headers=self._get_headers(), timeout=30)
            logger.info(f"Instance {instance_id} destroyed")
        except Exception:
            pass

    def launch_for_stage1(self, max_price: float = 0.60) -> Dict[str, Any]:
        candidates = ['RTX_3090', 'RTX_4090', 'RTX_3080_Ti', 'RTX_3080', 'RTX_4080', 'RTX_A5000', 'RTX_A6000']
        for gpu in candidates:
            offers = self.search_instances(gpu_type=gpu, gpu_count=1, min_gpu_ram=10)
            affordable = [o for o in offers if o['dph_total'] <= max_price]
            if affordable:
                return self.launch_instance(affordable[0]['id'], 'page_selection', max_price)
        raise Exception("No GPUs found for Stage 1.")
    
    def launch_for_stage3(self, max_price: float = 5.00) -> Dict[str, Any]:
        candidates = ['A100_80GB', 'A100_SXM4_80GB', 'H100', 'RTX_A6000', 'RTX_6000_Ada']
        for gpu in candidates:
            offers = self.search_instances(gpu_type=gpu, gpu_count=2, min_gpu_ram=40)
            affordable = [o for o in offers if o['dph_total'] <= max_price]
            if affordable:
                return self.launch_instance(affordable[0]['id'], 'extraction', max_price)
        raise Exception("No GPUs found for Stage 3.")

    def launch_for_stage5(self, max_price: float = 5.00) -> Dict[str, Any]:
        return self.launch_for_stage3(max_price)
