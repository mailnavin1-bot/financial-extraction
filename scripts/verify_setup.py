"""
Setup Verification Script
Verifies all prerequisites are installed and configured correctly
"""

import os
import sys
import subprocess
import importlib
import json
from pathlib import Path

class SetupVerifier:
    """
    Verify system setup
    """
    
    def __init__(self):
        self.checks_passed = 0
        self.checks_failed = 0
        self.warnings = []
    
    def print_header(self):
        print("=" * 70)
        print("FINANCIAL EXTRACTION SYSTEM - SETUP VERIFICATION")
        print("=" * 70)
        print()
    
    def print_check(self, name, status, message=""):
        """
        Print check result
        """
        if status:
            print(f"✓ {name}")
            if message:
                print(f"  {message}")
            self.checks_passed += 1
        else:
            print(f"✗ {name}")
            if message:
                print(f"  ERROR: {message}")
            self.checks_failed += 1
    
    def print_warning(self, message):
        """
        Print warning
        """
        print(f"⚠ WARNING: {message}")
        self.warnings.append(message)
    
    def check_python_version(self):
        """
        Check Python version
        """
        version = sys.version_info
        name = f"Python version {version.major}.{version.minor}.{version.micro}"
        
        if version.major == 3 and version.minor in [10, 11]:
            self.print_check(name, True, "Compatible version detected")
            return True
        else:
            self.print_check(name, False, 
                           f"Python 3.10 or 3.11 required, found {version.major}.{version.minor}")
            return False
    
    def check_package(self, package_name, import_name=None):
        """
        Check if a Python package is installed
        """
        if import_name is None:
            import_name = package_name
        
        try:
            module = importlib.import_module(import_name)
            version = getattr(module, '__version__', 'unknown')
            self.print_check(f"Package: {package_name}", True, f"Version: {version}")
            return True
        except ImportError:
            self.print_check(f"Package: {package_name}", False, 
                           f"Not installed. Run: pip install {package_name}")
            return False
    
    def check_ghostscript(self):
        """
        Check if Ghostscript is installed
        """
        try:
            result = subprocess.run(['gswin64c', 'gs', '--version'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                version = result.stdout.strip()
                self.print_check("Ghostscript", True, f"Version: {version}")
                return True
            else:
                self.print_check("Ghostscript", False, "Not found in PATH")
                return False
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.print_check("Ghostscript", False, 
                           "Not installed or not in PATH. Download from ghostscript.com")
            return False
    
    def check_poppler(self):
        """
        Check if Poppler is installed
        """
        try:
            result = subprocess.run(['pdftoppm', '-v'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0 or 'pdftoppm' in result.stderr:
                self.print_check("Poppler (pdftoppm)", True, "Found in PATH")
                return True
            else:
                self.print_check("Poppler (pdftoppm)", False, "Not found in PATH")
                return False
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.print_check("Poppler (pdftoppm)", False, 
                           "Not installed. Download from github.com/oschwartz10612/poppler-windows")
            return False
    
    def check_docker(self):
        """
        Check if Docker is installed
        """
        try:
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                version = result.stdout.strip()
                self.print_check("Docker", True, version)
                return True
            else:
                self.print_check("Docker", False, "Not found")
                return False
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.print_warning("Docker not installed (optional for pilot, required for production)")
            return False
    
    def check_settings_file(self):
        """
        Check if settings.json exists and is valid
        """
        settings_path = "config/settings.json"
        
        if not os.path.exists(settings_path):
            self.print_check("Settings file", False, 
                           f"{settings_path} not found. Copy from template and configure.")
            return False
        
        try:
            with open(settings_path, 'r') as f:
                settings = json.load(f)
            
            # Check required keys
            required_keys = ['gemini_api_key']
            missing_keys = [k for k in required_keys if k not in settings]
            
            if missing_keys:
                self.print_check("Settings file", False, 
                               f"Missing keys: {missing_keys}")
                return False
            
            # Check if keys are placeholder values
            if 'YOUR_' in settings.get('gemini_api_key', ''):
                self.print_warning("Gemini API key appears to be a placeholder. Update config/settings.json")
            
            self.print_check("Settings file", True, "Valid JSON with required keys")
            return True
            
        except json.JSONDecodeError as e:
            self.print_check("Settings file", False, f"Invalid JSON: {e}")
            return False
    
    def check_gemini_api_key(self):
        """
        Check if Gemini API key is valid
        """
        try:
            with open("config/settings.json", 'r') as f:
                settings = json.load(f)
            
            api_key = settings.get('gemini_api_key', '')
            
            if not api_key or 'YOUR_' in api_key:
                self.print_check("Gemini API key", False, "Not configured in settings.json")
                return False
            
            # Try to import and configure (don't actually call API)
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            
            self.print_check("Gemini API key", True, "Configured (not validated - will check on first use)")
            return True
            
        except ImportError:
            self.print_check("Gemini API key", False, 
                           "google-generativeai package not installed")
            return False
        except Exception as e:
            self.print_check("Gemini API key", False, str(e))
            return False
    
    def check_directory_structure(self):
        """
        Check if required directories exist
        """
        required_dirs = [
            'input/annual_reports',
            'output',
            'config',
            'scripts',
            'logs'
        ]
        
        missing_dirs = []
        for dir_path in required_dirs:
            if not os.path.exists(dir_path):
                missing_dirs.append(dir_path)
        
        if missing_dirs:
            self.print_check("Directory structure", False, 
                           f"Missing directories: {missing_dirs}")
            # Try to create them
            try:
                for dir_path in missing_dirs:
                    os.makedirs(dir_path, exist_ok=True)
                print(f"  ✓ Created missing directories")
            except Exception as e:
                print(f"  ✗ Could not create directories: {e}")
            return False
        else:
            self.print_check("Directory structure", True, "All required directories exist")
            return True
    
    def check_prompt_file(self):
        """
        Check if universal prompt file exists
        """
        prompt_path = "config/universal_prompt.txt"
        
        if not os.path.exists(prompt_path):
            self.print_check("Universal prompt", False, 
                           f"{prompt_path} not found")
            return False
        
        # Check file size (should be substantial)
        size = os.path.getsize(prompt_path)
        if size < 1000:  # Less than 1KB is suspicious
            self.print_check("Universal prompt", False, 
                           "File exists but seems too small")
            return False
        
        self.print_check("Universal prompt", True, 
                        f"Found ({size} bytes)")
        return True
    
    def check_sample_pdfs(self):
        """
        Check if any sample PDFs are present
        """
        pdf_dir = "input/annual_reports"
        
        if not os.path.exists(pdf_dir):
            self.print_warning(f"PDF directory not found: {pdf_dir}")
            return False
        
        pdf_files = list(Path(pdf_dir).glob("*.pdf"))
        
        if not pdf_files:
            self.print_warning(f"No PDF files found in {pdf_dir}. Add PDFs to start processing.")
            return False
        
        self.print_check("Sample PDFs", True, 
                        f"Found {len(pdf_files)} PDF(s) in {pdf_dir}")
        return True
    
    def run_all_checks(self):
        """
        Run all verification checks
        """
        self.print_header()
        
        print("CHECKING PYTHON ENVIRONMENT")
        print("-" * 70)
        self.check_python_version()
        print()
        
        print("CHECKING PYTHON PACKAGES")
        print("-" * 70)
        packages = [
            ('pdfplumber', 'pdfplumber'),
            ('PyMuPDF', 'fitz'),
            ('pdf2image', 'pdf2image'),
            ('camelot-py', 'camelot'),
            ('Pillow', 'PIL'),
            ('opencv-python', 'cv2'),
            ('google-generativeai', 'google.generativeai'),
            ('pandas', 'pandas'),
            ('numpy', 'numpy'),
            ('requests', 'requests'),
            ('colorlog', 'colorlog'),
        ]
        
        for package_name, import_name in packages:
            self.check_package(package_name, import_name)
        print()
        
        print("CHECKING SYSTEM DEPENDENCIES")
        print("-" * 70)
        self.check_ghostscript()
        self.check_poppler()
        self.check_docker()
        print()
        
        print("CHECKING CONFIGURATION")
        print("-" * 70)
        self.check_directory_structure()
        self.check_settings_file()
        self.check_gemini_api_key()
        self.check_prompt_file()
        self.check_sample_pdfs()
        print()
        
        # Print summary
        print("=" * 70)
        print("VERIFICATION SUMMARY")
        print("=" * 70)
        print(f"✓ Checks passed: {self.checks_passed}")
        print(f"✗ Checks failed: {self.checks_failed}")
        print(f"⚠ Warnings: {len(self.warnings)}")
        print()
        
        if self.checks_failed == 0:
            print("🎉 ALL CHECKS PASSED! System is ready.")
            print()
            print("Next steps:")
            print("1. Place PDF files in input/annual_reports/")
            print("2. Run: python scripts/run_pipeline.ps1 <pdf_filename>")
            return True
        else:
            print("❌ SETUP INCOMPLETE. Please fix the errors above.")
            print()
            print("Common fixes:")
            print("1. Install missing packages: pip install -r requirements.txt")
            print("2. Install Ghostscript: https://ghostscript.com/releases/gsdnld.html")
            print("3. Install Poppler: https://github.com/oschwartz10612/poppler-windows/releases")
            print("4. Configure API keys in config/settings.json")
            return False

def main():
    verifier = SetupVerifier()
    success = verifier.run_all_checks()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
