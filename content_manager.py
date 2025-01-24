import os
import requests
from urllib.parse import urljoin
from config import SERVER_CONFIG

def ensure_download_directory():
    print("Checking download directory...")
    if not os.path.exists(SERVER_CONFIG['DOWNLOAD_DIR']):
        os.makedirs(SERVER_CONFIG['DOWNLOAD_DIR'])
        print(f"Created download directory: {SERVER_CONFIG['DOWNLOAD_DIR']}")
        os.chmod(SERVER_CONFIG['DOWNLOAD_DIR'], 0o755)
    else:
        print("Download directory already exists")

def download_content(file_path):
    print(f"Attempting to download: {file_path}")
    try:
        if not file_path:
            print("No file path provided")
            return None
            
        full_url = urljoin(SERVER_CONFIG['BASE_URL'], file_path)
        filename = os.path.basename(file_path)
        local_path = os.path.join(SERVER_CONFIG['DOWNLOAD_DIR'], filename)
        local_url = f"http://{SERVER_CONFIG['LOCAL_SERVER_IP']}:{SERVER_CONFIG['LOCAL_SERVER_PORT']}/{filename}"
        
        if os.path.exists(local_path):
            print(f"File already exists: {local_path}")
            return local_url
            
        print(f"Downloading from: {full_url}")
        response = requests.get(full_url, stream=True)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        os.chmod(local_path, 0o644)
        
        print(f"File successfully downloaded: {local_path}")
        print(f"File available at: {local_url}")
        return local_url
        
    except Exception as e:
        print(f"Error downloading file {file_path}: {e}")
        return None