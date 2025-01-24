from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
import threading
from config import SERVER_CONFIG

class CustomHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        current_dir = os.path.abspath(SERVER_CONFIG['DOWNLOAD_DIR'])
        super().__init__(*args, directory=current_dir, **kwargs)
    
    def do_GET(self):
        print(f"Received GET request for: {self.path}")
        try:
            super().do_GET()
        except Exception as e:
            print(f"Error serving file: {e}")
            self.send_error(500, str(e))

def start_http_server():
    try:
        if not os.path.exists(SERVER_CONFIG['DOWNLOAD_DIR']):
            os.makedirs(SERVER_CONFIG['DOWNLOAD_DIR'])
        
        server_address = ('0.0.0.0', SERVER_CONFIG['LOCAL_SERVER_PORT'])
        httpd = HTTPServer(server_address, CustomHandler)
        
        print(f"Serving files from {SERVER_CONFIG['DOWNLOAD_DIR']}")
        print(f"Server URL: http://{SERVER_CONFIG['LOCAL_SERVER_IP']}:{SERVER_CONFIG['LOCAL_SERVER_PORT']}/")
        
        nested_dir = os.path.join(SERVER_CONFIG['DOWNLOAD_DIR'], "downloaded_content")
        if os.path.exists(nested_dir):
            import shutil
            shutil.rmtree(nested_dir)
        
        server_thread = threading.Thread(target=httpd.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        print("HTTP Server started successfully")
        
    except Exception as e:
        print(f"Error starting HTTP server: {e}")
        import traceback
        print("Stack trace:", traceback.format_exc())