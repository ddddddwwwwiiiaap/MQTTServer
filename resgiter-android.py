import paho.mqtt.client as mqtt
import mysql.connector
import json
import time
import os
import requests
from datetime import datetime
from urllib.parse import urljoin
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading

print("Starting program...")

# Konfigurasi MQTT
MQTT_HOST = "202.149.67.14"
MQTT_PORT = 1883
MQTT_USER = "usrg"
MQTT_PASSWORD = "M@juJ@ya2000#"

# Konfigurasi MySQL
mysql_config = {
    'host': 'localhost',
    'user': 'pedet',
    'password': 'Pedet@2024!',
    'database': 'dashboardadmin',
    'raise_on_warnings': True
}

# Konfigurasi Download dan Server
BASE_URL = "https://gamma.solusimedia.co.id/"
LOCAL_SERVER_IP = "202.149.67.14"  # IP Server Python
LOCAL_SERVER_PORT = 8000
DOWNLOAD_DIR = "downloaded_content"

print("Configuration loaded...")

def cleanup_old_messages():
    """Menghapus pesan yang lebih dari 24 jam berdasarkan created_at"""
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        # Hapus data yang lebih dari 24 jam
        delete_query = """
            DELETE FROM mqtt_messages 
            WHERE created_at < DATE_SUB(NOW(), INTERVAL 24 HOUR)
        """
        
        cursor.execute(delete_query)
        rows_deleted = cursor.rowcount
        connection.commit()
        
        print(f"Cleaned up {rows_deleted} old messages from mqtt_messages table")
        
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as e:
        print(f"Database error during cleanup: {e}")
    except Exception as e:
        print(f"Error during cleanup: {e}")

def start_cleanup_scheduler():
    """Memulai scheduler untuk membersihkan pesan lama setiap jam"""
    cleanup_timer = threading.Timer(3600.0, start_cleanup_scheduler)  # Run every hour
    cleanup_timer.daemon = True
    cleanup_timer.start()
    
    cleanup_old_messages()  # Run cleanup

def start_http_server():
    """Start HTTP server untuk serving file yang didownload"""
    try:
        if not os.path.exists(DOWNLOAD_DIR):
            os.makedirs(DOWNLOAD_DIR)
        
        server_address = ('0.0.0.0', LOCAL_SERVER_PORT)
        
        class CustomHandler(SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                # Set working directory ke folder yang benar
                current_dir = os.path.abspath(DOWNLOAD_DIR)
                super().__init__(*args, directory=current_dir, **kwargs)
            
            def do_GET(self):
                print(f"Received GET request for: {self.path}")
                try:
                    super().do_GET()
                except Exception as e:
                    print(f"Error serving file: {e}")
                    self.send_error(500, str(e))
        
        httpd = HTTPServer(server_address, CustomHandler)
        print(f"Serving files from {DOWNLOAD_DIR}")
        print(f"Server URL: http://{LOCAL_SERVER_IP}:{LOCAL_SERVER_PORT}/")
        
        # Hapus folder downloaded_content di dalam downloaded_content
        nested_dir = os.path.join(DOWNLOAD_DIR, "downloaded_content")
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

def ensure_download_directory():
    """Memastikan direktori download tersedia"""
    print("Checking download directory...")
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print(f"Created download directory: {DOWNLOAD_DIR}")
        # Set permissions
        os.chmod(DOWNLOAD_DIR, 0o755)
    else:
        print("Download directory already exists")

def download_content(file_path):
    """Download file dan return URL lokal untuk akses"""
    print(f"Attempting to download: {file_path}")
    try:
        if not file_path:
            print("No file path provided")
            return None
            
        full_url = urljoin(BASE_URL, file_path)
        filename = os.path.basename(file_path)
        local_path = os.path.join(DOWNLOAD_DIR, filename)
        local_url = f"http://{LOCAL_SERVER_IP}:{LOCAL_SERVER_PORT}/{filename}"
        
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
        
        # Set file permissions
        os.chmod(local_path, 0o644)
        
        print(f"File successfully downloaded: {local_path}")
        print(f"File available at: {local_url}")
        return local_url
        
    except Exception as e:
        print(f"Error downloading file {file_path}: {e}")
        return None

def get_device_schedules(mac_address, client):
    print(f"\nGetting schedules for device: {mac_address}")
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor(dictionary=True)
        
        device_query = """
            SELECT d.*, u.name as client_name, g.name as group_name 
            FROM devices d
            LEFT JOIN users u ON d.client_id = u.id
            LEFT JOIN `groups` g ON d.group_id = g.id_group
            WHERE d.mac_address = %s
        """
        cursor.execute(device_query, (mac_address,))
        device_data = cursor.fetchone()
        
        if device_data:
            try:
                print(f"Found device data for MAC: {mac_address}")
                
                schedules_query = """
                    SELECT s.*, d.mac_address, 
                           p.id_playlist, p.name as playlist_name, 
                           p.client_id as playlist_client_id,
                           p.created_at as playlist_created_at, 
                           p.updated_at as playlist_updated_at
                    FROM schedules s
                    INNER JOIN devices d ON s.device_id = d.id_device
                    LEFT JOIN playlists p ON s.playlist_id = p.id_playlist
                    WHERE d.mac_address = %s
                """
                cursor.execute(schedules_query, (mac_address,))
                schedules_data = cursor.fetchall()
                
                response_data = {
                    'device_id': device_data['id_device'],
                    'mac_address': device_data['mac_address'],
                    'serial_number': device_data['serial_number'],
                    'location': device_data['location'],
                    'api_url': device_data['api_url'],
                    'status': device_data['status'],
                    'client': device_data['client_name'],
                    'group': device_data['group_name'],
                    'last_seen': device_data['last_seen'].isoformat() if device_data['last_seen'] else None,
                    'first_seen': device_data['first_seen'].isoformat() if device_data['first_seen'] else None,
                    'expired_at': device_data['expired_at'].isoformat() if device_data['expired_at'] else None,
                    'schedules': []
                }
                
                ensure_download_directory()
                
                for schedule in schedules_data:
                    playlist_data = None
                    if schedule['playlist_id'] is not None:
                        contents_query = """
                            SELECT pc.*, 
                                   c.id_content, c.title, c.file_path, c.type,
                                   c.duration, c.status as content_status,
                                   c.client_id as content_client_id,
                                   c.created_at as content_created_at,
                                   c.updated_at as content_updated_at
                            FROM playlist_content pc
                            LEFT JOIN contents c ON pc.content_id = c.id_content
                            WHERE pc.playlist_id = %s
                            ORDER BY pc.order_index
                        """
                        cursor.execute(contents_query, (schedule['playlist_id'],))
                        contents_data = cursor.fetchall()
                        
                        playlist_contents = []
                        for content in contents_data:
                            local_url = download_content(content['file_path'])
                            
                            content_item = {
                                'id_playlist_content': content['id_playlist_content'],
                                'content_id': content['content_id'],
                                'order_index': content['order_index'],
                                'flag': content['flag'],
                                'created_at': content['created_at'].isoformat() if content['created_at'] else None,
                                'updated_at': content['updated_at'].isoformat() if content['updated_at'] else None,
                                'content': {
                                    'id_content': content['id_content'],
                                    'title': content['title'],
                                    'file_path': content['file_path'],
                                    'local_url': local_url,  # URL lokal untuk akses file
                                    'type': content['type'],
                                    'duration': content['duration'],
                                    'status': content['content_status'],
                                    'client_id': content['content_client_id'],
                                    'created_at': content['content_created_at'].isoformat() if content['content_created_at'] else None,
                                    'updated_at': content['content_updated_at'].isoformat() if content['content_updated_at'] else None
                                }
                            }
                            playlist_contents.append(content_item)

                        playlist_data = {
                            'id_playlist': schedule['id_playlist'],
                            'name': schedule['playlist_name'],
                            'client_id': schedule['playlist_client_id'],
                            'created_at': schedule['playlist_created_at'].isoformat() if schedule.get('playlist_created_at') else None,
                            'updated_at': schedule['playlist_updated_at'].isoformat() if schedule.get('playlist_updated_at') else None,
                            'contents': playlist_contents
                        }

                    schedule_item = {
                        'id_schedule': schedule['id_schedule'],
                        'group_id': schedule['group_id'],
                        'device_id': schedule['device_id'],
                        'playlist_id': schedule['playlist_id'],
                        'playlist': playlist_data,
                        'start_datetime': str(schedule['start_datetime']) if schedule['start_datetime'] else None,
                        'end_datetime': str(schedule['end_datetime']) if schedule['end_datetime'] else None,
                        'repeat_type': schedule['repeat_type'],
                        'days_of_week': schedule['days_of_week'],
                        'is_active': schedule['is_active'],
                        'created_at': schedule['created_at'].isoformat() if schedule['created_at'] else None,
                        'updated_at': schedule['updated_at'].isoformat() if schedule['updated_at'] else None
                    }
                    response_data['schedules'].append(schedule_item)
                
                print("\nSending response data to MQTT...")
                response_topic = f"device/info/{mac_address}"
                client.publish(response_topic, json.dumps(response_data), qos=1)
                print(f"Data sent to topic: {response_topic}")
                print(f"Processed {len(schedules_data)} schedules")
                
            except Exception as e:
                print(f"Error processing data: {e}")
                import traceback
                print("Stack trace:", traceback.format_exc())
            
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
        import traceback
        print("Stack trace:", traceback.format_exc())
    except Exception as e:
        print(f"General Error: {e}")
        import traceback
        print("Stack trace:", traceback.format_exc())

def check_and_save_mac_address(mac_address, client):
    print(f"\nProcessing MAC address: {mac_address}")
    try:
        # Clear retained message
        response_topic = f"device/status/{mac_address}"
        print(f"Clearing retained message for topic: {response_topic}")
        client.publish(response_topic, None, qos=1, retain=True)
        time.sleep(0.1)
        
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor(dictionary=True)
        
        try:
            check_query = "SELECT * FROM devices WHERE mac_address = %s"
            cursor.execute(check_query, (mac_address,))
            device = cursor.fetchone()
            
            if device is None:
                print(f"New MAC address detected: {mac_address}")
                insert_query = """
                    INSERT INTO devices (
                        mac_address, status, created_at, updated_at, first_seen, last_seen
                    ) VALUES (%s, 'UNREGISTERED', NOW(), NOW(), NOW(), NOW())
                """
                cursor.execute(insert_query, (mac_address,))
                connection.commit()
                status = 'UNREGISTERED'
            else:
                print(f"Existing MAC address found: {mac_address}")
                update_query = "UPDATE devices SET last_seen = NOW() WHERE mac_address = %s"
                cursor.execute(update_query, (mac_address,))
                connection.commit()
                status = device['status']
                
                get_device_schedules(mac_address, client)
            
            print(f"Publishing status '{status}' to topic: {response_topic}")
            client.publish(response_topic, status.encode(), qos=1, retain=False)
            
        except mysql.connector.Error as e:
            print(f"Database operation error: {e}")
            status = 'ERROR'
            client.publish(response_topic, status.encode(), qos=1, retain=False)
        
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
        import traceback
        print("Stack trace:", traceback.format_exc())
    except Exception as e:
        print(f"General Error: {e}")
        import traceback
        print("Stack trace:", traceback.format_exc())

def handle_playback_completion(client, mac_address, content_info):
    """Handle playback completion messages from devices"""
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        # Insert into mqtt_messages table dengan timestamp
        insert_query = """
            INSERT INTO mqtt_messages (
                device_id, 
                mac_address, 
                message, 
                type_message, 
                status,
                created_at,
                updated_at
            )
            SELECT 
                id_device, 
                %s, 
                %s, 
                'In', 
                'Done',
                NOW(),  # Tambahkan timestamp untuk created_at
                NOW()   # Tambahkan timestamp untuk updated_at
            FROM devices 
            WHERE mac_address = %s
        """
        
        message_json = json.dumps(content_info)
        cursor.execute(insert_query, (mac_address, message_json, mac_address))
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print(f"Playback completion recorded for device {mac_address}")
        
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error handling playback completion: {e}")

def on_connect(client, userdata, flags, rc):
    print(f"MQTT Connection result code: {rc}")
    if rc == 0:
        print("Successfully connected to MQTT broker")
        # Logging tambahan
        print(f"Connected to host: {MQTT_HOST}, port: {MQTT_PORT}")
        try:
            client.subscribe("device/mac_address", qos=1)
            client.subscribe("device/playback/complete/+", qos=1)
        except Exception as e:
            print(f"Error subscribing to topics: {e}")
    else:
        print(f"Failed to connect with code: {rc}")
        # Tambahkan interpretasi kode error
        error_messages = {
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorized"
        }
        print(f"Error detail: {error_messages.get(rc, 'Unknown error')}")

def on_message(client, userdata, msg):
    print(f"\nMessage received on topic: {msg.topic}")
    try:
        if msg.topic == "device/mac_address":
            mac_address = msg.payload.decode().strip()
            print(f"MAC Address received: {mac_address}")
            check_and_save_mac_address(mac_address, client)
        elif msg.topic.startswith("device/playback/complete/"):
            mac_address = msg.topic.split('/')[-1]
            content_info = json.loads(msg.payload.decode())
            handle_playback_completion(client, mac_address, content_info)
    except Exception as e:
        print(f"Error processing message: {e}")
        import traceback
        print("Stack trace:", traceback.format_exc())

def main():
    try:
        print("\n=== Starting MQTT Server ===")
        
        # Start cleanup scheduler
        start_cleanup_scheduler()
        
        # Start HTTP server untuk serving files
        start_http_server()
        
        client = mqtt.Client(client_id=f"python-server-{time.time()}")
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        client.on_connect = on_connect
        client.on_message = on_message
        client.enable_logger()
        
        print(f"Attempting connection to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        
        print("Starting MQTT loop...")
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\nProgram stopped by user")
    except Exception as e:
        print(f"Error during connection: {e}")
        import traceback
        print("Stack trace:", traceback.format_exc())
    finally:
        print("Program ended")

if __name__ == "__main__":
    print("Script started")
    main()