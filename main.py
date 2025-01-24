import time
import paho.mqtt.client as mqtt
from mqtt_handler import on_connect, on_message
from database import start_cleanup_scheduler
from file_server import start_http_server
from config import MQTT_CONFIG

def main():
    try:
        print("\n=== Starting MQTT Server ===")
        
        # Start cleanup scheduler
        start_cleanup_scheduler()
        
        # Start HTTP server
        start_http_server()
        
        # Initialize MQTT client
        client = mqtt.Client(client_id=f"python-server-{time.time()}")
        client.username_pw_set(MQTT_CONFIG['USER'], MQTT_CONFIG['PASSWORD'])
        client.on_connect = on_connect
        client.on_message = on_message
        client.enable_logger()
        
        print(f"Attempting connection to MQTT broker at {MQTT_CONFIG['HOST']}:{MQTT_CONFIG['PORT']}")
        client.connect(MQTT_CONFIG['HOST'], MQTT_CONFIG['PORT'], 60)
        
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