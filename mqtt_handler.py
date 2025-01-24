import paho.mqtt.client as mqtt
import json
from config import MQTT_CONFIG
from device_manager import check_and_save_mac_address, handle_playback_completion

def on_connect(client, userdata, flags, rc):
    print(f"MQTT Connection result code: {rc}")
    if rc == 0:
        print("Successfully connected to MQTT broker")
        print(f"Connected to host: {MQTT_CONFIG['HOST']}, port: {MQTT_CONFIG['PORT']}")
        try:
            client.subscribe("device/mac_address", qos=1)
            client.subscribe("device/playback/complete/+", qos=1)
        except Exception as e:
            print(f"Error subscribing to topics: {e}")
    else:
        print(f"Failed to connect with code: {rc}")
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