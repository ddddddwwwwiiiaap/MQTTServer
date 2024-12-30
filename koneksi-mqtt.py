import paho.mqtt.client as mqtt
import mysql.connector
import json
from datetime import datetime

# Fungsi untuk mengkonversi datetime menjadi string
def datetime_handler(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

# Konfigurasi MySQL
mysql_config = {
    'host': '192.168.100.8',
    'user': 'root',
    'password': 'root',
    'database': 'dashboardadmin'
}

# Fungsi untuk mengambil data dari MySQL
def get_data_from_mysql():
    try:
        print("Mencoba koneksi ke MySQL...")
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor(dictionary=True)
        
        query = "SELECT * FROM users"
        print(f"Menjalankan query: {query}")
        
        cursor.execute(query)
        result = cursor.fetchall()
        print(f"Data yang didapat: {result}")
        
        cursor.close()
        connection.close()
        return result
    except mysql.connector.Error as e:
        print(f"Error MySQL: {e}")
        return None
    except Exception as e:
        print(f"Error umum: {e}")
        return None

# Callback saat client MQTT terhubung
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("request/data")
    # Langsung ambil data saat pertama kali connect
    data = get_data_from_mysql()
    if data:
        json_data = json.dumps(data, default=datetime_handler)
        client.publish("response/data", json_data)
        print(f"Data terkirim ke topic response/data")

# Callback saat ada message masuk
def on_message(client, userdata, msg):
    print(f"Pesan diterima pada topic: {msg.topic}")
    if msg.topic == "request/data":
        data = get_data_from_mysql()
        if data:
            json_data = json.dumps(data, default=datetime_handler)
            client.publish("response/data", json_data)
            print(f"Data terkirim ke topic response/data")

# Inisialisasi MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Koneksi ke broker MQTT
print("Mencoba koneksi ke MQTT broker...")
client.connect("localhost", 1883, 60)

# Loop forever
client.loop_forever()
