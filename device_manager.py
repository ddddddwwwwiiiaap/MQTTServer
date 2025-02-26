import mysql.connector
import json
import time
from datetime import datetime
from config import MYSQL_CONFIG
from content_manager import ensure_download_directory, download_content

def get_device_schedules(mac_address, client):
    print(f"\nGetting schedules for device: {mac_address}")
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
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
                                    'local_url': local_url,
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
        response_topic = f"device/status/{mac_address}"
        print(f"Clearing retained message for topic: {response_topic}")
        client.publish(response_topic, None, qos=1, retain=True)
        time.sleep(0.1)
        
        connection = mysql.connector.connect(**MYSQL_CONFIG)
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
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        
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
                NOW(),
                NOW()
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