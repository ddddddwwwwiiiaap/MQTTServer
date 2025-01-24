import mysql.connector
from config import MYSQL_CONFIG
import threading

def cleanup_old_messages():
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        
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
    cleanup_timer = threading.Timer(3600.0, start_cleanup_scheduler)
    cleanup_timer.daemon = True
    cleanup_timer.start()
    cleanup_old_messages()