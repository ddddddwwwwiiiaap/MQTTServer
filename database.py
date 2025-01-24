import mysql.connector
from config import MYSQL_CONFIG
import threading
from datetime import datetime

def update_expired_devices():
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        
        # Update ke EXPIRED jika sudah melewati expired_at
        expired_query = """
            UPDATE devices 
            SET status = 'EXPIRED', updated_at = NOW()
            WHERE expired_at <= NOW() 
            AND status != 'EXPIRED'
        """
        
        # Update ke REGISTERED jika belum melewati expired_at
        registered_query = """
            UPDATE devices 
            SET status = 'REGISTERED', updated_at = NOW()
            WHERE expired_at > NOW() 
            AND status = 'EXPIRED'
        """
        
        cursor.execute(expired_query)
        expired_count = cursor.rowcount
        
        cursor.execute(registered_query)
        registered_count = cursor.rowcount
        
        connection.commit()
        
        if expired_count > 0:
            print(f"Updated {expired_count} devices to EXPIRED status")
        if registered_count > 0:
            print(f"Updated {registered_count} devices back to REGISTERED status")
        
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as e:
        print(f"Database error during status update: {e}")
    except Exception as e:
        print(f"Error during status update: {e}")

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
    # Run cleanup tasks
    cleanup_old_messages()
    update_expired_devices()
    
    # Schedule next run
    cleanup_timer = threading.Timer(3600.0, start_cleanup_scheduler)  # Run every hour
    cleanup_timer.daemon = True
    cleanup_timer.start()