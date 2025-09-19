# Configuración de la base de datos de tickets usando psycopg2
import psycopg2
from psycopg2.extras import RealDictCursor

# Credenciales de la base de datos usando Shared Pooler (IPv4 compatible)
DB_CONFIG = {
    "user": "postgres.wawpmejhydmlxdofbsfs",
    "password": "Valen44043436?",
    "host": "aws-1-us-east-1.pooler.supabase.com",
    "port": "5432",
    "dbname": "postgres"
}

def get_database_connection():
    """Establece conexión con la base de datos"""
    try:
        connection = psycopg2.connect(
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            dbname=DB_CONFIG["dbname"]
        )
        return connection
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        return None

def test_connection():
    """Prueba la conexión a la base de datos"""
    try:
        connection = get_database_connection()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT NOW();")
            result = cursor.fetchone()
            print(f"✅ Conexión exitosa! Hora actual: {result[0]}")
            cursor.close()
            connection.close()
            return True
        else:
            print("❌ No se pudo establecer conexión")
            return False
    except Exception as e:
        print(f"❌ Error en la prueba de conexión: {e}")
        return False