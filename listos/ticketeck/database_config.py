import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "user": "postgres.wawpmejhydmlxdofbsfs",
    "password": "Valen44043436?",
    "host": "aws-1-us-east-1.pooler.supabase.com",
    "port": "5432",
    "dbname": "postgres"
}

def get_database_connection():
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
