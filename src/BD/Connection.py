import psycopg2 # libreria para base de datos
from dotenv import load_dotenv
import os

load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME_LOAD = os.getenv('DB_NAME_LOAD')


def get_connection():
    try:
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        
        print("Conexión exitosa a la base de datos")
        return connection

    except Exception as error:
        print(f"Ocurrió un error al conectar a la base de datos principal {error}")


def get_connection_load():
    try:
        connection = psycopg2.connect(
            dbname=DB_NAME_LOAD,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        
        print("Conexión exitosa a la base de datos de carga")
        return connection

    except Exception as error:
        print(f"Ocurrió un error al conectar a la base de datos de carga {error}")


if __name__ == '__main__':
    pass
