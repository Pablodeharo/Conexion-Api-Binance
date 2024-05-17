import pandas as pd
from binance.client import Client
import sqlite3
import schedule
import time
from dotenv import load_dotenv
import os

def connect_to_binance(api_key, api_secret):
    client = Client(api_key=api_key, api_secret=api_secret)
    return client

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las claves de las variables de entorno
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

# Llamar a la función para conectarse a la API de Binance
binance_client = connect_to_binance(api_key, api_secret)

# Conectar a la base de datos (creará el achivo si no existe)
conn = sqlite3.connect('crypto_data.db')
conn.close()

# Llamar a la función para conectarse a la API de Binance
binance_client = connect_to_binance(api_key, api_secret)

def get_crypto_data():
    # Conectar a la base de datos
    conn = sqlite3.connect('crypto_data.db')

    # Llamar a la función para conectarse a la API de Binance
    binance_client = connect_to_binance(api_key, api_secret)

    # Definir los símbolos de las criptomonedas y el intervalo de tiempo
    symbols = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'LTCUSDT']
    interval = '1h'

    for symbol in symbols:
        # Obtener los datos del intervalo de tiempo especificado
        klines = binance_client.get_klines(symbol=symbol, interval=interval)
    
        # Procesar los datos y crear un DataFrame de Pandas
        data = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
    
        # Seleccionar solo las columnas que necesitamos
        data = data[['timestamp', 'open', 'close', 'high', 'low', 'volume']]
    
        # Convertir el timestamp a formato de fecha y hora
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    
        # Guardar los datos en la base de datos
        data.to_sql(f'{symbol.lower()}_1h_data', conn, if_exists='replace', index=False)

# Obtener los últimos dos conjuntos de datos actualizados de la base de datos
    for symbol in symbols:
        query = f'SELECT * FROM {symbol.lower()}_1h_data ORDER BY timestamp DESC LIMIT 2'
        last_two_updates = pd.read_sql_query(query, conn)
        print(f'Últimas dos actualizaciones para {symbol}:')
        print(last_two_updates)
        print()

# Cerrar la conexión a la base de datos
conn.close()

# Programar la ejecución de la función get_crypto_data cada hora
schedule.every().hour.do(get_crypto_data)
# Ejecutar el bucle principal para que el programa se ejecute continuamente
while True:
    schedule.run_pending()
    time.sleep(60)  # Esperar 60 segundos antes de volver a revisar el schedule