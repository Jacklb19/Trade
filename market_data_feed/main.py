import json
import logging
import os
import random
import time
from datetime import datetime, timezone

import pika
from pika.exceptions import AMQPConnectionError

logging.basicConfig(
    level=logging.INFO,
    format="[market_data_feed] %(asctime)s %(levelname)s: %(message)s",
)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE_NAME = os.getenv("TICKS_EXCHANGE", "market_ticks")

# Precios base reales
PRICES = {
    "BTC": 62000.0,
    "ETH": 3100.0,
    "SOL": 145.0,
    "ADA": 0.45,
    "BNB": 580.0,
}

def connect_to_rabbitmq():
    while True:
        try:
            logging.info("Conectando a RabbitMQ en %s", RABBITMQ_URL)
            parameters = pika.URLParameters(RABBITMQ_URL)
            return pika.BlockingConnection(parameters)
        except AMQPConnectionError:
            logging.warning("RabbitMQ no disponible. Reintentando en 5 segundos...")
            time.sleep(5)

def main():
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    # Declarar el exchange de ticks de mercado (no durable para el hot-path)
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type="topic",
        durable=False,
    )

    logging.info("Iniciando generación de ticks de mercado...")

    try:
        while True:
            # Variar precios ±2%
            for asset, base_price in PRICES.items():
                change = random.uniform(-0.02, 0.02)
                PRICES[asset] = round(base_price * (1 + change), 2)
                
                tick = {
                    "asset": asset,
                    "price": PRICES[asset],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                
                routing_key = f"ticks.crypto.{asset.lower()}"
                body = json.dumps(tick)
                
                channel.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=1,  # Transmitir rápido (no persistente en Hot-Path)
                    )
                )
                
                logging.info("Tick publicado: %s = %s", asset, PRICES[asset])
            
            # Dormir 1 segundo antes del siguiente lote de ticks
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Generación de ticks detenida")
    except Exception as e:
        logging.error("Error en el market_data_feed: %s", e)
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()
