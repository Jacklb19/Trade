# market_data_feed — Productor RabbitMQ
# Genera precios aleatorios cada segundo para 5 activos (BTC, ETH, SOL, ADA, BNB)
# y los publica en el Topic Exchange 'market_ticks' de RabbitMQ.

import pika
import json
import time
import random
import signal
import sys
import os
import logging
from datetime import datetime, timezone

# --- Logging estructurado ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [market_data_feed] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
EXCHANGE_NAME = "market_ticks"

# Precios base realistas (USD)
BASE_PRICES = {
    "BTC": 60000.00,
    "ETH": 3200.00,
    "SOL": 140.00,
    "ADA": 0.45,
    "BNB": 580.00,
}

current_prices = dict(BASE_PRICES)
running = True


def connect_rabbitmq():
    """Conexión con backoff exponencial: 1s, 2s, 4s, 8s... (máx 30s)."""
    delay = 1
    max_delay = 30
    while running:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600)
            )
            logger.info("Conectado a RabbitMQ")
            return connection
        except pika.exceptions.AMQPConnectionError:
            logger.warning(f"RabbitMQ no disponible, reintentando en {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
    return None


def graceful_shutdown(signum, frame):
    """Maneja SIGTERM/SIGINT para cierre limpio."""
    global running
    logger.info("Señal de apagado recibida, cerrando conexiones...")
    running = False
    sys.exit(0)


def main():
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    connection = connect_rabbitmq()
    if not connection:
        return

    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)

    logger.info("Publicando ticks de precios cada segundo...")
    tick_count = 0

    try:
        while running:
            for asset, price in current_prices.items():
                variation = random.uniform(-0.02, 0.02)
                new_price = round(price * (1 + variation), 2)
                current_prices[asset] = new_price

                tick = {
                    "asset": asset,
                    "price": new_price,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                routing_key = f"ticks.crypto.{asset.lower()}"

                channel.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key=routing_key,
                    body=json.dumps(tick),
                    properties=pika.BasicProperties(
                        delivery_mode=1,
                        content_type="application/json",
                    ),
                )

            tick_count += 1
            if tick_count % 10 == 0:
                prices_str = " | ".join(f"{a}: ${p:,.2f}" for a, p in current_prices.items())
                logger.info(f"Tick #{tick_count} — {prices_str}")

            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Detenido por el usuario")
    finally:
        if connection and connection.is_open:
            connection.close()
            logger.info("Conexión RabbitMQ cerrada")


if __name__ == "__main__":
    main()
