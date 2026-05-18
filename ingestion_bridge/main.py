# ingestion_bridge — Puente RabbitMQ → Kafka
# Consume ticks del Topic Exchange de RabbitMQ y los republica
# en el topic 'historical_ticks' de Kafka usando el asset como key.

import pika
import json
import time
import signal
import sys
import os
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Logging estructurado ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ingestion_bridge] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
EXCHANGE_NAME = "market_ticks"
KAFKA_TOPIC = "historical_ticks"

running = True
bridge_count = 0


def connect_kafka_producer():
    """Conexión Kafka con backoff exponencial."""
    delay = 1
    max_delay = 30
    while running:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            logger.info("Conectado a Kafka")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka no disponible, reintentando en {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
    return None


def connect_rabbitmq():
    """Conexión RabbitMQ con backoff exponencial."""
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
    global running
    logger.info("Señal de apagado recibida, cerrando...")
    running = False
    sys.exit(0)


def main():
    global bridge_count

    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    kafka_producer = connect_kafka_producer()
    connection = connect_rabbitmq()
    if not kafka_producer or not connection:
        return

    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key="ticks.#")

    def callback(ch, method, properties, body):
        global bridge_count
        tick = json.loads(body)
        asset = tick.get("asset", "UNKNOWN")

        kafka_producer.send(KAFKA_TOPIC, key=asset, value=tick)
        bridge_count += 1

        if bridge_count % 25 == 0:
            logger.info(f"Mensajes transferidos: {bridge_count} (último: {asset})")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    logger.info("Puente RabbitMQ → Kafka activo...")
    try:
        channel.start_consuming()
    finally:
        if kafka_producer:
            kafka_producer.close()
        if connection and connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
