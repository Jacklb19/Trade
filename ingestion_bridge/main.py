import json
import logging
import os
import time

import pika
from kafka import KafkaProducer
from pika.exceptions import AMQPConnectionError

logging.basicConfig(
    level=logging.INFO,
    format="[ingestion_bridge] %(asctime)s %(levelname)s: %(message)s",
)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
RABBITMQ_EXCHANGE = os.getenv("TICKS_EXCHANGE", "market_ticks")
RABBITMQ_BINDING_KEY = os.getenv("TICKS_BINDING_KEY", "ticks.#")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "historical_ticks")

def connect_to_rabbitmq():
    while True:
        try:
            logging.info("Conectando a RabbitMQ en %s", RABBITMQ_URL)
            parameters = pika.URLParameters(RABBITMQ_URL)
            return pika.BlockingConnection(parameters)
        except AMQPConnectionError:
            logging.warning("RabbitMQ no disponible. Reintentando en 5 segundos...")
            time.sleep(5)

def connect_to_kafka():
    while True:
        try:
            logging.info("Conectando a Kafka en %s", KAFKA_BOOTSTRAP_SERVERS)
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            return producer
        except Exception as e:
            logging.warning("Kafka no disponible (%s). Reintentando en 5 segundos...", e)
            time.sleep(5)

def main():
    # Conexiones
    rabbitmq_conn = connect_to_rabbitmq()
    rabbitmq_channel = rabbitmq_conn.channel()
    
    kafka_producer = connect_to_kafka()

    # Declarar el exchange en RabbitMQ
    rabbitmq_channel.exchange_declare(
        exchange=RABBITMQ_EXCHANGE,
        exchange_type="topic",
        durable=False,
    )

    # Declarar cola exclusiva, anónima y no duradera
    result = rabbitmq_channel.queue_declare(
        queue="",
        exclusive=True,
        auto_delete=True,
        durable=False,
    )
    queue_name = result.method.queue

    # Bind
    rabbitmq_channel.queue_bind(
        exchange=RABBITMQ_EXCHANGE,
        queue=queue_name,
        routing_key=RABBITMQ_BINDING_KEY,
    )

    def on_tick_callback(ch, method, properties, body):
        try:
            tick = json.loads(body.decode("utf-8"))
            asset = tick.get("asset", "UNKNOWN")
            
            logging.info("Puenteando tick: %s -> %s", asset, tick.get("price"))
            
            # Publicar en Kafka usando el activo como key de partición
            kafka_producer.send(
                topic=KAFKA_TOPIC,
                key=asset,
                value=tick,
            )
            kafka_producer.flush()
            
        except Exception as e:
            logging.error("Error puenteando mensaje a Kafka: %s", e)

    rabbitmq_channel.basic_consume(
        queue=queue_name,
        on_message_callback=on_tick_callback,
        auto_ack=True,
    )

    logging.info("Puente de ingestión listo. Escuchando ticks de RabbitMQ y enviando a Kafka...")
    
    try:
        rabbitmq_channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Puente detenido manualmente")
    finally:
        if rabbitmq_conn and rabbitmq_conn.is_open:
            rabbitmq_conn.close()
        if kafka_producer:
            kafka_producer.close()

if __name__ == "__main__":
    main()
