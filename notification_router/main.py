import json
import logging
import os
import time

import pika
from kafka import KafkaConsumer
from pika.exceptions import AMQPConnectionError


logging.basicConfig(
    level=logging.INFO,
    format="[notification_router] %(asctime)s %(levelname)s: %(message)s",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "trading_signals")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "notification_router_group")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EMAIL_QUEUE = os.getenv("EMAIL_QUEUE", "email_queue")
CONSOLE_ALERTS_EXCHANGE = os.getenv("CONSOLE_ALERTS_EXCHANGE", "console_alerts")


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
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            return consumer
        except Exception as e:
            logging.warning("Kafka no disponible (%s). Reintentando en 5 segundos...", e)
            time.sleep(5)


def main():
    # Inicializar RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    # Declaraciones idempotentes
    channel.queue_declare(queue=EMAIL_QUEUE, durable=True)
    channel.exchange_declare(
        exchange=CONSOLE_ALERTS_EXCHANGE, 
        exchange_type="fanout", 
        durable=True
    )

    # Inicializar Kafka
    consumer = connect_to_kafka()
    logging.info("Suscrito a Kafka topic '%s' con group.id '%s'", TOPIC_NAME, GROUP_ID)

    try:
        for msg in consumer:
            signal = msg.value
            asset = signal.get("asset", "UNKNOWN")
            logging.info("Reenviando señal de %s a RabbitMQ", asset)

            body = json.dumps(signal)

            # Publicar en email_queue (Work Queue)
            channel.basic_publish(
                exchange="",
                routing_key=EMAIL_QUEUE,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensaje persistente
                    content_type="application/json"
                )
            )

            # Publicar en console_alerts (Fanout Exchange)
            channel.basic_publish(
                exchange=CONSOLE_ALERTS_EXCHANGE,
                routing_key="",
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensaje persistente
                    content_type="application/json"
                )
            )

    except KeyboardInterrupt:
        logging.info("Router detenido manualmente")
    except Exception:
        logging.exception("Error en el bucle principal")
    finally:
        if connection and connection.is_open:
            connection.close()
        if consumer:
            consumer.close()


if __name__ == "__main__":
    main()
