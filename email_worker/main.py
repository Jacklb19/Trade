import json
import logging
import os
import time

import pika
from pika.exceptions import AMQPConnectionError


logging.basicConfig(
    level=logging.INFO,
    format="[email_worker] %(asctime)s %(levelname)s: %(message)s",
)

import socket

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EMAIL_QUEUE = os.getenv("EMAIL_QUEUE", "email_queue")
EMAIL_SIMULATION_SECONDS = float(os.getenv("EMAIL_SIMULATION_SECONDS", "0.5"))
WORKER_ID = os.getenv("WORKER_ID", socket.gethostname())


def connect_to_rabbitmq():
    while True:
        try:
            logging.info("Conectando a RabbitMQ en %s", RABBITMQ_URL)
            parameters = pika.URLParameters(RABBITMQ_URL)
            return pika.BlockingConnection(parameters)

        except AMQPConnectionError:
            logging.warning("RabbitMQ no disponible. Reintentando en 5 segundos...")
            time.sleep(5)


def handle_email(ch, method, properties, body):
    try:
        signal = json.loads(body.decode("utf-8"))

        asset = signal.get("asset", "UNKNOWN")
        action = signal.get("signal", "UNKNOWN")
        reason = signal.get("reason", "Sin razón")
        price = signal.get("price", "N/A")
        sma = signal.get("sma", "N/A")

        logging.info(
            "ENVIANDO EMAIL [Worker %s]: ¡Momento de Invertir! Señal de %s para %s | "
            "Razón: %s | Precio: %s | SMA: %s",
            WORKER_ID,
            action,
            asset,
            reason,
            price,
            sma,
        )

        time.sleep(EMAIL_SIMULATION_SECONDS)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        logging.exception("Mensaje inválido en email_queue. Se descarta.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception:
        logging.exception("Error procesando email. Se reencola el mensaje.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    while True:
        connection = None

        try:
            connection = connect_to_rabbitmq()
            channel = connection.channel()

            channel.queue_declare(
                queue=EMAIL_QUEUE,
                durable=True,
            )

            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue=EMAIL_QUEUE,
                on_message_callback=handle_email,
                auto_ack=False,
            )

            logging.info("Esperando mensajes en cola '%s'", EMAIL_QUEUE)
            channel.start_consuming()

        except KeyboardInterrupt:
            logging.info("Worker detenido manualmente")
            break

        except Exception:
            logging.exception("Falló email_worker. Reintentando en 5 segundos...")
            time.sleep(5)

        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()