import json
import logging
import os
import time

import pika
from pika.exceptions import AMQPConnectionError


logging.basicConfig(
    level=logging.INFO,
    format="[console_alert_worker] %(asctime)s %(levelname)s: %(message)s",
)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
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


def handle_console_alert(ch, method, properties, body):
    try:
        signal = json.loads(body.decode("utf-8"))

        asset = signal.get("asset", "UNKNOWN")
        action = signal.get("signal", "UNKNOWN")
        reason = signal.get("reason", "Sin razón")
        price = signal.get("price", "N/A")

        logging.info(
            "*** ALERTA DE SISTEMA: SEÑAL DE %s EN %s | Razón: %s | Precio: %s ***",
            action,
            asset,
            reason,
            price,
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        logging.exception("Mensaje inválido recibido en console_alerts")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception:
        logging.exception("Error procesando alerta de consola")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    while True:
        connection = None

        try:
            connection = connect_to_rabbitmq()
            channel = connection.channel()

            channel.exchange_declare(
                exchange=CONSOLE_ALERTS_EXCHANGE,
                exchange_type="fanout",
                durable=True,
            )

            result = channel.queue_declare(
                queue="",
                exclusive=True,
                auto_delete=True,
            )

            queue_name = result.method.queue

            channel.queue_bind(
                exchange=CONSOLE_ALERTS_EXCHANGE,
                queue=queue_name,
            )

            channel.basic_consume(
                queue=queue_name,
                on_message_callback=handle_console_alert,
                auto_ack=False,
            )

            logging.info(
                "Esperando alertas en Fanout Exchange '%s'",
                CONSOLE_ALERTS_EXCHANGE,
            )

            channel.start_consuming()

        except KeyboardInterrupt:
            logging.info("Worker detenido manualmente")
            break

        except Exception:
            logging.exception("Falló console_alert_worker. Reintentando en 5 segundos...")
            time.sleep(5)

        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()