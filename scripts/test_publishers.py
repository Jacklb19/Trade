import json
import os
import sys
from datetime import datetime, timezone

import pika


RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


def get_channel():
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    return connection, channel


def publish_tick():
    connection, channel = get_channel()

    channel.exchange_declare(
        exchange="market_ticks",
        exchange_type="topic",
        durable=False,
    )

    tick = {
        "asset": "BTC",
        "price": 63000.45,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    channel.basic_publish(
        exchange="market_ticks",
        routing_key="ticks.crypto.btc",
        body=json.dumps(tick),
        properties=pika.BasicProperties(content_type="application/json"),
    )

    print("Tick publicado:", tick)

    connection.close()


def publish_signal():
    connection, channel = get_channel()

    channel.queue_declare(
        queue="email_queue",
        durable=True,
    )

    channel.exchange_declare(
        exchange="console_alerts",
        exchange_type="fanout",
        durable=True,
    )

    signal = {
        "asset": "BTC",
        "signal": "BUY",
        "reason": "Momentum Crossover",
        "price": 68000.00,
        "sma": 61200.00,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    channel.basic_publish(
        exchange="",
        routing_key="email_queue",
        body=json.dumps(signal),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )

    channel.basic_publish(
        exchange="console_alerts",
        routing_key="",
        body=json.dumps(signal),
        properties=pika.BasicProperties(content_type="application/json"),
    )

    print("Señal publicada:", signal)

    connection.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso:")
        print("  python scripts/test_publishers.py tick")
        print("  python scripts/test_publishers.py signal")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "tick":
        publish_tick()
    elif mode == "signal":
        publish_signal()
    else:
        print("Modo inválido. Usa: tick o signal")
        sys.exit(1)