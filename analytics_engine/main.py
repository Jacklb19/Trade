# analytics_engine — Consumidor Kafka / Productor Kafka (Stateful)
# Consume del topic 'historical_ticks', calcula la SMA de 5 períodos
# y genera señales BUY/SELL basadas en momentum.

import json
import time
import signal
import sys
import os
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Logging estructurado ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [analytics_engine] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = "historical_ticks"
OUTPUT_TOPIC = "trading_signals"
SMA_WINDOW = 5
BUY_THRESHOLD = 1.05   # Precio > SMA * 1.05 → BUY
SELL_THRESHOLD = 0.95   # Precio < SMA * 0.95 → SELL

# Estado en memoria: historial de precios por asset
price_history = {}
# Cooldown: evitar señales repetidas consecutivas por asset
last_signal = {}

running = True


def connect_kafka():
    """Conexión con backoff exponencial: 1s, 2s, 4s, 8s... (máx 30s)."""
    delay = 1
    max_delay = 30
    while running:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="analytics_engine_group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            logger.info("Conectado a Kafka")
            return consumer, producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka no disponible, reintentando en {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
    return None, None


def calculate_sma(prices, window=SMA_WINDOW):
    """Calcula la Media Móvil Simple de los últimos 'window' precios."""
    if len(prices) < window:
        return None
    return sum(prices[-window:]) / window


def graceful_shutdown(signum, frame):
    global running
    logger.info("Señal de apagado recibida, cerrando...")
    running = False
    sys.exit(0)


def main():
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    consumer, producer = connect_kafka()
    if not consumer:
        return

    logger.info(f"Consumiendo de '{INPUT_TOPIC}', SMA window={SMA_WINDOW}")
    logger.info(f"Umbral BUY: precio > SMA * {BUY_THRESHOLD} | SELL: precio < SMA * {SELL_THRESHOLD}")

    signals_generated = 0

    for message in consumer:
        if not running:
            break

        tick = message.value
        asset = tick.get("asset")
        price = tick.get("price")

        if not asset or price is None:
            continue

        # Actualizar historial
        if asset not in price_history:
            price_history[asset] = []

        price_history[asset].append(price)

        # Mantener solo los últimos 100 precios en memoria
        if len(price_history[asset]) > 100:
            price_history[asset] = price_history[asset][-100:]

        # Calcular SMA
        sma = calculate_sma(price_history[asset])

        if sma is None:
            logger.debug(f"{asset}: ${price:,.2f} (acumulando: {len(price_history[asset])}/{SMA_WINDOW})")
            continue

        sma_rounded = round(sma, 2)
        signal_type = None
        reason = None

        # Verificar señal de BUY (momentum alcista)
        if price > sma * BUY_THRESHOLD:
            signal_type = "BUY"
            reason = "Momentum Crossover (Alcista)"

        # Verificar señal de SELL (momentum bajista)
        elif price < sma * SELL_THRESHOLD:
            signal_type = "SELL"
            reason = "Momentum Crossover (Bajista)"

        if signal_type:
            # Cooldown: no repetir la misma señal consecutiva para el mismo asset
            if last_signal.get(asset) == signal_type:
                logger.debug(f"{asset}: señal {signal_type} repetida, ignorando (cooldown)")
                continue

            last_signal[asset] = signal_type
            signals_generated += 1

            signal_msg = {
                "asset": asset,
                "signal": signal_type,
                "reason": reason,
                "price": price,
                "sma": sma_rounded,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            producer.send(OUTPUT_TOPIC, key=asset, value=signal_msg)
            producer.flush()

            emoji = "🚨" if signal_type == "BUY" else "📉"
            logger.info(f"{emoji} SEÑAL {signal_type} #{signals_generated} | {asset}: ${price:,.2f} vs SMA=${sma_rounded:,.2f}")
        else:
            logger.debug(f"{asset}: ${price:,.2f} | SMA={sma_rounded:,.2f} — sin señal")

    if producer:
        producer.close()


if __name__ == "__main__":
    main()
