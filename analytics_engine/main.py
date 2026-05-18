import json
import logging
import os
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="[analytics_engine] %(asctime)s %(levelname)s: %(message)s",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONSUME_TOPIC = os.getenv("KAFKA_CONSUME_TOPIC", "historical_ticks")
PRODUCE_TOPIC = os.getenv("KAFKA_PRODUCE_TOPIC", "trading_signals")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "analytics_engine_group")

# Estado en memoria para el historial de precios por activo
HISTORY = {}
# Control de cooldown de señales consecutivas por activo para evitar spam de alertas idénticas
LAST_SIGNAL = {}

def connect_to_kafka_consumer():
    while True:
        try:
            logging.info("Conectando consumidor Kafka en %s", KAFKA_BOOTSTRAP_SERVERS)
            consumer = KafkaConsumer(
                CONSUME_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            return consumer
        except Exception as e:
            logging.warning("Kafka no disponible (%s). Reintentando en 5 segundos...", e)
            time.sleep(5)

def connect_to_kafka_producer():
    while True:
        try:
            logging.info("Conectando productor Kafka en %s", KAFKA_BOOTSTRAP_SERVERS)
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
    consumer = connect_to_kafka_consumer()
    producer = connect_to_kafka_producer()
    
    logging.info("analytics_engine listo. Procesando ticks e identificando señales de inversión...")

    try:
        for msg in consumer:
            tick = msg.value
            asset = tick.get("asset", "UNKNOWN")
            price = tick.get("price", 0.0)
            
            if asset not in HISTORY:
                HISTORY[asset] = []
                
            HISTORY[asset].append(price)
            
            # Mantener solo los últimos 5 precios
            if len(HISTORY[asset]) > 5:
                HISTORY[asset].pop(0)
                
            # Solo calcular la SMA si tenemos al menos 5 datos
            if len(HISTORY[asset]) == 5:
                sma = round(sum(HISTORY[asset]) / 5, 2)
                logging.info("Cálculo para %s: Precio = %s | SMA(5) = %s", asset, price, sma)
                
                signal_type = None
                reason = ""
                
                # Regla de inversión BUY: Precio actual supera en 5% la SMA
                if price > sma * 1.05:
                    signal_type = "BUY"
                    reason = f"Momentum Crossover Alcista (Precio actual supera SMA en >5%)"
                # Regla de inversión SELL: Precio actual es menor que el 95% de la SMA
                elif price < sma * 0.95:
                    signal_type = "SELL"
                    reason = f"Momentum Crossover Bajista (Precio actual es inferior a SMA en >5%)"
                
                if signal_type:
                    # Validar cooldown para evitar spam: no repetir señal idéntica consecutiva
                    if LAST_SIGNAL.get(asset) != signal_type:
                        LAST_SIGNAL[asset] = signal_type
                        
                        signal_event = {
                            "asset": asset,
                            "signal": signal_type,
                            "reason": reason,
                            "price": price,
                            "sma": sma,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                        
                        producer.send(
                            topic=PRODUCE_TOPIC,
                            key=asset,
                            value=signal_event,
                        )
                        producer.flush()
                        
                        logging.info("¡SEÑAL DETECTADA Y PUBLICADA! %s -> %s (Precio: %s)", asset, signal_type, price)

    except KeyboardInterrupt:
        logging.info("Motor de análisis detenido")
    finally:
        if consumer:
            consumer.close()
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
