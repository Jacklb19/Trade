import asyncio
import contextlib
import json
import logging
import os
from pathlib import Path

from aiokafka import AIOKafkaConsumer
from aiohttp import web, WSMsgType


logging.basicConfig(
    level=logging.INFO,
    format="[investment_dashboard_backend] %(asctime)s %(levelname)s: %(message)s",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "trading_signals")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "invest_dashboard_group")
PORT = int(os.getenv("PORT", "8082"))

CLIENTS = set()
# Mantener estado en memoria con la última señal por activo
STATE = {}


async def index(request: web.Request) -> web.Response:
    html_path = Path(__file__).parent / "investment.html"
    if not html_path.exists():
        return web.Response(text="investment.html not found", status=404)
    html = html_path.read_text(encoding="utf-8")
    return web.Response(text=html, content_type="text/html")


async def health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok", 
        "service": "investment_dashboard_backend",
        "clients_connected": len(CLIENTS),
        "assets_tracked": list(STATE.keys())
    })


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    CLIENTS.add(ws)
    logging.info("Cliente WebSocket conectado. Total clientes: %s", len(CLIENTS))

    # Enviar inmediatamente el estado actual completo
    await ws.send_json({
        "type": "initial_state",
        "data": list(STATE.values())
    })

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                logging.debug("Mensaje recibido desde frontend: %s", msg.data)
            elif msg.type == WSMsgType.ERROR:
                logging.warning("Error WebSocket: %s", ws.exception())
    finally:
        CLIENTS.discard(ws)
        logging.info("Cliente WebSocket desconectado. Total clientes: %s", len(CLIENTS))

    return ws


async def broadcast_signal(signal: dict) -> None:
    if not CLIENTS:
        return

    message = json.dumps({
        "type": "signal",
        "data": signal
    })
    
    disconnected_clients = []

    for ws in list(CLIENTS):
        try:
            await ws.send_str(message)
        except Exception as exc:
            logging.warning("No se pudo enviar señal a un cliente: %s", exc)
            disconnected_clients.append(ws)

    for ws in disconnected_clients:
        CLIENTS.discard(ws)


async def kafka_consumer(app: web.Application) -> None:
    while True:
        consumer = None
        try:
            logging.info("Conectando a Kafka en %s, topic %s", KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME)
            
            consumer = AIOKafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest"
            )
            
            await consumer.start()
            app["kafka_consumer"] = consumer
            logging.info("Suscrito a Kafka topic '%s' con group.id '%s'", TOPIC_NAME, GROUP_ID)

            async for msg in consumer:
                signal = msg.value
                asset = signal.get("asset", "UNKNOWN")
                
                logging.info("Señal recibida: %s -> %s", asset, signal.get("signal"))
                
                # Actualizar estado en memoria
                STATE[asset] = signal
                
                # Broadcast de nuevas señales a todos los clientes
                await broadcast_signal(signal)

        except asyncio.CancelledError:
            logging.info("Cancelando consumidor Kafka")
            raise
        except Exception:
            logging.exception("Falló el consumidor Kafka. Reintentando en 5 segundos...")
            await asyncio.sleep(5)
        finally:
            if consumer:
                await consumer.stop()


async def on_startup(app: web.Application) -> None:
    app["kafka_task"] = asyncio.create_task(kafka_consumer(app))


async def on_cleanup(app: web.Application) -> None:
    task = app.get("kafka_task")
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    consumer = app.get("kafka_consumer")
    if consumer:
        await consumer.stop()


def create_app() -> web.Application:
    app = web.Application()

    app.router.add_get("/", index)
    app.router.add_get("/health", health)
    app.router.add_get("/ws", websocket_handler)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    return app


if __name__ == "__main__":
    logging.info("Iniciando investment_dashboard_backend en puerto %s", PORT)
    web.run_app(create_app(), host="0.0.0.0", port=PORT)
