import asyncio
import contextlib
import json
import logging
import os
from pathlib import Path

import aio_pika
from aio_pika import ExchangeType
from aiohttp import web, WSMsgType


logging.basicConfig(
    level=logging.INFO,
    format="[live_dashboard_backend] %(asctime)s %(levelname)s: %(message)s",
)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE_NAME = os.getenv("TICKS_EXCHANGE", "market_ticks")
BINDING_KEY = os.getenv("TICKS_BINDING_KEY", "ticks.#")
PORT = int(os.getenv("PORT", "8081"))

CLIENTS = set()


async def index(request: web.Request) -> web.Response:
    html_path = Path(__file__).parent / "live_ticker.html"
    html = html_path.read_text(encoding="utf-8")
    return web.Response(text=html, content_type="text/html")


async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "live_dashboard_backend"})


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    CLIENTS.add(ws)
    logging.info("Cliente WebSocket conectado. Total clientes: %s", len(CLIENTS))

    await ws.send_json(
        {
            "type": "status",
            "message": "Conectado al live dashboard",
        }
    )

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


async def broadcast_tick(tick: dict) -> None:
    if not CLIENTS:
        return

    message = json.dumps(tick)
    disconnected_clients = []

    for ws in list(CLIENTS):
        try:
            await ws.send_str(message)
        except Exception as exc:
            logging.warning("No se pudo enviar tick a un cliente: %s", exc)
            disconnected_clients.append(ws)

    for ws in disconnected_clients:
        CLIENTS.discard(ws)


async def on_tick_message(message: aio_pika.IncomingMessage) -> None:
    try:
        tick = json.loads(message.body.decode("utf-8"))
        tick["type"] = "tick"

        asset = tick.get("asset", "UNKNOWN")
        price = tick.get("price", "N/A")

        logging.info("Tick recibido: %s = %s", asset, price)

        await broadcast_tick(tick)

    except json.JSONDecodeError:
        logging.exception("Mensaje inválido: no es JSON")
    except Exception:
        logging.exception("Error procesando tick")


async def rabbitmq_consumer(app: web.Application) -> None:
    while True:
        connection = None

        try:
            logging.info("Conectando a RabbitMQ en %s", RABBITMQ_URL)

            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            app["rabbitmq_connection"] = connection

            channel = await connection.channel()
            await channel.set_qos(prefetch_count=100)

            exchange = await channel.declare_exchange(
                EXCHANGE_NAME,
                ExchangeType.TOPIC,
                durable=True,
            )

            queue = await channel.declare_queue(
                name="",
                exclusive=True,
                auto_delete=True,
            )

            await queue.bind(exchange, routing_key=BINDING_KEY)

            logging.info(
                "Suscrito al exchange '%s' con binding key '%s'",
                EXCHANGE_NAME,
                BINDING_KEY,
            )

            await queue.consume(on_tick_message, no_ack=True)

            await asyncio.Future()

        except asyncio.CancelledError:
            logging.info("Cancelando consumidor RabbitMQ")
            raise

        except Exception:
            logging.exception("Falló el consumidor RabbitMQ. Reintentando en 5 segundos...")
            await asyncio.sleep(5)

        finally:
            if connection and not connection.is_closed:
                await connection.close()


async def on_startup(app: web.Application) -> None:
    app["rabbitmq_task"] = asyncio.create_task(rabbitmq_consumer(app))


async def on_cleanup(app: web.Application) -> None:
    task = app.get("rabbitmq_task")

    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    connection = app.get("rabbitmq_connection")
    if connection and not connection.is_closed:
        await connection.close()


def create_app() -> web.Application:
    app = web.Application()

    app.router.add_get("/", index)
    app.router.add_get("/health", health)
    app.router.add_get("/ws", websocket_handler)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    return app


if __name__ == "__main__":
    logging.info("Iniciando live_dashboard_backend en puerto %s", PORT)
    web.run_app(create_app(), host="0.0.0.0", port=PORT)