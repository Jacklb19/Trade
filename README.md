# 📈 Plataforma de Trading y Alertas de Inversión

> **Proyecto Final — Sistemas Distribuidos | Grupo 7**
> Arquitectura Hot-Path / Cold-Path para un broker de criptomonedas distribuido

---

## 🏗️ Visión General

Sistema distribuido que implementa el núcleo de un broker de criptomonedas (similar a Binance) usando una **arquitectura híbrida de mensajería** con dos flujos de datos simultáneos:

| Ruta | Tecnología | Propósito | CAP | PACELC |
|------|-----------|-----------|-----|--------|
| **Hot-Path** 🔴 | RabbitMQ | Ticker en vivo, latencia mínima | AP | EL |
| **Cold-Path** 🔵 | Kafka (KRaft) | Análisis histórico, persistencia | CP | EC |

El sistema corre **12 contenedores Docker** (2 infraestructura + 10 microservicios, incluyendo 3 réplicas de email_worker) orquestados con Docker Compose y expone **2 dashboards web en tiempo real**.

---

## 📁 Estructura del Proyecto

```
trading_platform/
│
├── docker-compose.yml              # Orquestación de todos los servicios
├── .gitignore
├── README.md
│
├── market_data_feed/                # Productor RabbitMQ
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                      # Genera y publica ticks cada segundo
│
├── live_dashboard_backend/          # Hot-Path WebSocket :8081
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                      # Consume RabbitMQ → WebSocket
│   └── live_ticker.html             # Dashboard de precios en vivo
│
├── ingestion_bridge/                # Puente RabbitMQ → Kafka
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
├── analytics_engine/                # Motor SMA stateful
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                      # Calcula SMA(5), genera señales BUY/SELL
│
├── investment_dashboard_backend/    # Cold-Path WebSocket :8082
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                      # Consume Kafka → WebSocket
│   └── investment.html              # Dashboard de señales de inversión
│
├── notification_router/             # Kafka → RabbitMQ (email + alerts)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
├── email_worker/                    # Work Queue consumer
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                      # Simula envío de emails
│
└── console_alert_worker/            # Fanout Exchange consumer
    ├── Dockerfile
    ├── requirements.txt
    └── main.py                      # Imprime alertas de sistema
```

---

## 🔄 Flujo de Datos

```
market_data_feed
       │
       ▼
   [RabbitMQ]  ──── Topic Exchange "market_ticks" ────┐
       │                                               │
       ├──► live_dashboard_backend ──► WebSocket :8081  │
       │         (HOT-PATH)           live_ticker.html  │
       │                                               │
       └──► ingestion_bridge ──────────────────────────┘
                    │
                    ▼
               [Kafka]  ──── Topic "historical_ticks"
                    │
                    ▼
            analytics_engine  (SMA stateful)
                    │
                    ▼
               [Kafka]  ──── Topic "trading_signals"
                    │
           ┌────────┴────────┐
           ▼                 ▼
  investment_dashboard    notification_router
  _backend                     │
       │                  ┌────┴────┐
       ▼                  ▼         ▼
  WebSocket :8082    [RabbitMQ]  [RabbitMQ]
  investment.html    email_queue  console_alerts
                         │         (Fanout)
                    ┌────┼────┐         │
                    ▼    ▼    ▼    console_alert
               worker1 worker2 worker3  _worker
                 (round-robin)
```

---

## 🚀 Comandos de Ejecución

### Requisitos Previos
- [Docker](https://docs.docker.com/get-docker/) y [Docker Compose](https://docs.docker.com/compose/install/) instalados

### Levantar el sistema

```bash
# Primera vez o después de cambios
docker compose up --build

# En segundo plano
docker compose up --build -d

# Ver logs de un servicio específico
docker compose logs -f analytics_engine
docker compose logs -f email_worker

# Ver todos los logs
docker compose logs -f

# Detener todo
docker compose down

# Detener y eliminar volúmenes (empezar desde cero)
docker compose down -v
```

### Dashboards Accesibles

| Dashboard | URL | Descripción |
|-----------|-----|-------------|
| **Precios en vivo** | http://localhost:8081 | `live_ticker.html` — Hot-Path |
| **Señales de inversión** | http://localhost:8082 | `investment.html` — Cold-Path |
| **Panel RabbitMQ** | http://localhost:15672 | Usuario: `guest` / Contraseña: `guest` |

---

## 🧩 Microservicios

### `market_data_feed` — Productor RabbitMQ
Genera precios aleatorios cada segundo para **5 activos** (BTC, ETH, SOL, ADA, BNB) con variaciones de ±2%. Publica en el Topic Exchange `market_ticks` con routing keys como `ticks.crypto.btc`.

### `live_dashboard_backend` — Hot-Path WebSocket `:8081`
Se suscribe a `ticks.#` en RabbitMQ y retransmite cada tick por WebSocket al `live_ticker.html`. **Cero procesamiento intermedio** = latencia mínima.

### `ingestion_bridge` — Puente RabbitMQ → Kafka
Consume los mismos ticks del Topic Exchange y los publica en Kafka `historical_ticks` usando el asset como key de partición.

### `analytics_engine` — Motor de Análisis (Stateful)
Consume de `historical_ticks`, mantiene historial de precios en memoria y calcula la **SMA(5)**. Genera señales bidireccionales:
- **BUY**: `precio > SMA × 1.05` (momentum alcista)
- **SELL**: `precio < SMA × 0.95` (momentum bajista)

Incluye cooldown para evitar señales repetidas consecutivas del mismo tipo.

### `investment_dashboard_backend` — Cold-Path WebSocket `:8082`
Consume de `trading_signals` con Consumer Group `invest_dashboard_group` y retransmite señales por WebSocket al `investment.html`.

### `notification_router` — Kafka → RabbitMQ
Consume de `trading_signals` con Consumer Group `notification_router_group` y publica en dos destinos RabbitMQ: `email_queue` (Work Queue) y `console_alerts` (Fanout Exchange).

### `email_worker` (×3 réplicas) — Work Queue Consumer
Consumo de `email_queue` con `prefetch_count=1` y acknowledgment manual. Se ejecutan **3 instancias** que demuestran el patrón de **Work Queue con balanceo round-robin**: RabbitMQ distribuye los mensajes entre los 3 workers sin duplicarlos. Cada worker tiene un `WORKER_ID` para identificarse en los logs.

### `console_alert_worker` — Fanout Consumer
Consume de una cola anónima vinculada al Fanout Exchange `console_alerts`. Recibe **todos** los mensajes (broadcast).

---

## 📨 Formato de Mensajes

### Tick de Precio
```json
{
  "asset": "BTC",
  "price": 63000.45,
  "timestamp": "2025-05-09T14:30:00"
}
```

### Señal de Inversión (BUY)
```json
{
  "asset": "BTC",
  "signal": "BUY",
  "reason": "Momentum Crossover (Alcista)",
  "price": 63000.45,
  "sma": 61000.00,
  "timestamp": "2025-05-09T14:30:05"
}
```

### Señal de Inversión (SELL)
```json
{
  "asset": "BTC",
  "signal": "SELL",
  "reason": "Momentum Crossover (Bajista)",
  "price": 57000.00,
  "sma": 60000.00,
  "timestamp": "2025-05-09T14:31:10"
}
```

---

## 🔧 Exchanges y Colas de RabbitMQ

| Tipo | Nombre | Propósito |
|------|--------|-----------|
| Topic Exchange | `market_ticks` | Enrutamiento por patrón (`ticks.crypto.*`) |
| Work Queue (Direct) | `email_queue` | Distribución round-robin entre workers |
| Fanout Exchange | `console_alerts` | Broadcast a todos los sistemas de alerta |

## 📊 Topics de Kafka

| Topic | Key de Partición | Consumer Groups |
|-------|-----------------|-----------------|
| `historical_ticks` | Asset (`BTC`, `ETH`...) | `analytics_engine_group` |
| `trading_signals` | Asset | `invest_dashboard_group`, `notification_router_group` |

---

## 🛡️ Tolerancia a Fallos

- **Healthchecks (Heartbeat)**: RabbitMQ, Kafka y ambos backends WebSocket verifican conectividad periódicamente
- **`depends_on: service_healthy`**: Ningún microservicio arranca antes que la infraestructura
- **Independencia de rutas**: Si Kafka cae, el Hot-Path sigue funcionando
- **Consumer Groups Kafka**: Retoman desde el último offset al reiniciar
- **Work Queue + ACK manual**: Si un `email_worker` cae, RabbitMQ re-encola el mensaje para otro worker
- **Backoff exponencial**: Todos los servicios reconectan con espera 1s→2s→4s→8s... (máx 30s) en vez de `time.sleep` fijo
- **Graceful Shutdown**: Manejo de SIGTERM/SIGINT para cerrar conexiones limpiamente en `docker compose down`
- **Health endpoints**: `/health` en ambos backends WebSocket devuelve estado, clientes conectados y contadores

---

## ⚠️ Limitaciones Conocidas

| Limitación | Solución en Producción |
|-----------|----------------------|
| Estado en memoria del `analytics_engine` | Persistir en Redis o InfluxDB |
| Kafka replicación = 1 | Cluster con ISR ≥ 2 brokers |
| RabbitMQ nodo único | Clustering con mirrored queues |
| WebSockets sin autenticación | JWT + WSS (TLS) |
| Sin observabilidad | OpenTelemetry + Jaeger |
| ~~`time.sleep` para esperar infra~~ | ✅ Resuelto: backoff exponencial implementado |

---

## 📚 Conceptos de Sistemas Distribuidos Aplicados

- **Publish/Subscribe** — RabbitMQ (Topic Exchange) + Kafka (Consumer Groups)
- **Teorema CAP** — RabbitMQ = AP, Kafka = CP
- **Teorema PACELC** — Hot-Path = PA/EL, Cold-Path = PC/EC
- **Procesamiento Stateful** — `analytics_engine` con Map en memoria
- **Escalabilidad Horizontal** — 3 réplicas de `email_worker` con round-robin demostrado
- **Patrón Bridge** — `ingestion_bridge` desacopla RabbitMQ de Kafka
- **Contenedores Docker** — Cada servicio aislado en su propio contenedor
- **Docker Compose** — Orquestación en red interna `trading_net`
- **Particionamiento Kafka** — Key = asset garantiza orden por activo
- **Modo KRaft** — Sin ZooKeeper, menor complejidad operacional
- **Backoff Exponencial** — Reconexión inteligente en todos los servicios
- **Graceful Shutdown** — Cierre limpio de conexiones con SIGTERM
- **Observabilidad básica** — Latencia por tick, sparklines, contadores en dashboards

---

## 👥 División de Trabajo

| Persona | Responsabilidad |
|---------|----------------|
| **Persona 1 (Líder)** | `docker-compose.yml`, `market_data_feed`, `ingestion_bridge`, `analytics_engine` |
| **Persona 2** | `live_dashboard_backend`, `live_ticker.html`, `email_worker`, `console_alert_worker` |
| **Persona 3** | `investment_dashboard_backend`, `investment.html`, `notification_router` |

---

## 🛠️ Tecnologías

- **Python 3.11** — Todos los microservicios
- **RabbitMQ 3** (con Management Plugin) — Hot-Path messaging
- **Apache Kafka 3.7** (KRaft mode) — Cold-Path event streaming
- **WebSockets** — Comunicación en tiempo real con dashboards
- **Docker & Docker Compose** — Contenerización y orquestación
- **pika** — Cliente RabbitMQ para Python
- **kafka-python-ng** — Cliente Kafka para Python
