# Persona 2 — Hot-Path y Notificaciones RabbitMQ

## Componentes implementados

- `live_dashboard_backend`
- `live_ticker.html`
- `email_worker`
- `console_alert_worker`

## Responsabilidad

Esta parte del proyecto implementa el Hot-Path con RabbitMQ y los workers de notificación.

## Servicios

### live_dashboard_backend

Consume ticks desde el Topic Exchange `market_ticks` usando la binding key `ticks.#`.

Expone el dashboard en:

```text
http://localhost:8083