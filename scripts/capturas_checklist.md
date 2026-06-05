# Checklist de capturas Grafana — Informe TPP
# Correr primero: .\scripts\gen_traffic.ps1
# URL: http://100.86.94.38:6969

Rango base: **Last 30 minutes** | Refresh: **5s** | Node: **All**

---

## [1] Overview general
**Dashboard:** HES — Overview
**Config:** Last 30 min | Node = All
**Debe mostrar:** 3 nodos (o 2 post-failover), 80 devices, sesiones activas, tasa de mensajes, 1 failover
**Archivo:** `grafana_01_overview.png`

---

## [2] Cluster Health (stat panels)
**Dashboard:** HES — Cluster | sección "Cluster Health"
**Config:** Last 30 min | Node = All
**Debe mostrar:** Total Nodes=3 (o 2), Active Nodes=1 post-failover, Failovers=1, Delegations>0
**Archivo:** `grafana_02_cluster_health.png`

---

## [3] Per-Node Load (series temporales)
**Dashboard:** HES — Cluster | sección "Per-Node Load"
**Config:** Last 30 min | Node = All
**Debe mostrar:** 2 líneas (node-2, node-3), node-3 cae a mitad del gráfico y node-2 sube
**Tip:** Este es el panel más llamativo — capturarlo cuando se vea claramente el salto
**Archivo:** `grafana_03_cluster_pernode.png`

---

## [4] Heartbeat Rate
**Dashboard:** HES — Cluster | sección "Heartbeats"
**Config:** Last 30 min
**Debe mostrar:** curvas sent/received con actividad, timeout spike al caer node-3
**Archivo:** `grafana_04_cluster_heartbeats.png`

---

## [5] State Changes
**Dashboard:** HES — Cluster | panel "Node State Changes"
**Config:** Last 30 min
**Debe mostrar:** transición active→suspect y suspect→dead del failover
**Archivo:** `grafana_05_cluster_state_changes.png`

---

## [6] Registration Funnel
**Dashboard:** HES — Devices | sección "Registration"
**Config:** Last 30 min | Node = All
**Debe mostrar:** 3 picos de registración (oleadas 1, 2, 3) + duration p50/p95/p99
**Archivo:** `grafana_06_devices_registration.png`

---

## [7] Session Outcomes
**Dashboard:** HES — Devices | sección "Sessions"
**Config:** Last 30 min
**Debe mostrar:** sesiones exitosas acumulándose, session_active >0 durante cada ciclo
**Archivo:** `grafana_07_devices_sessions.png`

---

## [8] Devices per Bucket
**Dashboard:** HES — Scheduler | panel "Devices per Bucket"
**Config:** Last 30 min
**Debe mostrar:** ~80 devices distribuidos en múltiples buckets (barras relativamente uniformes)
**Archivo:** `grafana_08_scheduler_buckets.png`

---

## [9] Session Success Rate
**Dashboard:** HES — Scheduler | panel "Session Success Rate"
**Config:** Last 30 min
**Debe mostrar:** porcentaje ~100% (baja levemente durante failover)
**Archivo:** `grafana_09_scheduler_success.png`

---

## [10] Message Rate by Type
**Dashboard:** HES — Protocol | panel "Message Rate by Type"
**Config:** Last 30 min
**Debe mostrar:** curvas paralelas de HANDSHAKE/READ/WRITE/ACK
**Archivo:** `grafana_10_protocol_messages.png`

---

## [11] Decode Errors
**Dashboard:** HES — Protocol | panel "Decode Errors" (o HES — Reliability)
**Config:** Last 30 min
**Debe mostrar:** ~15 errores de decode (paquetes malformados del inicio)
**Archivo:** `grafana_11_protocol_errors.png`

---

## [12] Reliability — Error breakdown
**Dashboard:** HES — Reliability
**Config:** Last 30 min | Node = All
**Debe mostrar:** errores por componente, decode errors visible
**Archivo:** `grafana_12_reliability.png`

---

## [13] Performance — Latencias
**Dashboard:** HES — Performance
**Config:** Last 30 min
**Debe mostrar:** p50/p95/p99 de registration_duration_ms y RPC duration
**Archivo:** `grafana_13_performance.png`

---

## [14] RPC Requests
**Dashboard:** HES — Performance | sección RPC (o buscar panel hes_rpc_request_total)
**Config:** Last 30 min
**Debe mostrar:** ~45 llamadas RPC distribuidas por método (device.list, config.list, etc.)
**Archivo:** `grafana_14_rpc.png`

---

## Carpeta destino
`Informe-Trabajo-Practico-Profesional/_Imagenes/S10/grafana/`

## Tips generales
- Usar Full Screen (F) en cada panel para captura limpia
- Si un panel muestra "No data" inesperado: refrescar y esperar 15s
- Para el panel de failover (Nro 3): capturar cuando se vea el salto visual
- Tiempo total del script: ~28 minutos
