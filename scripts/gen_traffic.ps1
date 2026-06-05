# gen_traffic.ps1 — Entorno de prueba representativo para capturas del informe
#
# Genera actividad en 3 oleadas, un failover planificado de node-3, paquetes
# malformados para poblar errores de decode, y llamadas RPC via hes-cli.
#
# Uso:
#   .\scripts\gen_traffic.ps1
#   .\scripts\gen_traffic.ps1 -WaitAfterLastWave 10

param(
    [int]$WaitAfterLastWave = 10  # minutos extra después de la oleada 3
)

Set-StrictMode -Off
$ErrorActionPreference = "Continue"

$root    = Resolve-Path "$PSScriptRoot\.."
$exe     = "$root\target\debug\hes.exe"
$mockExe = "$root\target\debug\mock_device.exe"
$malfExe = "$root\target\debug\malformed_registry.exe"
$cliExe  = "$root\target\debug\hes-cli.exe"
$logDir  = "$root\logs_traffic"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

# ─── helpers ────────────────────────────────────────────────────────────────

function Write-Step($n, $msg) {
    Write-Host ""
    Write-Host "[$n] $msg" -ForegroundColor Cyan
}

function Write-Info($msg) {
    Write-Host "    $msg" -ForegroundColor Gray
}

function Wait-WithStatus($seconds, $label) {
    $end = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $end) {
        $rem = [int]($end - (Get-Date)).TotalSeconds
        $sessions = (Get-ChildItem "$logDir\mock_*.log" -EA SilentlyContinue |
                     Select-String "Session.*done" -EA SilentlyContinue).Count
        Write-Host -NoNewline "`r    $label — faltan ${rem}s | sesiones completadas: $sessions   "
        Start-Sleep -Seconds 3
    }
    Write-Host ""
}

function Start-Mock($port, $imei, $backdoorPort, $battery, $liters, $tag) {
    $p = Start-Process -FilePath $mockExe `
        -ArgumentList '--backdoor-ip','127.0.0.1',
                      '--backdoor-port',$backdoorPort,
                      '--device-port',$port,
                      '--imei',$imei,
                      '--battery',$battery,
                      '--liters-per-session',$liters `
        -WorkingDirectory $root `
        -WindowStyle Hidden `
        -RedirectStandardOutput "$logDir\mock_${tag}_${port}.log" `
        -RedirectStandardError  "$logDir\mock_${tag}_${port}.err" `
        -PassThru
    return $p
}

function Send-RpcBatch($host, $port, $commands) {
    # Envía una lista de comandos al hes-cli via stdin piped y espera EOF
    $input = ($commands -join "`n") + "`nexit`n"
    try {
        $input | & $cliExe --host $host --port $port 2>$null | Out-Null
    } catch { }
}

# ─── 0. build check ──────────────────────────────────────────────────────────
Write-Step "0" "Verificando binarios"
foreach ($bin in @($exe, $mockExe, $malfExe, $cliExe)) {
    if (-not (Test-Path $bin)) {
        Write-Host "    FALTA: $bin — corré 'cargo build --all' primero" -ForegroundColor Red
        exit 1
    }
}
Write-Info "Todos los binarios presentes."

# ─── 1. iniciar nodos ────────────────────────────────────────────────────────
Write-Step "1" "Iniciando node-2 y node-3 en test-mode"

$node2 = Start-Process -FilePath $exe `
    -ArgumentList '--config',"configs\node-2.yaml",'--test-mode' `
    -WorkingDirectory $root -WindowStyle Hidden `
    -RedirectStandardOutput "$logDir\node2.log" `
    -RedirectStandardError  "$logDir\node2.err" -PassThru

$node3 = Start-Process -FilePath $exe `
    -ArgumentList '--config',"configs\node-3.yaml",'--test-mode' `
    -WorkingDirectory $root -WindowStyle Hidden `
    -RedirectStandardOutput "$logDir\node3.log" `
    -RedirectStandardError  "$logDir\node3.err" -PassThru

Write-Info "node-2 PID $($node2.Id) | node-3 PID $($node3.Id)"
Write-Info "Esperando arranque (8s)..."
Start-Sleep -Seconds 8

# ─── 2. oleada 1: 30 devices (variedad de batería y consumo) ─────────────────
Write-Step "2" "Oleada 1 — 30 mock_devices (15 × node-2, 15 × node-3)"

# Grupos de parámetros: (battery, liters)  — baja/media/alta
$groups = @(
    @(15,  50),   # 0-4:  batería baja, consumo bajo
    @(60, 150),   # 5-9:  batería media, consumo medio
    @(88, 300)    # 10-14: batería alta, consumo alto
)

$allMocks = @()
$basePort = 7000
$waveTag  = "w1"

for ($i = 0; $i -lt 15; $i++) {
    $g = $groups[[Math]::Floor($i / 5)]
    $p = $basePort + $i
    $allMocks += Start-Mock $p ("3510000{0:D8}" -f $i) 6565 $g[0] $g[1] "${waveTag}_n2"
    Start-Sleep -Milliseconds 200
}
for ($i = 0; $i -lt 15; $i++) {
    $g = $groups[[Math]::Floor($i / 5)]
    $p = $basePort + 15 + $i
    $allMocks += Start-Mock $p ("3520000{0:D8}" -f $i) 6566 $g[0] $g[1] "${waveTag}_n3"
    Start-Sleep -Milliseconds 200
}

Write-Info "30 devices registrados en oleada 1."

# ─── 3. paquetes malformados → decode errors ─────────────────────────────────
Write-Step "3" "Enviando 15 paquetes malformados al backdoor de node-2"

for ($i = 0; $i -lt 15; $i++) {
    Start-Process -FilePath $malfExe -WorkingDirectory $root `
        -WindowStyle Hidden `
        -RedirectStandardOutput "$logDir\malf_$i.log" `
        -RedirectStandardError  "$logDir\malf_$i.err" | Out-Null
    Start-Sleep -Milliseconds 300
}
Write-Info "Paquetes malformados enviados — hes_message_decode_errors_total debe subir."

# ─── 4. esperar primera sesión + oleada 2 ────────────────────────────────────
Write-Step "4" "Esperando primera tanda de sesiones (5 min)"
Wait-WithStatus 310 "Primera tanda"

Write-Step "4b" "Oleada 2 — 30 mock_devices adicionales"
$waveTag = "w2"
for ($i = 0; $i -lt 15; $i++) {
    $g = $groups[[Math]::Floor($i / 5)]
    $p = $basePort + 30 + $i
    $allMocks += Start-Mock $p ("3530000{0:D8}" -f $i) 6565 $g[0] $g[1] "${waveTag}_n2"
    Start-Sleep -Milliseconds 200
}
for ($i = 0; $i -lt 15; $i++) {
    $g = $groups[[Math]::Floor($i / 5)]
    $p = $basePort + 45 + $i
    $allMocks += Start-Mock $p ("3540000{0:D8}" -f $i) 6566 $g[0] $g[1] "${waveTag}_n3"
    Start-Sleep -Milliseconds 200
}
Write-Info "60 devices en total (oleadas 1+2)."

# ─── 5. RPC calls → métricas de hes-cli ─────────────────────────────────────
Write-Step "5" "Generando tráfico RPC contra node-2 (puerto 6600)"

$rpcCommands = @(
    "version",
    "config list",
    "cluster peers",
    "cluster status",
    "device list",
    "device list --limit 20",
    "config get node_id",
    "config get buckets_number",
    "cluster status",
    "device list --limit 5 --offset 5",
    "version",
    "cluster peers",
    "device list",
    "config list",
    "cluster status"
)

# 3 pasadas → ~45 llamadas RPC
for ($round = 1; $round -le 3; $round++) {
    Write-Info "Ronda RPC $round/3..."
    Send-RpcBatch "127.0.0.1" 6600 $rpcCommands
    Start-Sleep -Seconds 2
}
Write-Info "~45 llamadas RPC enviadas — hes_rpc_request_total y duration_ms poblados."

# ─── 6. esperar segunda sesión + oleada 3 ────────────────────────────────────
Write-Step "6" "Esperando segunda tanda de sesiones (5 min)"
Wait-WithStatus 310 "Segunda tanda"

Write-Step "6b" "Oleada 3 — 20 mock_devices finales (solo node-2, antes del failover)"
$waveTag = "w3"
for ($i = 0; $i -lt 20; $i++) {
    $g = $groups[[Math]::Floor($i / 7)]
    $p = $basePort + 60 + $i
    $allMocks += Start-Mock $p ("3550000{0:D8}" -f $i) 6565 $g[0] $g[1] "${waveTag}_n2"
    Start-Sleep -Milliseconds 200
}
Write-Info "80 devices en total (oleadas 1+2+3)."

# ─── 7. failover planificado de node-3 ───────────────────────────────────────
Write-Step "7" "FAILOVER — deteniendo node-3 (PID $($node3.Id))"
Write-Info "Esto genera state_changes, failover, redistribución de devices..."

Stop-Process -Id $node3.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# Unos RPC más post-failover para ver el estado actualizado
Write-Info "RPC post-failover para reflejar nueva distribución..."
Send-RpcBatch "127.0.0.1" 6600 @("cluster status","cluster peers","device list","cluster status")

# ─── 8. esperar ciclos finales ───────────────────────────────────────────────
Write-Step "8" "Esperando $WaitAfterLastWave minutos de sesiones post-failover"
Wait-WithStatus ($WaitAfterLastWave * 60) "Sesiones post-failover"

# ─── 9. resumen ──────────────────────────────────────────────────────────────
$totalSessions = (Get-ChildItem "$logDir\mock_*.log" -EA SilentlyContinue |
                  Select-String "Session.*done" -EA SilentlyContinue).Count

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host " LISTO — Ya podés tomar las capturas en Grafana :6969  " -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  Sesiones completadas : $totalSessions"
Write-Host "  Devices totales      : 80 (30 + 30 + 20)"
Write-Host "  node-2 PID           : $($node2.Id)  (CORRIENDO)"
Write-Host "  node-3               : detenido (failover ejecutado)"
Write-Host ""
Write-Host "  Métricas que deben tener datos ahora:" -ForegroundColor Yellow
Write-Host "    scheduled_devices_total        — 80"
Write-Host "    connections_tracker            — attempt/success acumulados"
Write-Host "    hes_message_decode_errors_total — ~15 (paquetes malformados)"
Write-Host "    hes_cluster_state_changes_total — active→suspect, suspect→dead"
Write-Host "    hes_cluster_failovers_total     — 1"
Write-Host "    hes_rpc_request_total           — ~45 llamadas"
Write-Host "    messages_total por tipo         — HANDSHAKE/READ/WRITE/ACK"
Write-Host "    hes_registration_total          — 80 success"
Write-Host "    hes_cluster_nodes_active        — 1 (solo node-2 queda)"
Write-Host ""
Write-Host "  Rango recomendado en Grafana: Last 30 min" -ForegroundColor Yellow
Write-Host "  Checklist de capturas: scripts\capturas_checklist.md"
Write-Host ""
Write-Host "  Para frenar todo cuando termines:"
Write-Host "    Stop-Process -Id $($node2.Id) -Force"
Write-Host "    Stop-Process -Name mock_device -Force"
Write-Host ""
Write-Host "  Logs en: $logDir"
