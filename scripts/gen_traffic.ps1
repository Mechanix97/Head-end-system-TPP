# gen_traffic.ps1 - Entorno de prueba representativo para capturas del informe
#
# Genera actividad en 3 oleadas, un failover planificado de node-3, paquetes
# malformados para poblar errores de decode, y llamadas JSON-RPC (TCP directo).
#
# Requiere binarios compilados: cargo build --all
# El nodo node-1 (server, docker) debe estar corriendo y clusterizado aparte.
#
# NOTA: archivo en ASCII puro a proposito (Windows PowerShell 5.1 lee los
# scripts como ANSI; cualquier caracter no-ASCII rompe el parseo).
#
# Uso:
#   .\scripts\gen_traffic.ps1
#   .\scripts\gen_traffic.ps1 -WaitAfterLastWave 10

param(
    [int]$WaitAfterLastWave = 10  # minutos extra despues del failover
)

Set-StrictMode -Off
$ErrorActionPreference = "Continue"

$root    = Resolve-Path "$PSScriptRoot\.."
$exe     = "$root\target\debug\hes.exe"
$mockExe = "$root\target\debug\mock_device.exe"
$malfExe = "$root\target\debug\malformed_registry.exe"
$logDir  = "$root\logs_traffic"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

# --- helpers ----------------------------------------------------------------

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
        Write-Host -NoNewline "`r    $label - faltan ${rem}s | sesiones completadas: $sessions   "
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

function Invoke-Rpc($rpcHost, $rpcPort, $requests) {
    # Cliente JSON-RPC 2.0 directo por TCP (newline-delimited).
    # Mas robusto que pipear al hes-cli, que depende de rustyline + stdin TTY.
    # $requests es un array de hashtables: @{ method='...'; params=@{...} }
    $sent = 0
    try {
        $client = New-Object System.Net.Sockets.TcpClient
        $client.Connect($rpcHost, [int]$rpcPort)
        $stream = $client.GetStream()
        $stream.ReadTimeout  = 3000   # ms - evita colgarse si el nodo no responde
        $stream.WriteTimeout = 3000
        $writer = New-Object System.IO.StreamWriter($stream)
        $writer.AutoFlush = $true
        $reader = New-Object System.IO.StreamReader($stream)

        $id = 1
        foreach ($r in $requests) {
            $payload = @{
                jsonrpc = "2.0"
                method  = $r.method
                params  = $r.params
                id      = $id
            } | ConvertTo-Json -Compress -Depth 6
            $writer.WriteLine($payload)
            try { $null = $reader.ReadLine() } catch { }  # consumir respuesta
            $sent++
            $id++
        }
        $writer.Close(); $reader.Close(); $client.Close()
    } catch {
        Write-Info "RPC a ${rpcHost}:${rpcPort} fallo: $($_.Exception.Message)"
    }
    return $sent
}

# --- 0. build check ---------------------------------------------------------
Write-Step "0" "Verificando binarios"
foreach ($bin in @($exe, $mockExe, $malfExe)) {
    if (-not (Test-Path $bin)) {
        Write-Host "    FALTA: $bin - corre 'cargo build --all' primero" -ForegroundColor Red
        exit 1
    }
}
Write-Info "Todos los binarios presentes."

# --- 1. iniciar nodos -------------------------------------------------------
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

# --- 2. oleada 1: 30 devices (parametros variados para realismo) ------------
Write-Step "2" "Oleada 1 - 30 mock_devices (15 x node-2, 15 x node-3)"

# Grupos (battery, liters): bajo/medio/alto. Da realismo a los datos de
# consumo en logs/device.info; NO altera message_size (payload de tam. fijo).
$groups = @(
    @(15,  50),
    @(60, 150),
    @(88, 300)
)

$basePort = 7000
$waveTag  = "w1"

for ($i = 0; $i -lt 15; $i++) {
    $g = $groups[[Math]::Floor($i / 5)]
    $p = $basePort + $i
    $null = Start-Mock $p ("3510000{0:D8}" -f $i) 6565 $g[0] $g[1] "${waveTag}_n2"
    Start-Sleep -Milliseconds 200
}
for ($i = 0; $i -lt 15; $i++) {
    $g = $groups[[Math]::Floor($i / 5)]
    $p = $basePort + 15 + $i
    $null = Start-Mock $p ("3520000{0:D8}" -f $i) 6566 $g[0] $g[1] "${waveTag}_n3"
    Start-Sleep -Milliseconds 200
}

Write-Info "30 devices registrados en oleada 1."

# --- 3. paquetes malformados -> decode errors -------------------------------
Write-Step "3" "Enviando 15 paquetes malformados al backdoor de node-2"

for ($i = 0; $i -lt 15; $i++) {
    Start-Process -FilePath $malfExe -WorkingDirectory $root `
        -WindowStyle Hidden `
        -RedirectStandardOutput "$logDir\malf_$i.log" `
        -RedirectStandardError  "$logDir\malf_$i.err" | Out-Null
    Start-Sleep -Milliseconds 300
}
Write-Info "Paquetes malformados enviados - hes_message_decode_errors_total debe subir."

# --- 4. esperar primera sesion + oleada 2 -----------------------------------
Write-Step "4" "Esperando primera tanda de sesiones (5 min)"
Wait-WithStatus 310 "Primera tanda"

Write-Step "4b" "Oleada 2 - 30 mock_devices adicionales"
$waveTag = "w2"
for ($i = 0; $i -lt 15; $i++) {
    $g = $groups[[Math]::Floor($i / 5)]
    $p = $basePort + 30 + $i
    $null = Start-Mock $p ("3530000{0:D8}" -f $i) 6565 $g[0] $g[1] "${waveTag}_n2"
    Start-Sleep -Milliseconds 200
}
for ($i = 0; $i -lt 15; $i++) {
    $g = $groups[[Math]::Floor($i / 5)]
    $p = $basePort + 45 + $i
    $null = Start-Mock $p ("3540000{0:D8}" -f $i) 6566 $g[0] $g[1] "${waveTag}_n3"
    Start-Sleep -Milliseconds 200
}
Write-Info "60 devices en total (oleadas 1+2)."

# --- 5. RPC calls -> metricas RPC (cliente TCP directo) ---------------------
Write-Step "5" "Generando trafico RPC contra node-2 (puerto 6600)"

$rpcRequests = @(
    @{ method = "system.version";  params = @{} },
    @{ method = "config.list";     params = @{} },
    @{ method = "cluster.peers";   params = @{} },
    @{ method = "cluster.status";  params = @{} },
    @{ method = "device.list";     params = @{} },
    @{ method = "device.list";     params = @{ limit = 20 } },
    @{ method = "config.get";      params = @{ key = "node_id" } },
    @{ method = "config.get";      params = @{ key = "buckets_number" } },
    @{ method = "cluster.status";  params = @{} },
    @{ method = "device.list";     params = @{ limit = 5; offset = 5 } },
    @{ method = "system.version";  params = @{} },
    @{ method = "cluster.peers";   params = @{} },
    @{ method = "device.list";     params = @{} },
    @{ method = "config.list";     params = @{} },
    @{ method = "cluster.status";  params = @{} }
)

# 3 pasadas -> ~45 llamadas RPC
$rpcTotal = 0
for ($round = 1; $round -le 3; $round++) {
    Write-Info "Ronda RPC $round/3..."
    $rpcTotal += Invoke-Rpc "127.0.0.1" 6600 $rpcRequests
    Start-Sleep -Seconds 2
}
Write-Info "$rpcTotal llamadas RPC enviadas - hes_rpc_request_total y duration_ms poblados."

# --- 6. esperar segunda sesion + oleada 3 -----------------------------------
Write-Step "6" "Esperando segunda tanda de sesiones (5 min)"
Wait-WithStatus 310 "Segunda tanda"

Write-Step "6b" "Oleada 3 - 20 mock_devices finales (solo node-2, antes del failover)"
$waveTag = "w3"
for ($i = 0; $i -lt 20; $i++) {
    $g = $groups[[Math]::Floor($i / 7)]
    $p = $basePort + 60 + $i
    $null = Start-Mock $p ("3550000{0:D8}" -f $i) 6565 $g[0] $g[1] "${waveTag}_n2"
    Start-Sleep -Milliseconds 200
}
Write-Info "80 devices en total (oleadas 1+2+3)."

# --- 7. failover planificado de node-3 --------------------------------------
Write-Step "7" "FAILOVER - deteniendo node-3 (PID $($node3.Id))"
Write-Info "Genera state_changes, failover y redistribucion de devices."
Write-Info "Con suspect_timeout=180s + dead_timeout=60s tarda hasta ~4 min."

Stop-Process -Id $node3.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# --- 8. esperar deteccion del failover + ciclos finales ---------------------
Write-Step "8" "Esperando $WaitAfterLastWave min (deteccion de failover + sesiones)"
Wait-WithStatus ($WaitAfterLastWave * 60) "Post-failover"

# RPC post-failover para reflejar la nueva distribucion ya consolidada
Write-Info "RPC post-failover para reflejar nueva distribucion..."
$null = Invoke-Rpc "127.0.0.1" 6600 @(
    @{ method = "cluster.status"; params = @{} },
    @{ method = "cluster.peers";  params = @{} },
    @{ method = "device.list";    params = @{} },
    @{ method = "cluster.status"; params = @{} }
)

# --- 9. resumen -------------------------------------------------------------
$totalSessions = (Get-ChildItem "$logDir\mock_*.log" -EA SilentlyContinue |
                  Select-String "Session.*done" -EA SilentlyContinue).Count

Write-Host ""
Write-Host "=======================================================" -ForegroundColor Green
Write-Host " LISTO - Ya podes tomar las capturas en Grafana :6969  " -ForegroundColor Green
Write-Host "=======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Sesiones completadas : $totalSessions"
Write-Host "  Devices totales      : 80 (30 + 30 + 20)"
Write-Host "  node-2 PID           : $($node2.Id)  (CORRIENDO)"
Write-Host "  node-3               : detenido (failover ejecutado)"
Write-Host ""
Write-Host "  Metricas que deben tener datos ahora:" -ForegroundColor Yellow
Write-Host "    scheduled_devices_total         - 80"
Write-Host "    connections_tracker             - attempt/success acumulados"
Write-Host "    hes_message_decode_errors_total - ~15 (paquetes malformados)"
Write-Host "    hes_cluster_state_changes_total - active->suspect, suspect->dead"
Write-Host "    hes_cluster_failovers_total     - 1"
Write-Host "    hes_rpc_request_total           - ~45 llamadas por metodo"
Write-Host "    messages_total por tipo         - HANDSHAKE/READ/WRITE/ACK"
Write-Host "    hes_registration_total          - 80 success"
Write-Host "    hes_cluster_nodes_active        - refleja nodos vivos del cluster"
Write-Host ""
Write-Host "  NOTA message_size_bytes: el protocolo usa payloads de tamano fijo;" -ForegroundColor DarkYellow
Write-Host "  el histograma tendra un unico valor por tipo (es el comportamiento real)." -ForegroundColor DarkYellow
Write-Host "  NOTA heartbeats: el intervalo es 60s; usar rate[5m] en esos paneles" -ForegroundColor DarkYellow
Write-Host "  para que las curvas no se vean planas." -ForegroundColor DarkYellow
Write-Host ""
Write-Host "  Rango recomendado en Grafana: Last 30 min" -ForegroundColor Yellow
Write-Host "  Checklist de capturas: scripts\capturas_checklist.md"
Write-Host ""
Write-Host "  Para frenar todo cuando termines:"
Write-Host "    Stop-Process -Id $($node2.Id) -Force"
Write-Host "    Stop-Process -Name mock_device -Force"
Write-Host ""
Write-Host "  Logs en: $logDir"
