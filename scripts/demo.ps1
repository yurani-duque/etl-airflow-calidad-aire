param(
    [ValidateSet("preparar", "ejecutar", "publicar", "informe", "cuarentena", "estado", "apagar")]
    [string]$Accion = "preparar"
)

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz

$dag = "etl_calidad_aire"
$airflow = "http://localhost:8080"
$realtime = "http://localhost:8502"

function Titulo($texto) {
    Write-Host ""
    Write-Host "=== $texto ===" -ForegroundColor Cyan
}

function Ok($texto) { Write-Host "  [OK] $texto" -ForegroundColor Green }
function Aviso($texto) { Write-Host "  [!]  $texto" -ForegroundColor Yellow }

function Pausa($texto) {
    if ($env:DEMO_SIN_PAUSA) { return }
    Read-Host $texto
}

function Abrir($destino) {
    if ($env:DEMO_SIN_PAUSA) { return }
    Start-Process $destino
}

function Fallar($texto) {
    Write-Host ""
    Write-Host "  [X]  $texto" -ForegroundColor Red
    Write-Host ""
    Pausa "Presiona Enter para cerrar"
    exit 1
}

function Probar-Docker {
    $null = & docker version 2>&1
    if ($LASTEXITCODE -ne 0) {
        Fallar "Docker no esta instalado o no esta en el PATH. Instala Docker Desktop y vuelve a intentar."
    }
    $null = & docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        Fallar "Docker Desktop esta apagado. Abrelo, espera a que diga 'Engine running' y ejecuta este archivo otra vez."
    }
}

function Esperar-Url($url, $nombre, $segundos) {
    $limite = (Get-Date).AddSeconds($segundos)
    while ((Get-Date) -lt $limite) {
        try {
            $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
            if ($r.StatusCode -eq 200) { Ok "$nombre responde"; return }
        } catch { }
        Start-Sleep -Seconds 3
    }
    Fallar "$nombre no respondio en $segundos segundos. Revisa con: docker compose ps"
}

function Obtener-Token {
    $r = Invoke-RestMethod -Uri "$airflow/auth/token" -Method Get
    return $r.access_token
}

function Lanzar-Dag($conf) {
    $token = Obtener-Token
    $cabeceras = @{ Authorization = "Bearer $token" }
    $cuerpo = @{ logical_date = $null; conf = $conf } | ConvertTo-Json -Depth 5
    $r = Invoke-RestMethod -Uri "$airflow/api/v2/dags/$dag/dagRuns" -Method Post `
        -Headers $cabeceras -ContentType "application/json" -Body $cuerpo
    return $r.dag_run_id
}

function Estado-Corrida($runId) {
    $token = Obtener-Token
    $cabeceras = @{ Authorization = "Bearer $token" }
    $id = [uri]::EscapeDataString($runId)
    $r = Invoke-RestMethod -Uri "$airflow/api/v2/dags/$dag/dagRuns/$id" -Headers $cabeceras
    return $r.state
}

function Ultima-Corrida {
    $token = Obtener-Token
    $cabeceras = @{ Authorization = "Bearer $token" }
    $r = Invoke-RestMethod -Uri "$airflow/api/v2/dags/$dag/dagRuns?order_by=-start_date&limit=1" -Headers $cabeceras
    if ($r.dag_runs.Count -eq 0) { return $null }
    return $r.dag_runs[0]
}

function Esperar-Corrida($runId, $segundos) {
    $limite = (Get-Date).AddSeconds($segundos)
    $anterior = ""
    while ((Get-Date) -lt $limite) {
        $estado = Estado-Corrida $runId
        if ($estado -ne $anterior) { Write-Host "       estado de la corrida: $estado"; $anterior = $estado }
        if ($estado -eq "success") { return $true }
        if ($estado -eq "failed") { return $false }
        Start-Sleep -Seconds 5
    }
    return $false
}

function Contar($sql) {
    $r = & docker compose exec -T warehouse psql -U dwh -d dwh -t -A -c $sql 2>$null
    return ($r | Out-String).Trim()
}

function Resumen {
    Titulo "Estado del sistema"
    & docker compose ps --format "table {{.Service}}\t{{.Status}}"
    $hechos = Contar "select count(*) from dw.fact_air_quality"
    $pred = Contar "select count(*) from dw.predictions"
    Write-Host ""
    Write-Host "  Filas en la tabla de hechos : $hechos"
    Write-Host "  Predicciones guardadas      : $pred"
}

switch ($Accion) {

    "preparar" {
        Titulo "1/5 Comprobando Docker"
        Probar-Docker
        Ok "Docker Desktop esta encendido"

        Titulo "2/5 Preparando la configuracion"
        if (-not (Test-Path ".env")) {
            Copy-Item ".env.example" ".env"
            Ok "Se creo .env con la base de datos local"
        } else {
            Ok ".env ya existe"
        }

        Titulo "3/5 Levantando los servicios (la primera vez tarda varios minutos)"
        & docker compose up -d --build
        if ($LASTEXITCODE -ne 0) { Fallar "docker compose no pudo levantar los servicios. Revisa que los puertos 8080, 8501, 8502, 9094 y 5433 esten libres." }

        Titulo "4/5 Esperando a que Airflow y el dashboard esten listos"
        Esperar-Url "$airflow/api/v2/monitor/health" "Airflow" 600
        Esperar-Url "$realtime/_stcore/health" "Dashboard en tiempo real" 300

        Titulo "5/5 Esperando la primera corrida del DAG (2 a 4 minutos)"
        $corrida = $null
        $limite = (Get-Date).AddSeconds(240)
        while ((Get-Date) -lt $limite -and -not $corrida) {
            try { $corrida = Ultima-Corrida } catch { }
            if (-not $corrida) { Start-Sleep -Seconds 5 }
        }
        if (-not $corrida) {
            Aviso "Airflow aun no lanzo la corrida automatica; la lanzo yo."
            $id = Lanzar-Dag @{}
        } else {
            $id = $corrida.dag_run_id
        }
        $exito = Esperar-Corrida $id 900
        $intentos = 1
        while (-not $exito -and $intentos -lt 3) {
            Aviso "La corrida fallo (casi siempre es el limite de uso de la API externa). Espero 2 minutos y reintento."
            Start-Sleep -Seconds 120
            $id = Lanzar-Dag @{}
            $intentos++
            $exito = Esperar-Corrida $id 900
        }
        if ($exito) {
            Ok "El DAG termino en verde"
        } else {
            Fallar "El DAG no termino en success tras 3 intentos. Espera 10 minutos (limite de la API) y ejecuta 'EXTRA_Reintentar_DAG.cmd'."
        }

        Resumen
        Titulo "Todo listo"
        Write-Host "  Airflow ................ $airflow/dags/$dag"
        Write-Host "  Dashboard tiempo real .. $realtime"
        Write-Host "  Siguiente paso ......... 2_PUBLICAR_EVENTOS.cmd (cuando la pantalla en vivo este abierta)"
        Abrir "$airflow/dags/$dag"
        Abrir $realtime
        Write-Host ""
        Pausa "Presiona Enter para cerrar esta ventana"
    }

    "ejecutar" {
        Probar-Docker
        Titulo "Lanzando el DAG $dag"
        $id = Lanzar-Dag @{}
        Ok "Corrida creada: $id"
        if (Esperar-Corrida $id 900) { Ok "Termino en success" } else { Fallar "La corrida no termino en success" }
        Pausa "Presiona Enter para cerrar"
    }

    "cuarentena" {
        Probar-Docker
        Titulo "Lanzando el DAG con datos que fallan la calidad"
        $id = Lanzar-Dag @{ prueba_falla_calidad = $true }
        Ok "Corrida creada: $id"
        Write-Host "  Abre Airflow y mira como validate_quality manda el lote a quarantine_data."
        Abrir "$airflow/dags/$dag/runs/$([uri]::EscapeDataString($id))"
        Pausa "Presiona Enter para cerrar"
    }

    "publicar" {
        Probar-Docker
        Titulo "Publicando eventos en Kafka (mira el dashboard en tiempo real)"
        & docker compose run --rm trainer python -m src.kafka_producer --dias 3 --retraso 0.02
        if ($LASTEXITCODE -ne 0) { Fallar "No se pudieron publicar los eventos. Comprueba que 'docker compose ps' muestre kafka en healthy." }
        Ok "Eventos publicados"
        Pausa "Presiona Enter para cerrar"
    }

    "informe" {
        Probar-Docker
        Titulo "Generando el informe analitico estatico"
        & docker compose run --rm report
        if ($LASTEXITCODE -ne 0) { Fallar "No se pudo generar el informe." }
        Start-Process (Join-Path $raiz "reports\analytical_report.html")
        Ok "Informe abierto en el navegador"
        Pausa "Presiona Enter para cerrar"
    }

    "estado" {
        Probar-Docker
        Resumen
        Pausa "Presiona Enter para cerrar"
    }

    "apagar" {
        Probar-Docker
        Titulo "Apagando los servicios (los datos se conservan)"
        & docker compose down
        Ok "Servicios apagados"
        Pausa "Presiona Enter para cerrar"
    }
}
