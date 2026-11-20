# Guía de la demostración en vivo

Esta guía es para quien presente la demostración sin haber armado el proyecto. No hay que escribir comandos: todo se hace con doble clic en archivos de la carpeta raíz del repositorio.

La demostración dura unos **5 minutos** y muestra los dos frentes que pide el docente:

1. **Datos validados poblando el almacén + informe estático.**
2. **Inferencia con Kafka + dashboard en tiempo real.**

## 0. Una sola vez, en el computador de la sustentación

1. Instalar **Docker Desktop** (con WSL2) y **Git**. En Docker Desktop, *Settings → Resources*, dar al menos **8 GB de RAM**.
2. Clonar el repositorio y entrar a la carpeta:
   ```bash
   git clone https://github.com/yurani-duque/etl-airflow-calidad-aire.git
   cd etl-airflow-calidad-aire
   ```
3. Ensayar el recorrido completo (secciones 2 a 6) con cronómetro, **al menos una vez en ese mismo computador**.

> El primer arranque descarga imágenes y tarda varios minutos. Hacerlo con internet y días antes, no el día de la sustentación.

## 1. Antes de entrar al salón (30 minutos antes)

| Revisar | Cómo se ve cuando está bien |
|---|---|
| Docker Desktop abierto | Ballena en la barra de tareas y la palabra **Engine running** |
| Internet | Abre cualquier página |
| Puertos libres | 8080, 8501, 8502, 9094 y 5433 sin otros programas |
| Diapositivas | Presentación abierta en inglés y la diapositiva 13 (capturas de respaldo) lista |

## 2. Paso 1: preparar todo (doble clic en `1_PREPARAR_DEMO.cmd`)

- Se abre una ventana negra. **No la cierres.**
- Hace cinco cosas solo: comprueba Docker, crea la configuración, levanta los servicios, espera a Airflow y al dashboard, y ejecuta el flujo completo una primera vez.
- **Tarda varios minutos** (10 o más si debe descargar las imágenes de Docker por primera vez). Con todo ya descargado, bastan unos pocos minutos.
- Termina con `Todo listo` y abre dos pestañas del navegador: **Airflow** y el **dashboard en tiempo real**.
- Si dice que Docker está apagado: ábrelo, espera `Engine running` y vuelve a dar doble clic.

Esto se hace **antes** de la sustentación. Durante la exposición solo se muestra.

## 3. Frente 1: datos validados y informe (2 min 30 s)

### 3.1 Airflow (pestaña `localhost:8080`, sin usuario ni contraseña)
1. Estás en el DAG `etl_calidad_aire`. Haz clic en **Latest Run** (la fecha en azul, con el visto verde).
2. Haz clic en el **ícono del grafo** (tercer ícono, arriba a la izquierda del panel).
3. Señala, de izquierda a derecha:
   - Las **4 fuentes en paralelo** (extraer y transformar).
   - `merge_sources` y **`validate_quality`** (Great Expectations).
   - Dos caminos: **`load_dw`** (carga el almacén) y **`quarantine_data`** (en rosa = *omitida*, porque la calidad fue buena).
   - **`publish_to_kafka`** en verde: ahí empieza el frente 2.
4. Qué decir: *«Si falla una regla crítica, el lote completo va a cuarentena y el almacén no se toca.»*

### 3.2 Ejecutarlo en vivo (opcional, dura ~1 minuto)
1. Botón **Trigger** (arriba a la derecha).
2. Deja el interruptor `prueba_falla_calidad` **apagado**.
3. Botón **Trigger** del cuadro. Cada tarea que se pone verde es un paso terminado.

### 3.3 El almacén (opcional)
Doble clic en `EXTRA_Estado.cmd`: muestra las filas de la tabla de hechos y las predicciones guardadas.

### 3.4 Informe estático (doble clic en `3_ABRIR_INFORME.cmd`)
Se abre `analytical_report.html`. Recorre despacio: **1.** ranking de ciudades, **3.** capital frente a su país, **5.** modelo frente a la línea base, **6.** calidad de datos y flujo en vivo.

## 4. Frente 2: inferencia y dashboard en vivo (2 min 30 s)

1. Ve a la pestaña **`localhost:8502`**. Ya muestra predicciones de la corrida anterior.
2. Con esa pantalla a la vista, doble clic en **`2_PUBLICAR_EVENTOS.cmd`** (publica ~780 eventos en 20 segundos).
3. Mira cómo suben **Predictions made**, **Messages per second** y **Cities with alert**.
4. Qué decir: *«Este dashboard lee del consumidor de Kafka, no consulta la base de datos. Cada evento se convierte en una predicción y una alerta en milisegundos.»*

> Si dice `Waiting for events`, publica de nuevo con el mismo archivo y espera unos segundos.

## 5. Si sobra tiempo: la cuarentena

En Airflow: **Trigger**, **enciende** el interruptor `prueba_falla_calidad` y **Trigger**. Se inyectan datos inválidos: `validate_quality` los detecta, `quarantine_data` queda en verde, y `load_dw` y `publish_to_kafka` quedan **omitidos** (rosa). La corrida figura como éxito porque la cuarentena es una ruta válida. Dura cerca de 1 minuto; si el tiempo aprieta, usa la diapositiva 7.

## 6. Al terminar

Doble clic en **`4_APAGAR.cmd`**. Los datos se conservan; se vuelve a levantar con `1_PREPARAR_DEMO.cmd`.

## Si algo falla

| Síntoma | Qué hacer |
|---|---|
| Docker apagado | Abrir Docker Desktop, esperar `Engine running` y repetir el archivo |
| El DAG sale en rojo en `extract_api` | Es el **límite de uso de la API externa** (error 429). Esperar 10 minutos y doble clic en `EXTRA_Reintentar_DAG.cmd`. Una corrida roja antigua en el historial es normal; importa la última en verde |
| El dashboard dice `Waiting for events` | Doble clic en `2_PUBLICAR_EVENTOS.cmd` y esperar unos segundos |
| `port is already allocated` | `4_APAGAR.cmd` y luego `1_PREPARAR_DEMO.cmd` |
| El informe dice 0 predicciones | Abrir antes el dashboard (puerto 8502) para que el consumidor procese los eventos, y volver a dar doble clic en `3_ABRIR_INFORME.cmd` |
| Nada funciona | Diapositiva 13 (capturas de respaldo) y contar lo que se vería |

## Resumen de archivos

| Archivo | Para qué sirve |
|---|---|
| `1_PREPARAR_DEMO.cmd` | Levanta todo y deja el DAG ejecutado |
| `2_PUBLICAR_EVENTOS.cmd` | Publica eventos en Kafka mientras se ve el dashboard |
| `3_ABRIR_INFORME.cmd` | Genera y abre el informe estático |
| `4_APAGAR.cmd` | Apaga los servicios al terminar |
| `EXTRA_Estado.cmd` | Muestra servicios, filas del almacén y predicciones |
| `EXTRA_Cuarentena.cmd` | Lanza una corrida con datos inválidos |
| `EXTRA_Reintentar_DAG.cmd` | Lanza el DAG de nuevo si el primero falló |
