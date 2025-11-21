# Diseño conceptual de pipeline ETL — Cadena de tiendas minoristas

## Resumen

Documento README que describe de forma ordenada el diseño conceptual de un pipeline ETL para integrar datos de una cadena de tiendas minoristas. Incluye: fuentes de datos, esquema destino, transformaciones, estrategia de carga, verificación, monitoreo y ejemplos (SQL y pseudocódigo).

---

## 1. Objetivo

Integrar y consolidar datos de ventas, inventario, clientes y comportamiento web para obtener una vista única y confiable que permita análisis como ventas por tienda, rendimiento por producto, rotación de stock y segmentación de clientes.

---

## 2. Fuentes de datos

* **POS (Sistema de punto de venta)**: Base de datos SQL (con tablas: `ventas`, `detalle_ventas`, `tiendas`, `productos`).
* **Inventario**: API REST que entrega stock por `producto` y `ubicacion` (p. ej. `/api/inventario`).
* **CRM**: Exportes semanales en CSV con información de clientes y su segmentación (`id_cliente`, `email`, `segmento`, `fecha_actualizacion`).
* **Sitio web**: Logs JSON de eventos (p. ej. `page_view`, `purchase`) con timestamps y `session_id`.

---

## 3. Esquema destino (modelo dimensional — tabla de hechos)

**Tabla de hechos: `ventas_consolidadas`**

```sql
CREATE TABLE ventas_consolidadas (
    id_venta INTEGER PRIMARY KEY,
    fecha_venta DATE,
    id_tienda INTEGER,
    id_cliente INTEGER,
    id_producto INTEGER,
    cantidad INTEGER,
    precio_unitario DECIMAL(10,2),
    total_venta DECIMAL(10,2),
    canal_venta VARCHAR(20), -- 'online' | 'tienda'
    segmento_cliente VARCHAR(20),
    created_at TIMESTAMP,
    fuente VARCHAR(20) -- 'pos' | 'web' | 'otros'
);
```

**Dimension tables sugeridas**

* `dim_tienda(id_tienda, nombre, region, tipo)`
* `dim_producto(id_producto, sku_unificado, nombre, categoria)`
* `dim_cliente(id_cliente, nombre, segmento, email)`
* `dim_fecha(fecha, año, mes, dia, trimestre)`

---

## 4. Transformaciones principales (ETL)

1. **Normalización de timestamps**

   * Convertir todos los timestamps a UTC o a la zona de la empresa.
   * Extraer `fecha_venta` (YYYY-MM-DD) para la dimensión fecha.

2. **Cálculo de totales**

   * `total_venta = cantidad * precio_unitario` si no viene ya calculado.

3. **Enriquecimiento con CRM**

   * Hacer un `LEFT JOIN` por `id_cliente` para añadir `segmento_cliente`.
   * Si `id_cliente` es nulo (compra anónima), marcar `segmento_cliente = 'anonimo'`.

4. **Normalizar códigos de producto**

   * Mapear SKUs/prod_ids de diferentes sistemas a `sku_unificado` en `dim_producto`.
   * Mantener una tabla de mapeo `producto_map(sistema, id_sistema, id_producto_unificado)`.

5. **Detección y manejo de duplicados**

   * Definir una clave de deduplicación (`id_venta` de origen, o hash de campos críticos: tienda+fecha+producto+cantidad+total).
   * Implementar deduplicación en carga (ej. `INSERT ... ON CONFLICT (id_venta) DO UPDATE` o mantener marca `is_duplicate`).

6. **Validaciones de calidad**

   * Valores nulos en campos críticos (`id_producto`, `cantidad`) → registrar fila en tabla de errores y omitir o encolar para revisión.
   * Rangos válidos (cantidad >= 0, precio_unitario >= 0).

---

## 5. Frecuencia y estrategia de carga

* **POS (ventas)**: incremental cada *hora*. Use watermarking por `created_at` / `updated_at` y CDC si está disponible.
* **Inventario**: carga completa diaria (noche), o incremental si la API lo permite.
* **CRM**: actualización semanal (CSV) — proceso de merge (upsert) sobre `dim_cliente`.
* **Web analytics**: batch diario (logs JSON) para transacciones offline; si el volumen es alto, considerar un pipeline streaming para eventos.

**Estrategia de idempotencia**

* Implementar cargas idempotentes (upsert, `merge`) para evitar duplicados si el job se reejecuta.

---

## 6. Orquestación y almacenamiento

* **Orquestador**: Airflow / Prefect / Dagster para coordinar DAGs: extracción → staging → transformaciones → carga.
* **Almacenamiento intermedio (staging)**: S3 / Azure Blob / GCS para batch; tablas staging en data warehouse para transformaciones SQL.
* **Data Warehouse**: Redshift / BigQuery / Snowflake / PostgreSQL según escala.

---

## 7. Monitoreo y logging

* **Logging estructurado**: registrar start/end, rows read, rows written, tiempo de ejecución, errores (stack traces) y métricas de calidad (porcentaje filas con errores).
* **Alertas**: notificar cuando: job falla, volumen de datos cae > 30% vs esperado, tasa de errores > 1%.
* **Dashboards**: métricas de ingestión en Grafana / Looker / Metabase.

---

## 8. Pseudocódigo del flujo ETL (por fuente)

**Ventas POS (incremental, cada hora)**

```pseudo
-- establecer watermark = última_fecha_cargada
nuevas_ventas = SELECT * FROM pos.ventas WHERE created_at > watermark
-- limpieza y transformaciones
for fila in nuevas_ventas:
    fecha = normalize_timestamp(fila.created_at)
    total = fila.cantidad * fila.precio_unitario
    segmento = lookup_segmento(fila.id_cliente) -- desde CRM staging
    sku_unificado = map_producto(fila.id_producto, 'pos')
    if not valida(fila):
        send_to_error_table(fila)
        continue
    upsert ventas_consolidadas using id_venta or dedup_hash
update watermark
```

**Inventario (diario)**

```pseudo
inventario_api = GET /api/inventario
for registro in inventario_api:
    sku_unificado = map_producto(registro.id_producto, 'inventario')
    upsert dim_inventario(sku_unificado, registro.ubicacion, registro.stock)
```

---

## 9. Ejemplos SQL útiles

* Upsert (Postgres 13+):

```sql
INSERT INTO ventas_consolidadas (id_venta, fecha_venta, id_tienda, id_cliente, id_producto, cantidad, precio_unitario, total_venta, canal_venta, segmento_cliente, created_at, fuente)
VALUES (...)
ON CONFLICT (id_venta) DO UPDATE SET
    cantidad = EXCLUDED.cantidad,
    precio_unitario = EXCLUDED.precio_unitario,
    total_venta = EXCLUDED.total_venta,
    segmento_cliente = EXCLUDED.segmento_cliente,
    created_at = EXCLUDED.created_at;
```

* Detección duplicados por hash:

```sql
ALTER TABLE ventas_consolidadas ADD COLUMN dedup_hash TEXT;
-- calcular hash en staging y usarlo para dedup
```

---

## 10. Verificación — cómo este diseño resuelve la fragmentación

Problemas iniciales: datos fragmentados entre POS, inventario, CRM y web, con distintos identificadores y formatos.

Soluciones implementadas:

* **Unificación de esquemas**: `sku_unificado` y dimensiones compartidas reducen discrepancias entre sistemas.
* **Enriquecimiento**: unión con CRM para agregar `segmento_cliente`, mejorando segmentación analítica.
* **Consistencia temporal**: normalización de timestamps evita errores de análisis por zonas horarias.
* **Deduplicación y idempotencia**: watermarking y upserts aseguran que re-ejecuciones no creen duplicados.
* **Calidad de datos**: reglas de validación y tablas de error capturan problemas para revisión humana.

---

## 11. Recomendaciones y mejoras futuras

* Introducir CDC (Change Data Capture) para cargas casi en tiempo real desde el POS.
* Implementar un catálogo de datos y linaje (eg. Amundsen, DataHub) para trazabilidad.
* Añadir pruebas automáticas de datos (data contracts) y validaciones con Great Expectations.

---

## 12. ¿Qué entregar como ejercicio?

* README (este documento).
* Diagrama simple del flujo ETL (extracción → staging → transformaciones → DW).
* Scripts/sentencias SQL de ejemplo para crear la tabla destino y reglas de upsert.
* Un pequeño dataset de ejemplo (CSV) y un script pseudocódigo para demostrar la carga incremental.

---


## 13. Diagramas del flujo ETL

He añadido dos representaciones del diagrama en este README: una versión **ASCII** (fácil de ver en terminal/README) y una **diagrama de bloques** más limpia (útil para presentaciones).

### Diagrama ASCII (versión compacta)

```
+----------------+      +-------------+      +----------------+      +--------------------+
| POS (SQL)      | ---> | Staging POS | ---> | Transform/ETL   | ---> | Data Warehouse     |
| ventas, detalle|      | (CSV/DB dump)|     | - limpieza      |      | ventas_consolidadas|
+----------------+      +-------------+      | - normalizar ts |      +--------------------+
                                              | - map SKU       |
+----------------+      +-------------+      | - enriquecer CRM|
| Inventario API  | ---> | Staging Inv | ---> | - deduplicar    |      +--------------------+
+----------------+      +-------------+      +----------------+ ---> | Dimensiones (producto,|
                                                                   | tienda, cliente, fecha)|
+----------------+      +-------------+      +----------------+      +--------------------+
| CRM (CSV)      | ---> | Staging CRM | ---> | Enriquecimiento | ---> | BI / Dashboards    |
+----------------+      +-------------+      | (merge clientes) |      +--------------------+

+----------------+
| Web Logs (JSON)| ->  | Staging Web | -> Transform -> Integrar con ventas online |
+----------------+     +-------------+
```

### Diagrama de bloques (texto legible para README / presentaciones)

1. **Extracción**

   * POS: consultas SQL incremental (watermarking)
   * Inventario: llamada a API REST (diaria)
   * CRM: carga de CSV (semanal)
   * Web: ingestion de logs JSON (diaria o streaming)

2. **Staging**

   * Guardar dumps en `staging/pos/`, `staging/inventario/`, `staging/crm/`, `staging/web/` (S3 o file system)
   * Validaciones iniciales: esquema, tipos, filas corruptas → `staging_errors`

3. **Transformación (ETL)**

   * Normalizar timestamps y monedas
   * Calcular totales y campos derivados
   * Mapear SKUs a `sku_unificado`
   * Enriquecer con `dim_cliente` desde CRM
   * Detectar/Marcar duplicados (por `id_venta` o `dedup_hash`)

4. **Carga**

   * Upsert a `ventas_consolidadas`
   * Actualizar tablas de dimensiones (`dim_producto`, `dim_tienda`, `dim_cliente`)

5. **Consumo**

   * BI/Dashboards (Looker, Metabase)
   * Reportes de inventario y alertas

---

He incluido ambos diagramas arriba en el README. A continuación agrego una versión **Mermaid** del diagrama (útil para render en Markdown o herramientas tipo Mermaid Live) y notas sobre los archivos de imagen generados.

### Diagrama Mermaid

```mermaid
flowchart LR
subgraph Extraccion
POS[POS (SQL)]
Invent[Inventario API]
CRM[CRM (CSV)]
Web[Web Logs (JSON)]
end

POS --> StagingPOS[Staging POS]
Invent --> StagingInv[Staging Inventario]
CRM --> StagingCRM[Staging CRM]
Web --> StagingWeb[Staging Web]

StagingPOS --> ETL[Transform / ETL]
StagingInv --> ETL
StagingCRM --> ETL
StagingWeb --> ETL

ETL --> DW[Data Warehouse]
DW --> BI[BI / Dashboards]

classDef source fill:#f9f,stroke:#333,stroke-width:1px
class POS,Invent,CRM,Web source
```


