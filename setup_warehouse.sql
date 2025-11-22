-- ============================================================================
-- SCRIPT DE SETUP DEL DATA WAREHOUSE
-- Crear las tablas de hechos y dimensiones para el pipeline ETL
-- ============================================================================

-- Tabla de hechos principal
CREATE TABLE IF NOT EXISTS ventas_consolidadas (
    id_venta INTEGER PRIMARY KEY,
    fecha_venta DATE NOT NULL,
    id_tienda INTEGER NOT NULL,
    id_cliente INTEGER,
    id_producto VARCHAR(50) NOT NULL,
    cantidad INTEGER NOT NULL CHECK (cantidad >= 0),
    precio_unitario DECIMAL(10,2) NOT NULL CHECK (precio_unitario >= 0),
    total_venta DECIMAL(10,2) NOT NULL,
    canal_venta VARCHAR(20) NOT NULL CHECK (canal_venta IN ('online', 'tienda')),
    segmento_cliente VARCHAR(20),
    created_at TIMESTAMP NOT NULL,
    fuente VARCHAR(20) NOT NULL CHECK (fuente IN ('pos', 'web', 'otros')),
    dedup_hash VARCHAR(32),
    carga_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_tienda FOREIGN KEY (id_tienda) REFERENCES dim_tienda(id_tienda),
    CONSTRAINT fk_producto FOREIGN KEY (id_producto) REFERENCES dim_producto(sku_unificado),
    CONSTRAINT fk_cliente FOREIGN KEY (id_cliente) REFERENCES dim_cliente(id_cliente),
    CONSTRAINT fk_fecha FOREIGN KEY (fecha_venta) REFERENCES dim_fecha(fecha)
);

-- Índices para mejorar performance
CREATE INDEX idx_ventas_fecha ON ventas_consolidadas(fecha_venta);
CREATE INDEX idx_ventas_tienda ON ventas_consolidadas(id_tienda);
CREATE INDEX idx_ventas_producto ON ventas_consolidadas(id_producto);
CREATE INDEX idx_ventas_cliente ON ventas_consolidadas(id_cliente);
CREATE INDEX idx_ventas_dedup ON ventas_consolidadas(dedup_hash);
CREATE INDEX idx_ventas_created_at ON ventas_consolidadas(created_at);

-- ============================================================================
-- TABLAS DIMENSIONALES
-- ============================================================================

-- Dimensión de tiendas
CREATE TABLE IF NOT EXISTS dim_tienda (
    id_tienda INTEGER PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    ciudad VARCHAR(50),
    tipo VARCHAR(30) CHECK (tipo IN ('flagship', 'outlet', 'pop-up', 'regular')),
    fecha_apertura DATE,
    activa BOOLEAN DEFAULT TRUE,
    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Datos de ejemplo para dim_tienda
INSERT INTO dim_tienda (id_tienda, nombre, region, ciudad, tipo, fecha_apertura) VALUES
(3, 'Tienda Centro', 'Metropolitana', 'Santiago', 'flagship', '2020-01-15'),
(5, 'Tienda Mall Plaza', 'Metropolitana', 'Santiago', 'regular', '2019-05-20'),
(8, 'Outlet Viña', 'Valparaíso', 'Viña del Mar', 'outlet', '2021-03-10')
ON CONFLICT (id_tienda) DO NOTHING;

-- Dimensión de productos
CREATE TABLE IF NOT EXISTS dim_producto (
    sku_unificado VARCHAR(50) PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    categoria VARCHAR(50),
    subcategoria VARCHAR(50),
    marca VARCHAR(50),
    precio_sugerido DECIMAL(10,2),
    activo BOOLEAN DEFAULT TRUE,
    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Datos de ejemplo para dim_producto
INSERT INTO dim_producto (sku_unificado, nombre, categoria, subcategoria, marca, precio_sugerido) VALUES
('PROD_001', 'Camiseta Básica', 'Ropa', 'Camisetas', 'BasicWear', 25.50),
('PROD_002', 'Zapatillas Running', 'Calzado', 'Deportivo', 'SportMax', 89.99),
('PROD_003', 'Calcetines Pack 3', 'Accesorios', 'Calcetería', 'ComfortPlus', 15.00),
('PROD_004', 'Gorra Deportiva', 'Accesorios', 'Gorras', 'SportMax', 45.00)
ON CONFLICT (sku_unificado) DO NOTHING;

-- Dimensión de clientes
CREATE TABLE IF NOT EXISTS dim_cliente (
    id_cliente INTEGER PRIMARY KEY,
    nombre VARCHAR(100),
    email VARCHAR(150),
    segmento VARCHAR(20) CHECK (segmento IN ('vip', 'premium', 'regular', 'nuevo', 'sin_clasificar', 'anonimo')),
    fecha_registro DATE,
    activo BOOLEAN DEFAULT TRUE,
    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Datos de ejemplo para dim_cliente
INSERT INTO dim_cliente (id_cliente, nombre, email, segmento, fecha_registro) VALUES
(2341, 'Ana García', 'ana.garcia@email.com', 'premium', '2022-03-15'),
(1523, 'Carlos López', 'carlos.lopez@email.com', 'regular', '2023-01-20'),
(4567, 'María Rodríguez', 'maria.rodriguez@email.com', 'vip', '2021-11-05')
ON CONFLICT (id_cliente) DO NOTHING;

-- Dimensión de fecha (tabla calendario)
CREATE TABLE IF NOT EXISTS dim_fecha (
    fecha DATE PRIMARY KEY,
    anio INTEGER NOT NULL,
    mes INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia INTEGER NOT NULL CHECK (dia BETWEEN 1 AND 31),
    trimestre INTEGER NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    semana INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL CHECK (dia_semana BETWEEN 1 AND 7),
    nombre_dia VARCHAR(20),
    nombre_mes VARCHAR(20),
    es_fin_semana BOOLEAN,
    es_festivo BOOLEAN DEFAULT FALSE,
    nombre_festivo VARCHAR(100)
);

-- Procedimiento para poblar dim_fecha (ejemplo simplificado)
-- En producción, esto debería generar varios años de datos
INSERT INTO dim_fecha (fecha, anio, mes, dia, trimestre, semana, dia_semana, nombre_dia, nombre_mes, es_fin_semana)
SELECT 
    fecha,
    EXTRACT(YEAR FROM fecha) as anio,
    EXTRACT(MONTH FROM fecha) as mes,
    EXTRACT(DAY FROM fecha) as dia,
    EXTRACT(QUARTER FROM fecha) as trimestre,
    EXTRACT(WEEK FROM fecha) as semana,
    EXTRACT(DOW FROM fecha) + 1 as dia_semana,
    TO_CHAR(fecha, 'Day') as nombre_dia,
    TO_CHAR(fecha, 'Month') as nombre_mes,
    EXTRACT(DOW FROM fecha) IN (0, 6) as es_fin_semana
FROM generate_series('2025-01-01'::date, '2025-12-31'::date, '1 day'::interval) fecha
ON CONFLICT (fecha) DO NOTHING;

-- ============================================================================
-- TABLA DE MAPEO DE PRODUCTOS (para normalización de SKUs)
-- ============================================================================

CREATE TABLE IF NOT EXISTS producto_map (
    id_mapeo SERIAL PRIMARY KEY,
    sistema VARCHAR(50) NOT NULL,
    id_sistema VARCHAR(100) NOT NULL,
    sku_unificado VARCHAR(50) NOT NULL,
    activo BOOLEAN DEFAULT TRUE,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(sistema, id_sistema),
    CONSTRAINT fk_producto_unificado FOREIGN KEY (sku_unificado) REFERENCES dim_producto(sku_unificado)
);

-- Datos de ejemplo para mapeo
INSERT INTO producto_map (sistema, id_sistema, sku_unificado) VALUES
('pos', 'POS_SKU_789', 'PROD_001'),
('pos', 'POS_SKU_456', 'PROD_002'),
('pos', 'POS_SKU_123', 'PROD_003'),
('pos', 'POS_SKU_999', 'PROD_004'),
('inventario', 'INV_PROD_001', 'PROD_001'),
('inventario', 'INV_PROD_002', 'PROD_002'),
('inventario', 'INV_PROD_003', 'PROD_003'),
('inventario', 'INV_PROD_004', 'PROD_004')
ON CONFLICT (sistema, id_sistema) DO NOTHING;

-- ============================================================================
-- TABLA DE ERRORES (para auditoría)
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging_errors (
    id_error SERIAL PRIMARY KEY,
    fuente VARCHAR(50) NOT NULL,
    registro_json JSONB NOT NULL,
    error_descripcion TEXT NOT NULL,
    error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    procesado BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_errors_timestamp ON staging_errors(error_timestamp);
CREATE INDEX idx_errors_fuente ON staging_errors(fuente);

-- ============================================================================
-- TABLA DE WATERMARKS (para cargas incrementales)
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_watermarks (
    fuente VARCHAR(50) PRIMARY KEY,
    ultimo_timestamp TIMESTAMP NOT NULL,
    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    registros_procesados INTEGER DEFAULT 0
);

-- Inicializar watermarks
INSERT INTO etl_watermarks (fuente, ultimo_timestamp) VALUES
('pos_ventas', '2025-01-15 10:00:00'),
('web_logs', '2025-01-15 00:00:00'),
('inventario', '2025-01-14 00:00:00'),
('crm', '2025-01-08 00:00:00')
ON CONFLICT (fuente) DO NOTHING;

-- ============================================================================
-- TABLA DE INVENTARIO (snapshot diario)
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_inventario (
    id_inventario SERIAL PRIMARY KEY,
    sku_unificado VARCHAR(50) NOT NULL,
    ubicacion VARCHAR(50) NOT NULL,
    stock INTEGER NOT NULL CHECK (stock >= 0),
    stock_minimo INTEGER,
    fecha_snapshot DATE NOT NULL,
    
    UNIQUE(sku_unificado, ubicacion, fecha_snapshot),
    CONSTRAINT fk_inv_producto FOREIGN KEY (sku_unificado) REFERENCES dim_producto(sku_unificado)
);

CREATE INDEX idx_inventario_fecha ON dim_inventario(fecha_snapshot);
CREATE INDEX idx_inventario_sku ON dim_inventario(sku_unificado);

-- ============================================================================
-- VISTAS ÚTILES PARA ANÁLISIS
-- ============================================================================

-- Vista de ventas enriquecidas
CREATE OR REPLACE VIEW v_ventas_detalle AS
SELECT 
    v.id_venta,
    v.fecha_venta,
    f.nombre_mes,
    f.anio,
    f.trimestre,
    f.es_fin_semana,
    t.nombre as nombre_tienda,
    t.region,
    t.ciudad,
    c.nombre as nombre_cliente,
    c.email,
    v.segmento_cliente,
    p.nombre as nombre_producto,
    p.categoria,
    p.marca,
    v.cantidad,
    v.precio_unitario,
    v.total_venta,
    v.canal_venta,
    v.fuente
FROM ventas_consolidadas v
LEFT JOIN dim_fecha f ON v.fecha_venta = f.fecha
LEFT JOIN dim_tienda t ON v.id_tienda = t.id_tienda
LEFT JOIN dim_cliente c ON v.id_cliente = c.id_cliente
LEFT JOIN dim_producto p ON v.id_producto = p.sku_unificado;

-- Vista de resumen de ventas por día y tienda
CREATE OR REPLACE VIEW v_ventas_resumen_diario AS
SELECT 
    fecha_venta,
    id_tienda,
    COUNT(*) as num_transacciones,
    SUM(cantidad) as unidades_vendidas,
    SUM(total_venta) as total_ventas,
    AVG(total_venta) as ticket_promedio,
    COUNT(DISTINCT id_cliente) as clientes_unicos
FROM ventas_consolidadas
GROUP BY fecha_venta, id_tienda;

-- ============================================================================
-- COMENTARIOS EN TABLAS (documentación)
-- ============================================================================

COMMENT ON TABLE ventas_consolidadas IS 'Tabla de hechos principal con todas las ventas consolidadas de diferentes fuentes';
COMMENT ON COLUMN ventas_consolidadas.dedup_hash IS 'Hash MD5 para detección de duplicados basado en campos críticos';
COMMENT ON COLUMN ventas_consolidadas.fuente IS 'Sistema origen: pos, web, otros';

COMMENT ON TABLE dim_fecha IS 'Dimensión calendario con todos los días del año y atributos temporales';
COMMENT ON TABLE producto_map IS 'Mapeo de SKUs entre diferentes sistemas al SKU unificado';
COMMENT ON TABLE etl_watermarks IS 'Marcas de agua para cargas incrementales por fuente';

-- ============================================================================
-- SCRIPT COMPLETADO
-- ============================================================================

SELECT 'Setup completado exitosamente' as mensaje;




