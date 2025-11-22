#!/usr/bin/env python3
"""
Script de demostración de carga incremental para el pipeline ETL
Simula la extracción, transformación y carga de ventas desde el POS
"""

import csv
import hashlib
from datetime import datetime
from typing import Dict, List, Optional

# =============================================================================
# CONFIGURACIÓN Y DATOS DE EJEMPLO
# =============================================================================

# Simular watermark (última fecha procesada)
WATERMARK = "2025-01-15 10:00:00"

# Datos de ejemplo del POS (ventas nuevas desde el watermark)
VENTAS_POS = [
    {
        "id_venta": 1001,
        "created_at": "2025-01-15 11:30:00",
        "id_tienda": 5,
        "id_cliente": 2341,
        "id_producto": "POS_SKU_789",
        "cantidad": 2,
        "precio_unitario": 25.50
    },
    {
        "id_venta": 1002,
        "created_at": "2025-01-15 12:15:00",
        "id_tienda": 3,
        "id_cliente": None,  # Compra anónima
        "id_producto": "POS_SKU_456",
        "cantidad": 1,
        "precio_unitario": 89.99
    },
    {
        "id_venta": 1003,
        "created_at": "2025-01-15 13:45:00",
        "id_tienda": 5,
        "id_cliente": 1523,
        "id_producto": "POS_SKU_123",
        "cantidad": 3,
        "precio_unitario": 15.00
    },
    {
        "id_venta": 1001,  # DUPLICADO - mismo id_venta
        "created_at": "2025-01-15 11:30:00",
        "id_tienda": 5,
        "id_cliente": 2341,
        "id_producto": "POS_SKU_789",
        "cantidad": 2,
        "precio_unitario": 25.50
    },
    {
        "id_venta": 1004,
        "created_at": "2025-01-15 14:20:00",
        "id_tienda": 8,
        "id_cliente": 4567,
        "id_producto": "POS_SKU_999",
        "cantidad": -1,  # ERROR: cantidad negativa
        "precio_unitario": 45.00
    }
]

# Mapeo de productos (normalización de SKUs)
PRODUCTO_MAP = {
    "POS_SKU_789": {"sku_unificado": "PROD_001", "nombre": "Camiseta Básica"},
    "POS_SKU_456": {"sku_unificado": "PROD_002", "nombre": "Zapatillas Running"},
    "POS_SKU_123": {"sku_unificado": "PROD_003", "nombre": "Calcetines Pack 3"},
    "POS_SKU_999": {"sku_unificado": "PROD_004", "nombre": "Gorra Deportiva"}
}

# Datos de CRM (segmentación de clientes)
CRM_DATA = {
    2341: {"nombre": "Ana García", "segmento": "premium", "email": "ana.g@email.com"},
    1523: {"nombre": "Carlos López", "segmento": "regular", "email": "carlos.l@email.com"},
    4567: {"nombre": "María Rodríguez", "segmento": "vip", "email": "maria.r@email.com"}
}

# =============================================================================
# FUNCIONES DE TRANSFORMACIÓN
# =============================================================================

def normalize_timestamp(ts_str: str) -> str:
    """Convierte timestamp a formato estándar UTC"""
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y-%m-%d")

def map_producto(id_producto: str, sistema: str = "pos") -> Optional[Dict]:
    """Mapea SKU del sistema origen a SKU unificado"""
    return PRODUCTO_MAP.get(id_producto)

def lookup_segmento(id_cliente: Optional[int]) -> str:
    """Busca segmento del cliente en CRM"""
    if id_cliente is None:
        return "anonimo"
    cliente = CRM_DATA.get(id_cliente, {})
    return cliente.get("segmento", "sin_clasificar")

def calcular_dedup_hash(venta: Dict) -> str:
    """Calcula hash para detección de duplicados"""
    # Crear string con campos críticos
    dedup_str = f"{venta['id_tienda']}|{venta['fecha_venta']}|{venta['id_producto']}|{venta['cantidad']}|{venta['total_venta']}"
    return hashlib.md5(dedup_str.encode()).hexdigest()

def validar_venta(venta: Dict) -> tuple[bool, Optional[str]]:
    """Valida reglas de negocio"""
    # Validar campos requeridos
    if venta["id_producto"] is None:
        return False, "id_producto es nulo"
    
    if venta["cantidad"] is None:
        return False, "cantidad es nula"
    
    # Validar rangos
    if venta["cantidad"] < 0:
        return False, "cantidad negativa"
    
    if venta["precio_unitario"] < 0:
        return False, "precio_unitario negativo"
    
    return True, None

# =============================================================================
# PIPELINE ETL
# =============================================================================

def extract_ventas_incrementales(watermark: str) -> List[Dict]:
    """Extrae ventas nuevas desde el watermark"""
    print(f"\n[EXTRACT] Extrayendo ventas con created_at > '{watermark}'")
    
    nuevas_ventas = [
        v for v in VENTAS_POS 
        if v["created_at"] > watermark
    ]
    
    print(f"[EXTRACT] ✓ Extraídas {len(nuevas_ventas)} ventas nuevas")
    return nuevas_ventas

def transform_ventas(ventas: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """Transforma y limpia ventas"""
    print(f"\n[TRANSFORM] Procesando {len(ventas)} ventas...")
    
    ventas_validas = []
    ventas_error = []
    dedup_set = set()
    duplicados_detectados = 0
    
    for venta in ventas:
        # 1. Normalizar timestamp
        fecha_venta = normalize_timestamp(venta["created_at"])
        
        # 2. Calcular total
        total_venta = venta["cantidad"] * venta["precio_unitario"]
        
        # 3. Mapear producto
        producto_info = map_producto(venta["id_producto"])
        if not producto_info:
            ventas_error.append({
                **venta,
                "error": f"Producto {venta['id_producto']} no encontrado en mapeo"
            })
            continue
        
        # 4. Enriquecer con CRM
        segmento_cliente = lookup_segmento(venta["id_cliente"])
        
        # 5. Crear registro transformado
        venta_transformada = {
            "id_venta": venta["id_venta"],
            "fecha_venta": fecha_venta,
            "id_tienda": venta["id_tienda"],
            "id_cliente": venta["id_cliente"],
            "id_producto": producto_info["sku_unificado"],
            "cantidad": venta["cantidad"],
            "precio_unitario": venta["precio_unitario"],
            "total_venta": round(total_venta, 2),
            "canal_venta": "tienda",
            "segmento_cliente": segmento_cliente,
            "created_at": venta["created_at"],
            "fuente": "pos"
        }
        
        # 6. Calcular hash para deduplicación
        dedup_hash = calcular_dedup_hash(venta_transformada)
        venta_transformada["dedup_hash"] = dedup_hash
        
        # 7. Validar
        es_valida, error_msg = validar_venta(venta_transformada)
        if not es_valida:
            ventas_error.append({
                **venta,
                "error": error_msg
            })
            continue
        
        # 8. Detectar duplicados
        if venta_transformada["id_venta"] in dedup_set:
            duplicados_detectados += 1
            print(f"[TRANSFORM] ⚠ Duplicado detectado: id_venta={venta_transformada['id_venta']}")
            continue
        
        dedup_set.add(venta_transformada["id_venta"])
        ventas_validas.append(venta_transformada)
    
    print(f"[TRANSFORM] ✓ Ventas válidas: {len(ventas_validas)}")
    print(f"[TRANSFORM] ⚠ Errores: {len(ventas_error)}")
    print(f"[TRANSFORM] ⚠ Duplicados: {duplicados_detectados}")
    
    return ventas_validas, ventas_error

def load_to_warehouse(ventas: List[Dict], errores: List[Dict]):
    """Simula carga al data warehouse (upsert)"""
    print(f"\n[LOAD] Cargando {len(ventas)} ventas a ventas_consolidadas...")
    
    # Simular upsert SQL
    for venta in ventas:
        sql = f"""
        INSERT INTO ventas_consolidadas 
        (id_venta, fecha_venta, id_tienda, id_cliente, id_producto, 
         cantidad, precio_unitario, total_venta, canal_venta, 
         segmento_cliente, created_at, fuente, dedup_hash)
        VALUES 
        ({venta['id_venta']}, '{venta['fecha_venta']}', {venta['id_tienda']}, 
         {venta['id_cliente']}, '{venta['id_producto']}', {venta['cantidad']}, 
         {venta['precio_unitario']}, {venta['total_venta']}, '{venta['canal_venta']}', 
         '{venta['segmento_cliente']}', '{venta['created_at']}', '{venta['fuente']}',
         '{venta['dedup_hash']}')
        ON CONFLICT (id_venta) DO UPDATE SET
            cantidad = EXCLUDED.cantidad,
            total_venta = EXCLUDED.total_venta,
            segmento_cliente = EXCLUDED.segmento_cliente;
        """
        print(f"  → Upsert id_venta={venta['id_venta']}, total=${venta['total_venta']}, segmento={venta['segmento_cliente']}")
    
    print(f"[LOAD] ✓ Carga completada")
    
    # Cargar errores a tabla de errores
    if errores:
        print(f"\n[LOAD] Cargando {len(errores)} registros a tabla de errores...")
        for error in errores:
            print(f"  → Error: {error.get('error', 'desconocido')} - Registro: {error}")

def actualizar_watermark(ventas: List[Dict]) -> str:
    """Actualiza watermark a la última fecha procesada"""
    if not ventas:
        return WATERMARK
    
    max_timestamp = max(v["created_at"] for v in ventas)
    print(f"\n[WATERMARK] Actualizando de '{WATERMARK}' a '{max_timestamp}'")
    return max_timestamp

# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================

def main():
    print("="*70)
    print("PIPELINE ETL - CARGA INCREMENTAL DE VENTAS POS")
    print("="*70)
    
    # 1. Extract
    ventas_nuevas = extract_ventas_incrementales(WATERMARK)
    
    if not ventas_nuevas:
        print("\n[INFO] No hay ventas nuevas para procesar")
        return
    
    # 2. Transform
    ventas_validas, ventas_error = transform_ventas(ventas_nuevas)
    
    # 3. Load
    load_to_warehouse(ventas_validas, ventas_error)
    
    # 4. Update watermark
    nuevo_watermark = actualizar_watermark(ventas_nuevas)
    
    # 5. Resumen
    print("\n" + "="*70)
    print("RESUMEN DE EJECUCIÓN")
    print("="*70)
    print(f"Registros extraídos:     {len(ventas_nuevas)}")
    print(f"Registros cargados:      {len(ventas_validas)}")
    print(f"Registros con error:     {len(ventas_error)}")
    print(f"Duplicados detectados:   {len(ventas_nuevas) - len(ventas_validas) - len(ventas_error)}")
    print(f"Nuevo watermark:         {nuevo_watermark}")
    print("="*70)

if __name__ == "__main__":
    main()