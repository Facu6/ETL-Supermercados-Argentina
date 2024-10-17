import pandera as pa
import pandas as pd
from pandera import Column, DataFrameSchema



esquema_extraccion = {
    'ventas_supermercados' : pa.DataFrameSchema({
        'indice_tiempo' : Column(pa.String, nullable = False),
        'ventas_precios_corrientes' : Column(pa.Float, nullable = False), #
        'ventas_precios_constantes' : Column(pa.Float, nullable = False),
        'ventas_totales_canal_venta' : Column(pa.Float, nullable = False),
        'salon_ventas' : Column(pa.Float, nullable = False),
        'canales_on_line' : Column(pa.Float, nullable = False),
        'ventas_totales_medio_pago' : Column(pa.Float, nullable = False),
        'efectivo' : Column(pa.Float, nullable = False),
        'tarjetas_debito' : Column(pa.Float, nullable = False),
        'tarjetas_credito' : Column(pa.Float, nullable = False),
        'otros_medios' : Column(pa.Float, nullable = False),
        'ventas_totales_grupo_articulos' : Column(pa.Float, nullable = False),
        'subtotal_ventas_alimentos_bebidas' : Column(pa.Float, nullable = False),
        'bebidas' : Column(pa.Float, nullable = False),
        'almacen' : Column(pa.Float, nullable = False),
        'panaderia' : Column(pa.Float, nullable = False),
        'lacteos' : Column(pa.Float, nullable = False),
        'carnes' : Column(pa.Float, nullable = False),
        'verduleria_fruteria' : Column(pa.Float, nullable = False),
        'alimentos_preparados_rotiseria' : Column(pa.Float, nullable = False),
        'articulos_limpieza_perfumeria' : Column(pa.Float, nullable = False),
        'indumentaria_calzado_textiles_hogar' : Column(pa.Float, nullable = False),
        'electronicos_articulos_hogar' : Column(pa.Float, nullable = False),
        'otros' : Column(pa.Float, nullable = False)    
    })
    
}



esquema_carga = DataFrameSchema({
        'indice_tiempo' : Column(pa.DateTime, nullable = False),
        'ventas_precios_corrientes' : Column(pa.Int, nullable = False), #
        'ventas_precios_constantes' : Column(pa.Int, nullable = False),
        'ventas_totales_canal_venta' : Column(pa.Int, nullable = False),
        'salon_ventas' : Column(pa.Int, nullable = False),
        'canales_on_line' : Column(pa.Int, nullable = False),
        'ventas_totales_medio_pago' : Column(pa.Int, nullable = False),
        'efectivo' : Column(pa.Int, nullable = False),
        'tarjetas_debito' : Column(pa.Int, nullable = False),
        'tarjetas_credito' : Column(pa.Int, nullable = False),
        'otros_medios' : Column(pa.Int, nullable = False),
        'ventas_totales_grupo_articulos' : Column(pa.Int, nullable = False),
        'subtotal_ventas_alimentos_bebidas' : Column(pa.Int, nullable = False),
        'bebidas' : Column(pa.Int, nullable = False),
        'almacen' : Column(pa.Int, nullable = False),
        'panaderia' : Column(pa.Int, nullable = False),
        'lacteos' : Column(pa.Int, nullable = False),
        'carnes' : Column(pa.Int, nullable = False),
        'verduleria_fruteria' : Column(pa.Int, nullable = False),
        'alimentos_preparados_rotiseria' : Column(pa.Int, nullable = False),
        'articulos_limpieza_perfumeria' : Column(pa.Int, nullable = False),
        'indumentaria_calzado_textiles_hogar' : Column(pa.Int, nullable = False),
        'electronicos_articulos_hogar' : Column(pa.Int, nullable = False),
        'otros' : Column(pa.Float, nullable = False)    
    })
    