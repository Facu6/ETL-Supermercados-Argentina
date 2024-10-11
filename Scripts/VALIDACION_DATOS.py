import pandera as pa
import pandas as pd
from pandera import Column, DataFrameSchema
from ETL import extraer_datos

df = extraer_datos()



columnas_flotantes = [columnas for columnas in df.columns if df[columnas].dtype == 'float64']

esquema1 = {
        'indice_tiempo' : Column(pa.String, nullable = False)
        }

for columnas in columnas_flotantes:
    esquema1[columnas] = Column(pa.Float, nullable = False)
    
# Crear esquema

esquema_final = pa.DataFrameSchema(esquema1)

# PENDIENTE --------------------> APLICAR LA VALIDACION PARA EL CSV DESCARGADO