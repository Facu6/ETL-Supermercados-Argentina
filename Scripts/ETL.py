import pandas as pd
import pandera as pa
import requests
import os
from pathlib import Path
from VALIDACION_DATOS import esquema


def extraer_datos():
    # URL del archivo CSV que se va a descargar
    url = 'https://infra.datos.gob.ar/catalog/sspm/dataset/455/distribution/455.1/download/ventas-totales-supermercados-2.csv'
    
    # Realiza una solicitud GET para descargar el archivo CSV desde la URL proporcionada
    response = requests.get(url)
    
    # Obtener la ruta del directorio actual del script
    directorio_actual = Path(os.getcwd())
    
    # Definir y crear un directorio llamado 'Data_Extraida' en el directorio actual para almacenar los datos descargados
    data_dir = directorio_actual / 'Data_Extraida'
    data_dir.mkdir(exist_ok=True)  # Crea el directorio si no existe

    # Ruta completa donde se guardará el archivo CSV descargado
    ruta_archivo_csv = data_dir / 'ventas_supermercado.csv'

    # Guardar el contenido descargado en el archivo CSV
    with open(ruta_archivo_csv, 'wb') as file:
        file.write(response.content)

    # Imprimir la ubicación del archivo guardado
    print(f'Archivo guardado en: {ruta_archivo_csv}')
    
    # Leer el archivo CSV en un DataFrame de pandas
    df = pd.read_csv(ruta_archivo_csv)    
    
    # Obtener los nombres de las columnas actuales del DataFrame
    columnas_actuales = set(df.columns)
    # Obtener los nombres de las columnas esperadas según el esquema de validación
    columnas_esperadas = set(esquema['ventas_supermercados'].columns.keys())
    
    # Determinar si hay nuevas columnas en el DataFrame que no están en el esquema
    nuevas_columnas = columnas_actuales - columnas_esperadas
    if nuevas_columnas:
        print(f'Se encontraron nuevas columnas no contempladas en el esquema de "Validación de Datos": {nuevas_columnas}')
    
    
    try:
        # Validar cada columna del DataFrame individualmente
        for columna in df.columns:
            # Crear un DataFrame con una sola columna para la validación
            df_columnas = df[[columna]]
            # Definir el esquema de validación para la columna específica
            esquema_columnas = pa.DataFrameSchema({columna: esquema['ventas_supermercados'].columns[columna]})
            # Validar la columna usando Pandera
            esquema_columnas.validate(df_columnas)
            # Imprimir detalles de la validación exitosa de la columna
            print(f'La validación de datos para la columna "{columna}" fue exitosa. \n - Tipo Dato: {df[columna].dtype} \n - Cantidad Nulos: {df[columna].isnull().sum()}')
    except pa.errors.SchemaError as e:
        # Imprimir un mensaje de error si la validación falla para alguna columna
        print(f'Error en la validación de datos para la columna: {columna}')

        
    
extraer_datos()

# CÓDIGO FUNCIONAL COMPLETO (HASTA VALIDACIÓN DE DATOS)
# PRÓXIMA TAREA: ¿QUÉ PASA SI SE AGREGA UNA NUEVA COLUMNA AL CSV EL CUAL SE DESCARGA? ¿LA VALIDACIÓN FALLA?