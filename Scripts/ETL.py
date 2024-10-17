import pandas as pd
import pandera as pa
import requests
import os
import pymysql
from pymysql import Error
from pathlib import Path
from VALIDACION_DATOS import esquema_extraccion, esquema_carga

df_global = None

def extraer_datos():
    
    global df_global
    
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
    df_global = pd.read_csv(ruta_archivo_csv)    
    
    # Obtener los nombres de las columnas actuales del DataFrame
    columnas_actuales = set(df_global.columns)
    # Obtener los nombres de las columnas esperadas según el esquema de validación
    columnas_esperadas = set(esquema_extraccion['ventas_supermercados'].columns.keys())
    
    # Determinar si hay nuevas columnas en el DataFrame que no están en el esquema
    nuevas_columnas = columnas_actuales - columnas_esperadas
    if nuevas_columnas:
        print(f'Se encontraron nuevas columnas no contempladas en el esquema de "Validación de Datos": {nuevas_columnas}')
    
    
    try:
        # Validar cada columna del DataFrame individualmente
        for columna in df_global.columns:
            # Crear un DataFrame con una sola columna para la validación
            df_columnas = df_global[[columna]]
            # Definir el esquema de validación para la columna específica
            esquema_columnas = pa.DataFrameSchema({columna: esquema_extraccion['ventas_supermercados'].columns[columna]})
            # Validar la columna usando Pandera
            esquema_columnas.validate(df_columnas)
            # Imprimir detalles de la validación exitosa de la columna
            print(f'La validación de datos para la columna "{columna}" fue exitosa. \n - Tipo Dato: {df_global[columna].dtype} \n - Cantidad Nulos: {df_global[columna].isnull().sum()}')
    except pa.errors.SchemaError as e:
        # Imprimir un mensaje de error si la validación falla para alguna columna
        print(f'ERROR EN LA VALIDACIÓN DE DATOS PARA LA COLUMNA: {columna}')


def transformar_datos():
    global df_global
    
    
    # Verifica si df_global es None
    if df_global is None:
        print('No hay datos cargados. Asegúrese de haber descargado el/los archivos correspondientes.')
    
    # Se modifica el tipo de dato de la columna "indice_tiempo"
    df_global['indice_tiempo'] = pd.to_datetime(df_global['indice_tiempo'])
    # Lista de columnas a convertir a enteros para aún mejor visualización.
    columnas_convertir = ['ventas_precios_corrientes',
    'ventas_precios_constantes',
    'ventas_totales_canal_venta',
    'salon_ventas',
    'canales_on_line',
    'ventas_totales_medio_pago',
    'efectivo',
    'tarjetas_debito',
    'tarjetas_credito',
    'otros_medios',
    'ventas_totales_grupo_articulos',
    'subtotal_ventas_alimentos_bebidas',
    'bebidas',
    'almacen',
    'panaderia',
    'lacteos',
    'carnes',
    'verduleria_fruteria',
    'alimentos_preparados_rotiseria',
    'articulos_limpieza_perfumeria',
    'indumentaria_calzado_textiles_hogar',
    'electronicos_articulos_hogar',
    'otros']

    for nombre_columna in columnas_convertir:
        df_global[nombre_columna] = df_global[nombre_columna].astype(int)
    
     # Selecciona columnas numéricas
    columnas_numericas = df_global.select_dtypes(include=['number']).columns
    # Reemplaza valores nulos en columnas numéricas con 0
    if df_global[columnas_numericas].isnull().any().any():
        df_global[columnas_numericas] = df_global[columnas_numericas].fillna(0)

    # Selecciona columnas de tipo datetime
    columna_fecha = df_global.select_dtypes(include=['datetime']).columns
    
    # Comprueba si hay valores nulos en columnas de tipo datetime
    if df_global[columna_fecha].isnull().any().any():
        print(f'Hay valores faltantes en la columna {df_global[columna_fecha]}. Corrobore si puede incorporar el año correspondiente')    
    
    # Comprobar si hay valores duplicados en columnas de tipo datetime
    if df_global[columna_fecha].duplicated().any():  # Verificar duplicados
        print(f'Hay una fecha repetida en la columna {columna_fecha}. Corrobore la fila correspondiente para realizar reemplazo y/o eliminación del dato.')
    
    else:
        print('Las columnas del archivo no poseen nulos y/o duplicados.')
    

def cargar_datos():
    
    global df_global
    
    
    try: 
        df_global = esquema_carga.validate(df_global)
        print('Validación de datos para "carga" exitosa.')
    except pa.errors.SchemaError as e:
        print(f'ERROR EN LA VALIDACIÓN DE DATOS PARA "CARGA": {e}') 
        
    
    
    try:       
        conexion = pymysql.connect(
            host='localhost',
            user='root',
            password='Lisandrotorre478-',  
            database='supermercadosargentina'
        )        
        cursor = conexion.cursor()
        
        print('Intentando crear la tabla...')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ventas(
                id INT AUTO_INCREMENT PRIMARY KEY,
                indice_tiempo DATE,
                ventas_precios_corrientes INT,
                ventas_precios_constantes INT,
                ventas_totales_canal_venta INT, 
                salon_ventas INT,
                canales_on_line INT,
                ventas_totales_medio_pago INT,
                efectivo INT,
                tarjetas_debito INT,
                tarjetas_credito INT,
                otros_medios INT,
                ventas_totales_grupo_articulos INT,
                subtotal_ventas_alimentos_bebidas INT,
                bebidas INT,
                almacen INT,
                panaderia INT,
                lacteos INT,
                carnes INT,
                verduleria_fruteria INT,
                alimentos_preparados_rotiseria INT,
                articulos_limpieza_perfumeria INT,
                indumentaria_calzado_textiles_hogar INT,
                electronicos_articulos_hogar INT,
                otros INT,
                UNIQUE(indice_tiempo)
            )
        ''')
        print('Tabla creada/existente.')
        
        print('Cargando datos a la tabla...')

        for i, row in df_global.iterrows():
            try:
                sql = '''
                INSERT IGNORE INTO ventas (
                    indice_tiempo, 
                    ventas_precios_corrientes, 
                    ventas_precios_constantes,
                    ventas_totales_canal_venta, 
                    salon_ventas, 
                    canales_on_line, 
                    ventas_totales_medio_pago,
                    efectivo, 
                    tarjetas_debito, 
                    tarjetas_credito,
                    otros_medios, 
                    ventas_totales_grupo_articulos, 
                    subtotal_ventas_alimentos_bebidas, 
                    bebidas, 
                    almacen,
                    panaderia, 
                    lacteos, 
                    carnes, 
                    verduleria_fruteria,
                    alimentos_preparados_rotiseria, 
                    articulos_limpieza_perfumeria, 
                    indumentaria_calzado_textiles_hogar,
                    electronicos_articulos_hogar, 
                    otros
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, tuple(row))
            except Error as e:
                print(f'ERROR AL INSERTAR FILA: {i}: {e}')
            
        conexion.commit()
        print(f'Datos insertados correctamente en la tabla.')
        
    except Error as e:
        print(f'ERROR AL CARGAR LOS DATOS: {e}')
    
    finally:
        if conexion.open:
            cursor.close()
            conexion.close()
            print('Conexión cerrada.')
    
        
 # PENDIENTE ---->  - INFORMARME QUE HACE Y LA FUNCIONALIDAD DE BLOQUE "FINALLY"
 #                  - CARGA INCREMENTAL (AL EJECUTAR VARIAS VECES EL SCRIPT, LAS FILAS EN MYSQL SE VAN MULTIPLICANDO)
           
if __name__ == '__main__':      
    extraer_datos()
    transformar_datos()
    cargar_datos()
