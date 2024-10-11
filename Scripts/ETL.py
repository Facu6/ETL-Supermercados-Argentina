import pandas as pd
import requests
import os
from pathlib import Path


def extraer_datos():
    
    url = 'https://infra.datos.gob.ar/catalog/sspm/dataset/455/distribution/455.1/download/ventas-totales-supermercados-2.csv'
    
    response = requests.get(url)
    
    # Obtener ruta del directorio actual del script
    directorio_actual = Path(os.getcwd())

    data_dir = directorio_actual / 'Data_Extraida'
    data_dir.mkdir(exist_ok= True)

    ruta_archivo_csv = data_dir / 'ventas_supermercado.csv'

    with open(ruta_archivo_csv, 'wb') as file:
        file.write(response.content)
    
    df = pd.read_csv(ruta_archivo_csv)
    return df

    print(f'Archivo guardado en: {ruta_archivo_csv}')

extraer_datos()