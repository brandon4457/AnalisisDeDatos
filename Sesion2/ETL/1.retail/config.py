## VAMOS A LLEVAR A SCRIPT EN PYTHON 
## CONTINUACIÓN DE LA SESION 4 DE ANALISIS DE DATOS
##TRAEMOS LA CONFIGURACIÓN DE BASE DE DATOS

##Este diccionario contiene la configuración para conectarse a una base de datos, probablemente 
# utilizando MySQL (por el puerto 3310 y el usuario root).
DATABASE_CONFIG = {
    'host': '10.0.2.74',
    'port': 3310,
    'user': 'root',
    'password': 'root',
    'database': 'retail_db'
}

##Este diccionario contiene rutas relativas de archivos CSV que corresponden a distintas tablas
#  del sistema de base de datos retail_db. Estos archivos suelen usarse para importar datos a la 
# base de datos o para realizar análisis sin conexión.
 
CSV_FILES = {
    'customers': 'data_retail/customers',
    'departments': 'data_retail/departments',
    'categories': 'data_retail/categories',
    'products': 'data_retail/products',
    'orders': 'data_retail/orders',
    'order_items': 'data_retail/order_items',
}

#La ruta para un archivo de registro o log:


 
LOG_FILE = 'log/pipeline.log'