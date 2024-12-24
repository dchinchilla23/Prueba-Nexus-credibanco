import sqlite3
import pandas as pd
from datetime import datetime

# Conectar a la base de datos SQLite
conn = sqlite3.connect('tienda.db')

# Cargar las tablas 'Venta' y 'Almacen' desde la base de datos a DataFrames de pandas
ventas_df = pd.read_sql_query('SELECT idAlmacen, Cantidad, valorUnitario, fecha FROM Venta', conn)
almacenes_df = pd.read_sql_query('SELECT idAlmacen, sucursal FROM Almacen', conn)

# Unir las tablas de ventas y almacenes por 'idAlmacen'
merged_df = pd.merge(ventas_df, almacenes_df, on='idAlmacen')

# Calcular el total de ventas por registro
merged_df['total_venta'] = merged_df['Cantidad'] * merged_df['valorUnitario']

# Convertir la columna 'fecha' a tipo datetime
merged_df['fecha'] = pd.to_datetime(merged_df['fecha'])

# Filtrar las ventas del año actual
año_actual = datetime.now().year
ventas_actual = merged_df[merged_df['fecha'].dt.year == año_actual]

# Agrupar por mes y sumar las ventas
ventas_mes = ventas_actual.groupby(ventas_actual['fecha'].dt.to_period('M'))['total_venta'].sum().reset_index()

# Renombrar las columnas para mayor claridad
ventas_mes.columns = ['mes', 'total_ventas']

# Mostrar el resultado en la consola
print(ventas_mes)

# Cerrar la conexión a la base de datos
conn.close()