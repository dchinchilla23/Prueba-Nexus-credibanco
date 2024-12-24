import sqlite3
import pandas as pd

# Conectar a la base de datos SQLite
conn = sqlite3.connect('tienda.db')

# Cargar las tablas 'Venta' y 'Almacen' desde la base de datos a DataFrames de pandas
ventas_df = pd.read_sql_query('SELECT idAlmacen, Cantidad, valorUnitario FROM Venta', conn)
almacenes_df = pd.read_sql_query('SELECT idAlmacen, sucursal FROM Almacen', conn)

# Unir las tablas de ventas y almacenes por 'idAlmacen'
datos_combinados = pd.merge(ventas_df, almacenes_df, on='idAlmacen')

# Calcular el total de ventas por registro
datos_combinados['total_venta'] = datos_combinados['Cantidad'] * datos_combinados['valorUnitario']

# Agrupar por sucursal y sumar el total de ventas por sucursal
ventas_por_sucursal = datos_combinados.groupby('sucursal')['total_venta'].sum().reset_index()

# Mostrar el resultado
print(ventas_por_sucursal)

# Cerrar la conexión
conn.close()
