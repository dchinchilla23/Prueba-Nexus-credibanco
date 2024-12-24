import sqlite3
import pandas as pd
from datetime import datetime

# Conectar a la base de datos SQ Lite
conn = sqlite3.connect('tienda.db')

# Cargar las tablas 'Venta' y 'Cliente' desde la base de datos a DataFrames de pandas 
ventas_df = pd.read_sql_query('SELECT idAlmacen, Cantidad, valorUnitario, fecha, idCliente FROM Venta', conn)
clientes_df = pd.read_sql_query('SELECT idCliente, nombreCliente FROM Cliente', conn)

# Unir las tablas de ventas y clientes por 'idCliente'
ventas_actual = pd.merge(ventas_df, clientes_df, on='idCliente')

# Calcular el total de ventas por registro
ventas_actual['total_venta'] = ventas_actual['Cantidad'] * ventas_actual['valorUnitario']

# Agrupar por cliente y sumar las ventas
ventas_cliente = ventas_actual.groupby(['idCliente', 'nombreCliente'])['total_venta'].sum().reset_index()

# Renombrar las columnas para claridad
ventas_cliente.columns = ['idCliente', 'nombreCliente', 'total_ventas']

# Obtener el cliente con mayor facturacion
cliente_top = ventas_cliente.loc[ventas_cliente['total_ventas'].idxmax()]

# Mostrar el cliente con mayor facturacion
print(cliente_top)

# Cerrar la conexión
conn.close()
