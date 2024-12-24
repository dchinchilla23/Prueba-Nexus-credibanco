import pyodbc
import pandas as pd

# Conectar a la base de datos Hive usando ODBC
conn = pyodbc.connect('DSN=HiveDSN;UID=username;PWD=password')

# Definir la consulta SQL
query = '''
SELECT 
    terminal,  
    codigo_unico,  
    version_contenedor,  
    date_monitoring,  
    fecha_carga,  
    '' AS serie
FROM (
    SELECT 
        terminal, 
        codigo_unico, 
        version_contenedor, 
        date_monitoring, 
        fecha_carga,
        ROW_NUMBER() OVER (
            PARTITION BY terminal, codigo_unico 
            ORDER BY date_monitoring DESC
        ) AS ranker
    FROM cld_bi_operacion_eng.microservicios_versiones_t
    WHERE 
        date_monitoring >= (SELECT fecha_inicio FROM cld_bi_operacion_eng.fecha_inicio_2_prueba)
) ranked_data
WHERE ranker = 1;
'''

# Ejecutar la consulta y cargar los datos en un DataFrame de pandas
df = pd.read_sql_query(query, conn)

# Mostrar el resultado
print(df)

# Cerrar la conexión
conn.close()