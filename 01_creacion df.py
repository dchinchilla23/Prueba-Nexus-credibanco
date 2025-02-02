import sqlite3
### genreacion conexion tablas y creaCION DE DATOS E INSERSION 

# Conectar a la base de datos SQLite (se crea automáticamente si no existe)
conn = sqlite3.connect('tienda.db')

# Crear un cursor para ejecutar las consultas SQL
cursor = conn.cursor()

# Crear las tablas (schema) en la base de datos
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Venta (
        idFactura INTEGER PRIMARY KEY,
        idAlmacen INTEGER NOT NULL,
        fecha TEXT NOT NULL,
        idCliente INTEGER NOT NULL,
        Cantidad INTEGER NOT NULL,
        valorUnitario INTEGER NOT NULL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Almacen (
        idAlmacen INTEGER PRIMARY KEY,
        nombreAlmacen TEXT NOT NULL,
        sucursal TEXT NOT NULL,
        direccion TEXT NOT NULL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Cliente (
        idCliente INTEGER PRIMARY KEY,
        nombreCliente TEXT NOT NULL,
        direccion TEXT NOT NULL,
        ciudad TEXT NOT NULL
    )
''')

# Confirmar la creación de las tablas
conn.commit()

# Insertar los datos en las tablas
cursor.execute('''
    INSERT INTO Venta (idFactura, idAlmacen, fecha, idCliente, Cantidad, valorUnitario) VALUES
    (1, 1, '2024-12-01 10:00:00', 1, 5, 200000),
    (2, 2, '2024-12-02 11:00:00', 2, 3, 150000),
    (3, 1, '2024-12-03 12:00:00', 3, 7, 250000),
    (4, 3, '2024-12-04 14:00:00', 4, 2, 180000),
    (5, 2, '2024-12-05 15:00:00', 5, 10, 120000),
    (6, 1, '2024-12-06 16:00:00', 6, 6, 210000),
    (7, 3, '2024-12-07 17:00:00', 7, 4, 220000),
    (8, 2, '2024-12-08 18:00:00', 8, 8, 200000),
    (9, 1, '2024-12-09 19:00:00', 9, 5, 190000),
    (10, 3, '2024-12-10 20:00:00', 10, 3, 230000);
''')

cursor.execute('''
    INSERT INTO Almacen (idAlmacen, nombreAlmacen, sucursal, direccion) VALUES
    (1, 'Almacen Central', 'Sucursal A', 'Calle 100 #10-10, Bogotá'),
    (2, 'Almacen Sur', 'Sucursal B', 'Carrera 50 #20-30, Medellín'),
    (3, 'Almacen Norte', 'Sucursal C', 'Avenida 80 #30-40, Cali'),
    (4, 'Almacen Occidente', 'Sucursal D', 'Calle 70 #40-50, Barranquilla'),
    (5, 'Almacen Oriente', 'Sucursal E', 'Carrera 10 #50-60, Cartagena'),
    (6, 'Almacen Capital', 'Sucursal F', 'Avenida 60 #60-70, Bucaramanga'),
    (7, 'Almacen Zona Este', 'Sucursal G', 'Calle 40 #70-80, Pereira'),
    (8, 'Almacen Costa', 'Sucursal H', 'Carrera 20 #80-90, Santa Marta'),
    (9, 'Almacen Centro', 'Sucursal I', 'Avenida 30 #90-100, Manizales'),
    (10, 'Almacen Región', 'Sucursal J', 'Calle 90 #100-110, Pasto');
''')

cursor.execute('''
    INSERT INTO Cliente (idCliente, nombreCliente, direccion, ciudad) VALUES
    (1, 'Juan Pérez', 'Calle 10 #5-6', 'Bogotá'),
    (2, 'Ana Gómez', 'Carrera 20 #10-11', 'Medellín'),
    (3, 'Carlos Rodríguez', 'Avenida 30 #15-16', 'Cali'),
    (4, 'Luis Martínez', 'Calle 50 #20-21', 'Barranquilla'),
    (5, 'Marta López', 'Carrera 70 #25-26', 'Cartagena'),
    (6, 'Pedro González', 'Avenida 90 #30-31', 'Bucaramanga'),
    (7, 'Sofía Ramírez', 'Calle 100 #35-36', 'Pereira'),
    (8, 'Miguel Sánchez', 'Carrera 120 #40-41', 'Santa Marta'),
    (9, 'Laura Torres', 'Avenida 150 #45-46', 'Manizales'),
    (10, 'Juanita Díaz', 'Calle 180 #50-51', 'Pasto');
''')

# Confirmar la inserción de los datos
conn.commit()

print("Datos insertados correctamente en las tablas.")

# Cerrar la conexión
conn.close()
