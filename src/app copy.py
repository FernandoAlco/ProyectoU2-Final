'''
    App copy esta destinado a hacer pruebas, nada de lo que se encuentra aqui es correcto para el DWH
'''

from BD.Connection import get_connection, get_connection_load

connection = get_connection()
connectionLoad = get_connection_load()
cursor = connection.cursor()
cursorLoad = connectionLoad.cursor()


# def extract_dates(cursor):
#     months = {
#         1: 'Enero',
#         2: 'Febrero',
#         3: 'Marzo',
#         4: 'Abril',
#         5: 'Mayo',
#         6: 'Junio',
#         7: 'Julio',
#         8: 'Agosto',
#         9: 'Septiembre',
#         10: 'Octubre',
#         11: 'Noviembre',
#         12: 'Diciembre'
#     }
#     cursor.execute("SELECT DISTINCT date_sale, date_part('month', date_sale), date_part('year', date_sale) FROM sale;")
#     dates = cursor.fetchall()
#     dates = [(date[0], months[int(date[1])], int(date[2])) for date in dates]
#     return dates


# Funcion que me devuelve una tupla con id's generados para las fechas, la fecha, el mes y el año
def extract_dates(cursor):
    cursor.execute("SELECT DISTINCT row_number() OVER (ORDER BY date_sale), date_sale, date_part('month', date_sale), date_part('year', date_sale) FROM sale ORDER BY 1;")
    dates = cursor.fetchall()
    return dates


# Funcion que me devuelve los datos del cliente
def extract_clients(cursor):
    cursor.execute('SELECT id_client, job_title, gender, college FROM client ORDER BY 1;')
    clients = cursor.fetchall()
    return clients


# Funcion que me devuelve los datos de la tarjeta
def extract_cards(cursor):
    cursor.execute('SELECT id_card, card FROM card ORDER BY 1;')
    cards = cursor.fetchall()
    return cards


# Funcion que extrae los datos necesarios para la tabla de hechos
def extract_sales(cursor):
    cursor.execute("""
        SELECT s.sale_paid, s.articles, c.id_client, s.date_sale, sp.id_product, s.id_card 
        FROM sale s 
        JOIN card c ON s.id_card = c.id_card 
        JOIN client cl ON cl.id_client = c.id_client 
        JOIN sale_product sp ON sp.id_sale = s.id_sale;
        """)
    sales = cursor.fetchall()
    return sales


# Insercion de la tabla de dimensiones para las fechas
def insert_dates(cursorLoad, dates):
    for date in dates:
        cursorLoad.execute("""
            INSERT INTO dimfechas (idfecha, fecha, mes, anio)
            VALUES (%s, %s, %s, %s);
        """, (date[0], date[1], date[2], date[3]))
    connectionLoad.commit()


# Insercion de datos de clientes
def insert_clients(cursorLoad, clients):
    for client in clients:
        cursorLoad.execute("""
            INSERT INTO dimclientes (idcliente, trabajo, genero, colegio)
            VALUES (%s, %s, %s, %s);
        """, (client[0], client[1], client[2], client[3]))
    connectionLoad.commit()


# Insercion para datos de tarjetas/cartas
def insert_cards(cursorLoad, cards):
    for card in cards:
        cursorLoad.execute("""
            INSERT INTO dimcartas (idcarta, carta)
            VALUES (%s, %s);
        """, (card[0], card[1]))
    connectionLoad.commit()


# Insercion en la tabla de hechos, la sub-consulta busca en la tabla de dimFechas una fecha igual al valor que se recogio de
# la extraccion de Ventas, por lo tanto, ese sera el idfecha para ese registro.
# Usamos Limit 1, ya que hay fechas que se repiten, pero al fin y al cabo solo ocupamos una
def insert_sales(cursorLoad, sales):
    for sale in sales:
        cursorLoad.execute("""
            INSERT INTO factventas (venta_pagada, articulos, idcliente, idfecha, idproducto, idcarta)
            VALUES (
                %s,
                %s,
                %s,
                (SELECT idfecha FROM dimfechas WHERE fecha = %s LIMIT 1),
                %s,
                %s
            );
        """, (sale[0], sale[1], sale[2], sale[3], sale[4], sale[5]))
    connectionLoad.commit()


# Cierra las conexiones completamente
def close_connection():
    connection.close()
    connectionLoad.close()
    cursor.close()
    cursorLoad.close()
    

# Borra todo del data warehouse, este metodo no es correcto, sin embargo, esta por fines practicos
def dont_do_this(cursorLoad):
    cursorLoad.execute('DELETE FROM factventas;')
    cursorLoad.execute('DELETE FROM dimcartas;')
    cursorLoad.execute('DELETE FROM dimclientes;')
    cursorLoad.execute('DELETE FROM dimfechas;')
    connectionLoad.commit()


if __name__ == '__main__':
    # dates = extract_dates(cursor)
    # clients = extract_clients(cursor)
    # cards = extract_cards(cursor)
    # sales = extract_sales(cursor)
    # insert_dates(cursorLoad, dates)
    # insert_clients(cursorLoad, clients)
    # insert_cards(cursorLoad, cards)
    # insert_sales(cursorLoad, sales)
    dont_do_this(cursorLoad)