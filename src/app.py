import threading
from tkinter import Tk, Label
from PIL import Image, ImageTk
from BD.Connection import get_connection, get_connection_load

# Crear las conexiones
connection = get_connection()
connectionLoad = get_connection_load()
# Crear los cursores de las conexiones
cursor = connection.cursor()
cursorLoad = connectionLoad.cursor()


# Carga y muestra el GIF en una ventana, genera los frames uno a uno del GIF
def show_gif(root, label, frames, stop_event):
    def update_gif(frame_index):
        if not stop_event.is_set():
            label.config(image=frames[frame_index])
            root.update()
            root.after(100, update_gif, (frame_index + 1) % len(frames))
        else:
            root.quit()  # Cerrar la ventana cuando se complete
    update_gif(0)


# Función que me devuelve los datos de las fechas para la tabla dimFechas
def extract_dates(cursor):
    cursor.execute("SELECT date_sale, date_part('month', date_sale), date_part('year', date_sale) FROM sale ORDER BY 1;")
    dates = cursor.fetchall()
    return dates

# Función que me devuelve los datos del cliente para dimClientes
def extract_clients(cursor):
    cursor.execute('SELECT first_name, last_name, job_title, gender, college FROM client;')
    clients = cursor.fetchall()
    return clients


# Función que me devuelve los datos de las tarjetas para dimCartas, EVITA NOMBRES DE TARJETAS REPETIDAS
def extract_cards(cursor):
    cursor.execute('SELECT DISTINCT card FROM card;')
    cards = cursor.fetchall()
    return cards


# Función que extrae los datos necesarios para la tabla de factVentas
def extract_sales(cursor):
    cursor.execute("""
        SELECT s.sale_paid, s.articles, cl.id_client, s.date_sale, sp.id_product, c.card
        FROM sale s 
        JOIN card c ON s.id_card = c.id_card 
        JOIN client cl ON cl.id_client = c.id_client 
        JOIN sale_product sp ON sp.id_sale = s.id_sale;
    """)
    sales = cursor.fetchall()
    return sales


# Inserción de la tabla de dimensiones para las fechas con un nuevo id
def insert_dates(cursorLoad, dates):
    new_id = 1
    for date in dates:
        cursorLoad.execute("""
            INSERT INTO dimfechas (idfecha, fecha, mes, anio)
            VALUES (%s, %s, %s, %s);
        """, (new_id, date[0], date[1], date[2]))
        new_id += 1
        
    connectionLoad.commit()


# Inserción de datos de clientes con un nuevo id, client map almacena en un diccionario el id orginal respecto al nuevo id, para despues
# poder relacionarlos e insertar correctamente el registro en factVentas con ese nuevo id del cliente
def insert_clients(cursorLoad, cursor, clients):
    new_id = 1
    client_map = {}
    for client in clients:
        cursorLoad.execute("""
            INSERT INTO dimclientes (idcliente, trabajo, genero, colegio)
            VALUES (%s, %s, %s, %s);
        """, (new_id, client[2], client[3], client[4]))
        
        cursor.execute('SELECT id_client FROM client WHERE first_name = %s AND last_name = %s;', (client[0], client[1]))
        original_id = cursor.fetchone()
        client_map[original_id[0]] = new_id
        new_id += 1
    connectionLoad.commit()
    return client_map


# Inserción para datos de tarjetas/cartas con nuevo id
def insert_cards(cursorLoad, cards):
    new_id = 1
    for card in cards:
        cursorLoad.execute("""
            INSERT INTO dimcartas (idcarta, carta)
            VALUES (%s, %s);
        """, (new_id, card[0]))
        new_id += 1
    connectionLoad.commit()


# Inserción en la tabla de hechos
def insert_sales(cursorLoad, sales, client_map):
    for sale in sales:
        # Linea que extrae el nuevo id del cliente con respecto al id original mediante el client_map que se declaro en insert clients
        new_id_client = client_map.get(sale[2])
        cursorLoad.execute("""
            INSERT INTO factventas (venta_pagada, articulos, idcliente, idfecha, idproducto, idcarta)
            VALUES (
                %s,
                %s,
                %s,
                (SELECT idfecha FROM dimfechas WHERE fecha = %s LIMIT 1),
                %s,
                (SELECT idcarta FROM dimcartas WHERE carta = %s)
            );
        """, (sale[0], sale[1], new_id_client, sale[3], sale[4], sale[5]))
    connectionLoad.commit()


# Cierra las conexiones completamente
def close_connection(connection, connectionLoad, cursor, cursorLoad):
    connection.close()
    connectionLoad.close()
    cursor.close()
    cursorLoad.close()


# Función para realizar el proceso de inserción
def run_insertions(stop_event):
    dates = extract_dates(cursor)
    clients = extract_clients(cursor)
    cards = extract_cards(cursor)
    sales = extract_sales(cursor)

    # Inserción en tablas de dimensiones
    insert_dates(cursorLoad, dates)
    client_map = insert_clients(cursorLoad, cursor, clients)
    insert_cards(cursorLoad, cards)

    # Inserción en tabla de hechos (factVentas)
    insert_sales(cursorLoad, sales, client_map)

    # Cierra las conexiones completamente
    close_connection(connection, connectionLoad, cursor, cursorLoad)

    stop_event.set()  # Indica que el proceso ha terminado


# Función principal para iniciar la ventana con el GIF y las inserciones
def start_gui_and_process(gif_path):
    stop_event = threading.Event()  # Evento para detener el GIF
    root = Tk()
    root.title("Cargando...")

    # Cargar el GIF
    img = Image.open(gif_path)
    frames = []
    for frame_index in range(0, img.n_frames):
        img.seek(frame_index)
        frame = ImageTk.PhotoImage(img.copy().convert("RGBA"))
        frames.append(frame)
    label = Label(root)
    label.pack()

    # Iniciar el hilo para las inserciones de datos
    insert_thread = threading.Thread(target=run_insertions, args=(stop_event,))
    insert_thread.start()

    # Iniciar el GIF en el hilo principal
    show_gif(root, label, frames, stop_event)

    root.mainloop()


if __name__ == '__main__':
    gif_path = 'src/cell.gif'  
    start_gui_and_process(gif_path)
