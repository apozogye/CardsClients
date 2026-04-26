import os
import json
import pika
import psycopg2
from datetime import datetime

QUEUE_NAME = "cardsclients_queue"
LOG_DIR = "logs"

log_file_path = None
lote_activo = False

total_procesados = 0
total_insertados = 0
total_errores = 0
total_ya_existian = 0


def reset_counters():
    global total_procesados, total_insertados, total_errores, total_ya_existian

    total_procesados = 0
    total_insertados = 0
    total_errores = 0
    total_ya_existian = 0


def create_log_file():
    global log_file_path, lote_activo

    os.makedirs(LOG_DIR, exist_ok=True)

    fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S_%f")    
    log_file_path = os.path.join(LOG_DIR, f"log_carga_{fecha_hora}.txt")

    with open(log_file_path, "w", encoding="utf-8") as log:
        log.write("LOG DE CARGA DE CARDSCLIENTS\n")
        log.write("--------------------------------------\n")
        log.write(f"Fecha inicio carga: {datetime.now()}\n")
        log.write(f"Cola RabbitMQ: {QUEUE_NAME}\n")
        log.write("--------------------------------------\n\n")

    lote_activo = True
    print(f"Log creado para nueva carga: {log_file_path}")


def write_log_line(row_id, estado, detalle=None):
    global log_file_path

    if log_file_path is None:
        create_log_file()

    with open(log_file_path, "a", encoding="utf-8") as log:
        log.write(f"Fecha proceso: {datetime.now()}\n")
        log.write(f"ID procesado: {row_id}\n")
        log.write(f"Estado: {estado}\n")

        if detalle:
            log.write(f"Detalle: {detalle}\n")

        log.write("--------------------------------------\n")


def write_log_summary():
    global log_file_path

    if log_file_path is None:
        return

    with open(log_file_path, "a", encoding="utf-8") as log:
        log.write("\nRESUMEN DE CARGA\n")
        log.write("--------------------------------------\n")
        log.write(f"Fecha fin carga: {datetime.now()}\n")
        log.write(f"Total procesados: {total_procesados}\n")
        log.write(f"Total insertados: {total_insertados}\n")
        log.write(f"Total ya existían: {total_ya_existian}\n")
        log.write(f"Total con errores: {total_errores}\n")
        log.write("--------------------------------------\n")

    print(f"Resumen escrito en log: {log_file_path}")


def close_current_batch_log():
    global log_file_path, lote_activo

    if lote_activo:
        write_log_summary()
        print(f"Carga finalizada. Log generado en: {log_file_path}")

    log_file_path = None
    lote_activo = False
    reset_counters()


def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="cardsdb",
        user="postgres",
        password="postgres",
        port="5432"
    )


def to_number(value):
    return float(value)


def validate_record(row):
    """
    Validaciones:
    - El valor de la factura a pagar no puede ser menor o igual a cero.
    - El valor pagado no puede ser menor o igual a cero.
    """

    bill_fields = ["BILL_1", "BILL_2", "BILL_3", "BILL_4", "BILL_5", "BILL_6"]
    pay_fields = ["VPAY_1", "VPAY_2", "VPAY_3", "VPAY_4", "VPAY_5", "VPAY_6"]

    for field in bill_fields:
        if to_number(row[field]) <= 0:
            return False, f"{field} Valor de Factura tiene valor inválido: {row[field]}"

    for field in pay_fields:
        if to_number(row[field]) <= 0:
            return False, f"{field} Valor Pagado tiene valor inválido: {row[field]}"

    return True, None


def insert_record(cursor, row):
    sql = """
        INSERT INTO cards_clients (
            id, limit_bal, sex, education, marriage, age,
            npay_1, npay_2, npay_3, npay_4, npay_5, npay_6,
            bill_1, bill_2, bill_3, bill_4, bill_5, bill_6,
            vpay_1, vpay_2, vpay_3, vpay_4, vpay_5, vpay_6,
            default_payment_next_month
        )
        VALUES (
            %(ID)s, %(LIMIT_BAL)s, %(SEX)s, %(EDUCATION)s, %(MARRIAGE)s, %(AGE)s,
            %(NPAY_1)s, %(NPAY_2)s, %(NPAY_3)s, %(NPAY_4)s, %(NPAY_5)s, %(NPAY_6)s,
            %(BILL_1)s, %(BILL_2)s, %(BILL_3)s, %(BILL_4)s, %(BILL_5)s, %(BILL_6)s,
            %(VPAY_1)s, %(VPAY_2)s, %(VPAY_3)s, %(VPAY_4)s, %(VPAY_5)s, %(VPAY_6)s,
            %(DEFAULT_PAYMENT_NEXT_MONTH)s
        )
        ON CONFLICT (id) DO NOTHING;
    """

    data = {
        "ID": int(row["ID"]),
        "LIMIT_BAL": float(row["LIMIT_BAL"]),
        "SEX": int(row["SEX"]),
        "EDUCATION": int(row["EDUCATION"]),
        "MARRIAGE": int(row["MARRIAGE"]),
        "AGE": int(row["AGE"]),

        "NPAY_1": int(row["NPAY_1"]),
        "NPAY_2": int(row["NPAY_2"]),
        "NPAY_3": int(row["NPAY_3"]),
        "NPAY_4": int(row["NPAY_4"]),
        "NPAY_5": int(row["NPAY_5"]),
        "NPAY_6": int(row["NPAY_6"]),

        "BILL_1": float(row["BILL_1"]),
        "BILL_2": float(row["BILL_2"]),
        "BILL_3": float(row["BILL_3"]),
        "BILL_4": float(row["BILL_4"]),
        "BILL_5": float(row["BILL_5"]),
        "BILL_6": float(row["BILL_6"]),

        "VPAY_1": float(row["VPAY_1"]),
        "VPAY_2": float(row["VPAY_2"]),
        "VPAY_3": float(row["VPAY_3"]),
        "VPAY_4": float(row["VPAY_4"]),
        "VPAY_5": float(row["VPAY_5"]),
        "VPAY_6": float(row["VPAY_6"]),

        "DEFAULT_PAYMENT_NEXT_MONTH": int(row["default_payment_next_month"])
    }

    cursor.execute(sql, data)
    return cursor.rowcount


def queue_is_empty(channel):
    queue_state = channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        passive=True
    )

    pending_messages = queue_state.method.message_count

    return pending_messages == 0


def callback(ch, method, properties, body):
    global total_procesados, total_insertados, total_errores, total_ya_existian

    conn = None
    cursor = None

    if not lote_activo:
        reset_counters()
        create_log_file()

    try:
        row = json.loads(body)
        row_id = row.get("ID", "SIN_ID")

        total_procesados += 1

        print(f"Procesando mensaje ID: {row_id}")

        is_valid, error_message = validate_record(row)

        if not is_valid:
            total_errores += 1

            write_log_line(
                row_id=row_id,
                estado="ERROR_VALIDACION",
                detalle=error_message
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

            if queue_is_empty(ch):
                close_current_batch_log()

            return

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            affected_rows = insert_record(cursor, row)

            conn.commit()

            if affected_rows == 1:
                total_insertados += 1

                write_log_line(
                    row_id=row_id,
                    estado="INSERTADO",
                    detalle="Registro insertado correctamente en PostgreSQL."
                )

                print(f"Registro insertado ID: {row_id}")

            else:
                total_ya_existian += 1

                write_log_line(
                    row_id=row_id,
                    estado="YA_EXISTE",
                    detalle="El registro ya existía en PostgreSQL. No se insertó por ON CONFLICT (id) DO NOTHING."
                )

                print(f"Registro ya existente ID: {row_id}")

        except Exception as e:
            total_errores += 1

            if conn:
                conn.rollback()

            write_log_line(
                row_id=row_id,
                estado="ERROR_BD",
                detalle=str(e)
            )

            print(f"Error BD en ID {row_id}: {e}")

        finally:
            if cursor:
                cursor.close()

            if conn:
                conn.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if queue_is_empty(ch):
            close_current_batch_log()

    except json.JSONDecodeError as e:
        total_procesados += 1
        total_errores += 1

        write_log_line(
            row_id="JSON_INVALIDO",
            estado="ERROR_JSON",
            detalle=str(e)
        )

        print(f"Error JSON: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if queue_is_empty(ch):
            close_current_batch_log()

    except Exception as e:
        total_errores += 1

        write_log_line(
            row_id="ERROR_GENERAL",
            estado="ERROR_GENERAL",
            detalle=str(e)
        )

        print(f"Error general procesando mensaje: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if queue_is_empty(ch):
            close_current_batch_log()


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()

channel.queue_declare(
    queue=QUEUE_NAME,
    durable=True
)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue=QUEUE_NAME,
    on_message_callback=callback
)

print("Esperando mensajes desde RabbitMQ. Presione CTRL+C para finalizar.")

try:
    channel.start_consuming()

except KeyboardInterrupt:
    print("Finalizando consumidor...")

    if lote_activo:
        close_current_batch_log()

    connection.close()
    print("Consumidor finalizado.")