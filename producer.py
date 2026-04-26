import csv
import json
import pika
import paramiko
from io import StringIO

SFTP_HOST = "localhost"
SFTP_PORT = 2222
SFTP_USER = "sftpuser"
SFTP_PASSWORD = "sftppass"
SFTP_FILE_PATH = "/upload/cardsclients.csv"

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "cardsclients_queue"


def read_csv_from_sftp():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)

    sftp = paramiko.SFTPClient.from_transport(transport)

    with sftp.open(SFTP_FILE_PATH, "r") as remote_file:
        content = remote_file.read().decode("utf-8")

    sftp.close()
    transport.close()

    return content


csv_content = read_csv_from_sftp()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST)
)

channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)

reader = csv.DictReader(StringIO(csv_content))

total_sent = 0

for row in reader:
    message = json.dumps(row)

    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)
    )

    total_sent += 1

print(f"Total de registros enviados a RabbitMQ: {total_sent}")

connection.close()