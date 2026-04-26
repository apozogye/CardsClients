# Proyecto Cards Clients - Carga de Datos con Docker, SFTP, RabbitMQ y PostgreSQL

**Autor:** Alex Giovanny Pozo Pachar  
**Fecha:** Abril 24 de 2026

---

## Descripción del proyecto

Este proyecto implementa un flujo de carga de datos desde un archivo CSV hacia una base de datos PostgreSQL, utilizando una arquitectura basada en servicios Docker.

El archivo `cardsclients.csv` se encuentra alojado en un servicio SFTP simulado mediante Docker. Posteriormente, un proceso productor lee el archivo desde el SFTP y envía cada registro a una cola de RabbitMQ. Finalmente, un proceso consumidor lee los mensajes de RabbitMQ, valida la información y registra los datos válidos en PostgreSQL.

---

## Arquitectura del flujo

```text
cardsclients.csv
      |
      v
Servicio SFTP Docker
      |
      v
producer.py
      |
      v
RabbitMQ
      |
      v
consumer.py
      |
      v
PostgreSQL

Requisitos previos

Antes de ejecutar el proyecto, asegúrese de tener instalado:

Docker
Docker Compose
Python 3
pip

Pasos para la ejecucion del proyecto

Paso 1  ejecutamos el siguiente comando para Levantar 1 contenedor con 3 imagenes Docker 
    BD poc_postgres  
    RabbitMQ poc_rabbitMQ
    SFTP poc_sftp

docker compose down -v  (Por si me salio mal a la primera stop y remover imagenes)
docker-compose up -d    (Crear contenedor con Imagenes)

--
docker ps   (Listar Imagenes)

Paso 2  Instalar dependencias segun archivo Requirements.txt
pip install -r requirements.txt

Paso 3  en Powershell Creamos la tabla en Postgres cards_clients

Get-Content schema.sql | docker exec -i poc_postgres psql -U postgres -d cardsdb

docker exec -it poc_postgres psql -U postgres -d cardsdb
select count(*) from cards_clients;
\q salir


Paso 4 En una terminal ejecutamos el proceso del productor
python producer.py

El productor realiza las siguientes acciones:
Se conecta al servicio SFTP.
Lee el archivo cardsclients.csv.
Convierte cada registro del archivo en un mensaje JSON.
Envía los mensajes a la cola cardsclients_queue de RabbitMQ.


Paso 5 En una terminal ejecutamos el proceso del consumidor
python consumer.py

El consumidor realiza las siguientes acciones:

Lee los mensajes desde RabbitMQ.
Valida la información de cada registro.
Inserta los registros válidos en PostgreSQL.
Genera un log de procesamiento por cada carga completa de mensajes.
Continúa activo esperando nuevos mensajes en la cola.
Cada vez que la cola queda vacía, se genera un archivo de log en la carpeta: logs
El nombre del archivo incluye fecha, hora y microsegundos para evitar sobrescrituras en los logs

Monitoreo de RabbitMQ
RabbitMQ cuenta con una consola web disponible en:  http://localhost:15672
Credenciales de acceso:
Usuario	Clave
guest	guest
