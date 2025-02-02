# CDC Inventory Project

This project implements Change Data Capture (CDC) using Debezium, Apache Kafka, and PySpark to track and process changes in a MySQL database.

## Prerequisites

- Docker installed on your system
- Python 3.8 or higher
- pip package manager

## Project Setup

1. Clone the repository and install dependencies:

```bash
git clone https://github.com/your-repo/cdc-inventory.git
cd cdc-inventory
pip install -r requirements.txt
```

2. Create a `.env` file in the project root with your AWS credentials and SO Repository path:
```bash
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
REPO_PATH=your_repository_path
```
## Starting the Infrastructure

### 1. Start ZooKeeper
First, start ZooKeeper in a new terminal:
```bash
docker run -it --rm --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 quay.io/debezium/zookeeper:3.0
```
### 2. Start Kafka
Open a new terminal and start Kafka:
```bash
docker run -it --rm --name kafka -p 9092:9092 --link zookeeper:zookeeper quay.io/debezium/kafka:3.0
```
### 3. Start MySQL
Open a new terminal and start the MySQL server:
```bash
docker run -it --rm --name mysql -p 3306:3306 \
-e MYSQL_ROOT_PASSWORD=debezium \
-e MYSQL_USER=mysqluser \
-e MYSQL_PASSWORD=mysqlpw \
quay.io/debezium/example-mysql:3.0
```
### 4. Start MySQL CLI (Optional)
To interact with MySQL directly:
```bash
docker run -it --rm --name mysqlterm --link mysql mysql:8.2 \
sh -c 'exec mysql -h"$MYSQL_PORT_3306_TCP_ADDR" \
-P"$MYSQL_PORT_3306_TCP_PORT" -uroot -p"$MYSQL_ENV_MYSQL_ROOT_PASSWORD"'
```
### 5. Start Kafka Connect
Open a new terminal and start Kafka Connect:
```bash
docker run -it --rm --name connect -p 8083:8083 \
-e GROUP_ID=1 \
-e CONFIG_STORAGE_TOPIC=my_connect_configs \
-e OFFSET_STORAGE_TOPIC=my_connect_offsets \
-e STATUS_STORAGE_TOPIC=my_connect_statuses \
--link kafka:kafka --link mysql:mysql \
quay.io/debezium/connect:3.0
```
## Configuring CDC

### 1. Deploy MySQL Connector
Register the Debezium MySQL connector:
```bash
curl -i -X POST -H "Accept:application/json" \
-H "Content-Type:application/json" \
localhost:8083/connectors/ -d '{
"name": "inventory-connector",
"config": {
"connector.class": "io.debezium.connector.mysql.MySqlConnector",
"tasks.max": "1",
"database.hostname": "mysql",
"database.port": "3306",
"database.user": "debezium",
"database.password": "dbz",
"database.server.id": "184054",
"topic.prefix": "dbserver1",
"database.include.list": "inventory",
"schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
"schema.history.internal.kafka.topic": "schemahistory.inventory"
}
}'
```
### 2. Watch Topic (Optional)
To monitor changes in the customers table:
```bash
docker run -it --rm --name watcher \
--link zookeeper:zookeeper --link kafka:kafka \
quay.io/debezium/kafka:3.0 watch-topic -a -k dbserver1.inventory.customers
```
## Running the PySpark Application

1. Start the PySpark application to process CDC events:
```bash
bash
python customers.py
```
The application will:
- Connect to Kafka and read CDC events
- Process changes (inserts, updates, deletes)
- Store the results in Delta Lake format

## Monitoring and Troubleshooting

- Check Kafka Connect status: `http://localhost:8083/connectors/`
- Monitor Kafka topics using the watcher container
- Check PySpark logs for processing status
- Delta Lake tables are stored in the specified S3 location

## Notes

- The project uses Delta Lake for reliable data storage
- CDC events are processed in real-time
- All changes to the customers table are captured and reflected in the Delta Lake table
- Make sure to properly configure AWS credentials for S3 access

