# Data Engineering Project:
## Summary
For this project there are 2 sections: 
- Basic task section
- Stretch goals

## Task Description

### Basic Task
1. Downloaded and installed Apache Spark locally on my Mac
2. Downloaded and installed Apache Zeppelin locally on my Mac.	
   - The Zeppelin Spark interpreter references the standalone Spark installation and not the bundled Spark interpreter provided with Zeppelin.
3. Access the TPC-DS data available on my AWS S3 bucket. Details for this dataset are provided [here](#base-dataset-tpcds_data_5g).
   - Data documentation is here: http://tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf
4. Create some basic data visualizations from at least 1 of the input data files.

## Stretch Goals
1. Dockerizing my approach such that all required services can be brought up with a single docker command
2. Implementing the basic task on an AWS EC2 instance.
3. Integrating the basic task into a larger data processing pipeline using Apache Airflow, integrating with Postgres database.
4. Framing the basic task as a transaction-level streaming problem using Apache Kafka. Details for this dataset are given [below](#streaming-dataset-tpcds_data_5g_streaming).
5. Framing the basic task as a periodic batch problem, with new batches of fact and dimension data arriving monthly. End users of reports should be able to view at least 1 data view or visualization in Zeppelin as of an arbitrary past date. Details for this dataset are given [below](#quarterly-batch-dataset).
6. Putting together a basic design for how you might implement the end-to-end process from data sourcing to productionized reporting for hundreds of users in a large enterprise environment.

# Dataset Description

## Base Dataset: tpcds_data_5g
The TPC-DS dataset is a benchmark dataset used to benchmark the performance of various big data tooling on specific queries.
Documentation for this dataset is available here: \
http://tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf

Used a cut-down version of the dataset to reduce the total data volume. This reduced dataset is labelled `tpcds_data_5g` on my S3 bucket.
With all tables included, this data forms a snowflake schema.

Files available:
```bash
.
├── ddl
│   └── create_tables.sql
├── call_center.dat
├── catalog_page.dat
├── catalog_returns.dat
├── catalog_sales.dat
├── customer.dat
├── customer_address.dat
├── customer_demographics.dat
├── date_dim.dat
├── dbgen_version.dat
├── household_demographics.dat
├── hs_err_pid93510.log
├── income_band.dat
├── inventory.dat
├── item.dat
├── output
├── promotion.dat
├── reason.dat
├── ship_mode.dat
├── store.dat
├── store_returns.dat
├── store_sales.dat
├── time_dim.dat
├── warehouse.dat
├── web_page.dat
├── web_returns.dat
├── web_sales.dat
└── web_site.dat
```

## Streaming Dataset: tpcds_data_5g_streaming
To simulate a streaming use case, I used a cut-down version of the base dataset focused on the single fact table store_sales and associated dimension tables, which together form their own star schema.
Within the dataset folder `tpcds_data_5g_streaming`, you will find 2 subfolders containing:
1. `initial_load_upto_2002`: an initial batch of the store_sales fact table along with the associated dimension tables
2. `streaming_data_after_2002`: a streaming folder containing new records for the store_sales table: each row of each file should be considered a new event:
```bash
tpcds_data_5g_streaming
├── initial_load_upto_2002
│   ├── customer.dat
│   ├── customer_address.dat
│   ├── customer_demographics.dat
│   ├── date_dim.dat
│   ├── household_demographics.dat
│   ├── item.dat
│   ├── promotion.dat
│   ├── store.dat
│   ├── store_sales.dat
│   └── time_dim.dat
└── streaming_data_after_2002
    ├── store_sales_2003-01-01.dat
    └── store_sales_2003-01-02.dat
```

## Quarterly Batch Dataset: 
The dataset `tpcds_data_5g_batch` has been constructed to simulate a quarterly batch of both fact and dimension data for the `store_sales` schema.
Each batch directory contains the new `store_sales` transactions for that time period (inserts only), along with any inserts and updates to the
associated dimensions tables within that time period.

```bash
├── batch_1998-04-01
│   ├── customer
│   │   ├── _SUCCESS
│   │   └── part-00000-589162dd-e263-440a-8ded-537f2908a11b-c000.csv
│   ├── customer_address
│   │   ├── _SUCCESS
│   │   └── part-00000-1a3c92a8-2532-4761-8dfd-c27040d2b29a-c000.csv
│   ├── date_dim
│   │   ├── _SUCCESS
│   │   └── part-00000-c56b7fe2-8b14-4172-8403-feeab0521105-c000.csv
│   ├── household_demographics
│   │   ├── _SUCCESS
│   │   └── part-00000-ef48775b-de62-4325-94e6-a749f03db0f1-c000.csv
│   ├── item
│   │   ├── _SUCCESS
│   │   └── part-00000-e9ff2eeb-2806-4426-8db8-d95a0c836bd8-c000.csv
│   ├── promotion
│   │   ├── _SUCCESS
│   │   └── part-00000-b97444e0-d4b7-45cc-bfda-8c642c08fa55-c000.csv
│   ├── store
│   │   ├── _SUCCESS
│   │   └── part-00000-8b82cce7-e8a5-47bd-9fb6-90d822a69456-c000.csv
│   ├── store_sales
│   │   ├── _SUCCESS
│   │   └── part-00000-86d9d885-0315-4211-927d-8db564f90f07-c000.csv
│   └── time_dim
│       ├── _SUCCESS
│       └── part-00000-deef998c-b5f2-46a5-91bd-f955913f8f80-c000.csv
...
└── ddl
    └── create_tables.sql

```
## Prerequisites
Before installing Spark and Zeppelin on a Mac, you should make sure that you have the following prerequisites:
- A Java Development Kit (JDK) version 8 or higher installed on your system. You can check if you have JDK installed by running the java -version command in a terminal window. If you don't have JDK installed, you can download and install it from the Oracle website (https://www.oracle.com/java/technologies/javase-downloads.html).
- A text editor or integrated development environment (IDE) installed on your system. You will need this to edit configuration files and write code. Some popular options include Visual Studio Code, Eclipse, and IntelliJ IDEA.

## Install Apache Spark 3.1.3 and Apache Zeppelin 0.10.1 on Mac

1. Download version 3.1.3 of Apache Spark from the official website (https://archive.apache.org/dist/spark/spark-3.1.3/). You can choose the package type "spark-3.1.3-bin-hadoop3.2.tgz".

2. Extract the downloaded tarball by running the following command in the terminal:

```bash
tar xvf spark-3.1.3-bin-hadoop3.2.tgz
```

3. Set the SPARK_HOME environment variable to the extracted Spark directory by adding the following line to your .bash_profile file:
```bash
export SPARK_HOME=/path/to/spark-3.1.3-bin-hadoop3.2
```

4. Add the Spark bin directory to your PATH by adding the following line to your .bash_profile file:
```bash
export PATH=$SPARK_HOME/bin:$PATH
```

5. Download the latest version of Apache Zeppelin from the official website (https://zeppelin.apache.org/download.html).

6. Extract the downloaded tarball by running the following command in the terminal:
```bash
tar xvf zeppelin-0.10.1-bin-all.tgz
```

7. Set the ZEPPELIN_HOME environment variable to the extracted Zeppelin directory by adding the following line to your .bash_profile file:
```bash
export ZEPPELIN_HOME=/path/to/zeppelin-0.10.1-bin-all
```

8. Add the Zeppelin bin directory to your PATH by adding the following line to your .bash_profile file:
```bash
export PATH=$ZEPPELIN_HOME/bin:$PATH
```

### Configure Zeppelin
- Navigate to the conf directory in the extracted zeppelin directory at path/to/zeppelin/zeppelin-0.6.2-bin-all/conf
- copy this file to a new name called zeppelin-env.sh using the following commands:
```
cd /path/to/zeppelin-0.6.2-bin-all/conf
cp zeppelin-env.sh.template zeppelin-env.sh
```
- Edit the zeppelin-env.sh file by adding this line to the very top of the file.
```
export SPARK_HOME=/path/to/spark/spark-3.1.3-bin-hadoop3.2
```
- Save and close the zeppelin-env.sh file.
- Edit the zeppellin-site.xml file by setting the value of ```zeppelin.server.port``` to ```8085```.
- Save and close the zeppellin-site.xml file.

### Starting Spark
Open the terminal and navigate to the Spark directory.

- Run the following command to start Spark:
```bash
./sbin/start-all.sh 
```

### Starting Zeppelin
Open the terminal and navigate to the Zeppelin directory.

- Run the following command to start Zeppelin:
```bash
./bin/zeppelin-daemon.sh start
```

Open your web browser and go to ```http://localhost:8085``` to access the Zeppelin web interface.

That's it! You should now have Apache Spark 3.1.3 and Apache Zeppelin installed and running on your Mac

## Airflow
You'll need to create a connection to the Zeppelin server in the Airflow UI using the zeppelin_conn connection ID.
You can do this by going to the Admin > Connections page in the Airflow UI and adding a new connection with the following details:
* Conn Id: zeppelin_conn
* Conn Type: HTTP
* Host: http://localhost:8085

You'll also need to make sure that the Airflow webserver has access to the Zeppelin server.
You can do this by adding the IP address of the Airflow webserver to the zeppelin.server.allowed.origins property in the Zeppelin configuration.
Once you've set up the connection and the DAG, you can trigger the DAG using the Airflow UI or the airflow command-line tool.

## Kafka

* Start the ZooKeeper service

    ```bin/zookeeper-server-start.sh config/zookeeper.properties```

* Start the Kafka broker service

    ```bin/kafka-server-start.sh config/server.properties```

* Create topics

    ```bin/kafka-topics.sh --create --topic <topic name> --bootstrap-server localhost:9092```
* list the Kafka topics

    ```./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list```

* write something into the topic

    ```bin/kafka-console-producer.sh --topic <topic name> --bootstrap-server localhost:9092```

* read the event

    ```bin/kafka-console-consumer.sh --topic <topic name> --from-beginning --bootstrap-server localhost:9092```

* delete any data of your local Kafka environment including any events you have created along the way, run the command

    ```rm -rf /tmp/kafka-logs /tmp/zookeeper```
* Delete a Kafka Topic

  ```./bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic <topic name>```

## Author
Cllifford Frempong
