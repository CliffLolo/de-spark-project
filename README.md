# Data Engineering Challenge
This repository contains the code for a data engineering take-home project from bigspark.

## Prerequisites
Before installing Spark and Zeppelin on a Mac, you should make sure that you have the following prerequisites:
- A Java Development Kit (JDK) version 8 or higher installed on your system. You can check if you have JDK installed by running the java -version command in a terminal window. If you don't have JDK installed, you can download and install it from the Oracle website (https://www.oracle.com/java/technologies/javase-downloads.html).
- A text editor or integrated development environment (IDE) installed on your system. You will need this to edit configuration files and write code. Some popular options include Visual Studio Code, Eclipse, and IntelliJ IDEA.

## Data
The data for this project is provided as part of the takehome challenge and is stored in an AWS S3 bucket.

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

## Author
Cllifford Frempong
