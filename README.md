# Project3
## Project Description
* Using Kafka and SparkSQL / DataFrames, process data streams of "Order Placements & Transactions" data. The data is to be generated on the fly (no file input) and pushed to a Kafka topic. Read the data from the Kafka topic and perform the following operations / processes: 1. Segregate the mode of payments like credit card, debit card, Internet banking, UPI, Wallet, Google PAY, PAYTM etc., and in each identify how many were successful and how many were failed due to what reason. 2. Determine City-wise number of orders placed and the total amount made in each payment modes mentioned above. 3. Store the results of point #4 in a Parquet file and also display the same on the console.
## Technology Used
* Kafka -version 2.12-2.8.0
* Spark -version 3.1.2
* Python -version 3.9.6
* Hadoop -version 3.2.2
* Git -version 2.32.0.windows.1
## Features
List of features ready and TODOs for future development using this project some query are solved using spark,kafka following tasks are listed below.Also the schema and sample dataset are listed below.
* Fields (Schema)
* Field name================Description
* order_id==================Order Id
* customer_id===============Customer Id
* customer_name=============Customer Name
* product_id================Product Id
* product_name==============Product Name
* product_category============Product Category
* payment_type==============Payment Type (card, Internet Banking, UPI, Wallet)
* qty=====================Quantity ordered
* price=====================Price of the product
* datetime==================Date and time when order was placed
* country===================Customer Country
* city======================Customer City
* ecommerce_website_name=====Site from where order was placed
* payment_txn_id=============Payment Transaction Confirmation Id
* payment_txn_success=========Payment Success or Failure (Y=Success. N=Failed)
* failure_reason==============Reason for payment failure
### Sample Data (CSV)
* 1,101,John Smith,201,Pen,Stationery,Card,24,10,2021-01-10 10:12,India,Mumbai,www.amazon.com,36766,Y,
* 2,102,Mary Jane,202,Pencil,Stationery,Internet Banking,36,5,2021-10-31 13:45,USA,Boston,www.flipkart.com,37167,Y,
* 3,103,Joe Smith,203,Some mobile,Electronics,UPI,1,4999,2021-04-23 11:32,UK,Oxford,www.tatacliq.com,90383,Y,
* 4,104,Neo,204,Some laptop,Electronics,Wallet,1,59999,2021-06-13 15:20,India,Indore,www.amazon.in,12224,N,Invalid CVV.
* 5,105,Trinity,205,Some book,Books,Card,1,259,2021-08-26 19:54,India,Bengaluru,www.ebay.in,99958,Y,
### Tasks:
1) Create a producer program in Python that will ingest data to a Kafka Topic.
  * a)Data will have to be generated in the program.
  * b)Ingest the data every 2 seconds into the Kafka Topic.
2) Display the data from the input Kafka Topic in a console consumer (CLI).
3) Create a consumer program in Python that will read the data stream from the input Kafka Topic and will process the data further.
   * a)Read the data into a DataFrame object.
   * b)Print the schema of the input data stream
   * c)Apply the above-mentioned schema to the dataframe and print the schema.
   * d)Categorize the data as follows:
       * Payment types: Card, Internet Banking, UPI, Wallet, Google PAY, PAYTM etc.
       * Success and Failed payment transactions.
   * e)Create separate topics for each of the following and send respective data rows to them:
       * Card.
       * Internet Banking.
       * UPI.
       * Wallet.
       * Successful Transactions.
       * Failed Transactions.
   * f)From the consumer program Determine and display on the console the number of orders and total amount for each city and payment type.
       * Also write the same information to a Parquet file (data should be appended to this file).

## Getting Started
* To start this project user need to install sandbox-Hortonworks in virtual machine.
* After installing the VM start the VM then put the following command in Git bash then connect to VM using SSH command "ssh maria_dev@sandbox-hdp.hortonworks.com -p 2222" after this you need to put the password the default password for USER maria_dev is "maria_dev".

##### OR

* If you are using Linux Os then first install all the software which are used in the project mentioned in the above Technology section (to install all the technology we added the installation guide which will easily guide you to set up your environment).
* Now using "cd kafka 2.3.3" (kafka2.3.3 is the folder which is unzipped after downloading)
* After perfectly setup with the environment open terminal where you have to start zookeeper server and in another terminal start the kafka-broker server.
*using below command:
###### bin/zookeeper-server-start.sh config/zookeeper.properties
###### bin/kafka-server-start.sh config/server.properties
* After this in one terminal create Topic using command listed below,
###### bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic Projectp3 --replication-factor 1 --partitions 1
* Now we are ready to create Producer as well as consumer in 2 different new Terminal using command listed below.
##### bin/kafka-console-producer.sh --topic Projectp3 --broker-list localhost:9092
##### bin/kafka-console-producer.sh --topic Projectp3 --broker-list localhost:9092
* After creating producer and consumer we perform task and all are added to this repository to take reference.
## Usage
* Using this project any one can generate data using kafka and can perform streaming using kafka.
## Contributors
* C.Sravani
* CH.Nishank
* Ankith Sharma
* Shetu Das
