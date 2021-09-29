#kafka_producer.py

from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

Topic_name = "data-topic"
Bootstrap_server_name = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=Bootstrap_server_name,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    #costomer_id=[101,102,103,104,105,106,107,108,107,108,109,110,111,112,113,114,115,116,117,118,119,120]
    costomer_id=[i for i in range(100,120)]
    costomer_names = ["Will","James","Samuel","John","George","Sam","Fred","Richard","William","Bert","Albert",
                      "David","Carl","Henry","Walter","Frederick","Andrew","Ernest","Lee","Louis"]
    #product_id=[201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220]
    product_id=[i for i in range(200,220)]
    product_name=["Shirt","Refrigerator","Jeans","Laptop","TV","Airconditioner","Sonata","Books",
                  "Football","Mercedes","Takeela","Rolex","Gucci","American","Express","Jaguar",
                  "Prada","Nike","Coca_Cola","GMC"]
    product_category=["Fashion/Clothing","Electrical","Fashion/Clothing", "Electrical Appliances",
                      "Electrical Appliances"	,"Electrical Appliances","Jewelry/Watch","Stationary",
                      "Sports-related Goods","Automobile","Alcohol"	,"Jewelry/Watch","Fashion/Clothing"	,
                      "Financial Services","Automobile","Aotomobile","Alcohol",
                      "Sports-realted Goods","Food/Beverages","Automobiles"]
    product_name_catogary=dict(zip(product_name,product_category))
    ke=list(product_name_catogary.keys())
    va=list(product_name_catogary.values())
    payment_type=["card", "Internet","Banking", "UPI","Wallet"]
    
    country_name_city_name_list = ["Florida,United States","Paris,France", "Colombo,Sri Lanka", "Dhaka,Bangladesh", "Islamabad,Pakistan",
                                   "Beijing,China", "Rome,Italy", "Chile,Santiago","China,Beijing","Berlin,Germany", "Ottawa,Canada",
                                   "London,United Kingdom","Sydney,Australia", "Jerusalem,Israel", "Bangkok,Thailand",
                                   "Chennai,India", "Bangalore,India","New York City,United States","Mumbai,India", "Pune,India",
                                   "New Delhi,India", "Hyderabad,India", "Kolkata,India", "Singapore,Singapore","Iran,Tehran","Iraq,Baghdad"]

    ecommerce_website_name_list = ["www.datamaking.com", "www.amazon.com", "www.flipkart.com", "www.snapdeal.com", "www.ebay.com"]
    payment_txn_id=[22324,123234,343231,21234345,132434,1343534,232431,4634563,4563534,56746,67467,73635,242345,245242]
    payment_txn_success=["success","failed"]



    message_list = []
    message = None
    for i in range(150):
        i = i + 1
        message = {}
        print("Preparing message: " + str(i))
        event_datetime = datetime.now()

        message["order_id"] = i
        message["costomer_id"]=random.choice(costomer_id)
        message["costomer_names"]=random.choice(costomer_names)
        message["product_id"]=random.choice(product_id)
        p_name=random.choice(ke)
        message["product_name"] = p_name
        message["product_category"]=product_name_catogary[p_name]
        message["payment_type"] = random.choice(payment_type)
        message["quantity"] = round(random.uniform(1, 10), 2)
        message["price"]=round(random.uniform(50.5, 5555.5), 2)
        message["datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
        country_name = None
        city_name = None
        country_name_city_name = None
        country_name_city_name = random.choice(country_name_city_name_list)
        country_name = country_name_city_name.split(",")[1]
        city_name = country_name_city_name.split(",")[0]
        message["order_country_name"] = country_name
        message["order_city_name"] = city_name
        message["order_ecommerce_website_name"] = random.choice(ecommerce_website_name_list)
        message["payment_txn_id"]=random.choice(payment_txn_id)
        message["payment_txn_success"]=random.choice(payment_txn_success)
        
        # print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(Topic_name, message)
        #message_list.append(message)
        if message["payment_type"]=="card":
            kafka_producer_obj.send("card-topic", message)

        elif message["payment_type"]=="Internet":
            kafka_producer_obj.send("internet-topic", message)

        elif message["payment_type"]=="Banking":
            kafka_producer_obj.send("banking-topic", message)

        elif message["payment_type"]=="UPI":
            kafka_producer_obj.send("upi-topic", message)
        
        elif message["payment_type"]=="Wallet":
            kafka_producer_obj.send("wallet-topic", message)

        elif message["payment_txn_success"]=="success":
            kafka_producer_obj.send("success-topic", message)

        elif message["payment_txn_success"]=="failed":
            kafka_producer_obj.send("failed-topic", message)

        time.sleep(1)

    # print(message_list)

    print("Kafka Producer Application Completed. ")
	