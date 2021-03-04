from pprint import *
import psycopg2 as pysql
import pymongo as pym

def write_to_rdb(col):
    try:
        # maak connectie naar de postgresDB
        conn = pysql.connect(
            database="huwebwinkel",
            host="localhost",
            user="postgres",
            password="123456",
        )
        cur = conn.cursor()
        print("Connection established : Postgres")
        
        # insert data naar de postgresDB
        for y in col.find({},{"_id", "gender", "price"}):
            try:
                cur.execute('insert into products (product_id, price, gender) values (%s,%s,%s)',(y['_id'],y["price"]["selling_price"],y['gender']))
            except KeyError:
                continue
        # commit inserts
        conn.commit()

    except pysql.OperationalError as x:
        print(f"Connection error: {x}")
    
    finally:
        # Sluit de cursor
        cur.close()
        # sluit connectie
        conn.close()

def get_collection_mongo(col):
    try:
        client = pym.MongoClient("localhost",27017)
        db = client.huwebwinkel
        collection = db[col]
        print(f"Mongo connection established : {col}")
        client.close()
        # return collection
        write_to_rdb(collection)
    except:
        print(f"Mongo connection failed : {col}")

get_collection_mongo("products")
# get_collection_mongo("sessions")
# get_collection_mongo("visitors")

