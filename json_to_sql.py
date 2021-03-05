import psycopg2 as pysql
import pymongo as pym

'''
Dit bestand is uniek en op eigen kennis geschreven.
Er is geen gebruik gemaakt van externe algoritmes en/of bronnen

Auteurs:
Colin Vlienden
Rutger Willard
Justin Klein
Bram van Leusden
'''

def connect_to_rdb(col,collection):
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
        if collection == "products":
            products_to_rdb(col,cur,conn,collection)
        elif collection == "visitors":
            visitors_to_rdb(col,cur,conn,collection)
        elif collection == "sessions":
            pass
    except pysql.OperationalError as x:
        print(f"Connection error : {x}")
    
    finally:
        
        # Sluit de cursor
        cur.close()
        # sluit connectie
        conn.close()

def products_to_rdb(col,cur,conn,collection):
        # insert data naar de postgresDB
        print(f"Prossessing : {collection}")
        for y in col.find({},{"_id", "gender", "price","recommendable","category","sub_category","sub_sub_category","properties","sm"}):
            try:
                cur.execute('insert into products (products_id, price, in_stock,active,recommendable, gender, category, sub_category, sub_sub_category) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                (y["_id"],y["price"]["selling_price"],y["properties"]["stock"],y["sm"]["is_active"],y["recommendable"],y["gender"], y["category"], y["sub_category"], y["sub_sub_category"]))
            except KeyError:
                continue
            
        # commit inserts 
        conn.commit()
        print("Data Commited")
        

def visitors_to_rdb(col,cur,conn,collection):
        # insert data naar de postgresDB
        print(f"Prossessing : {collection}")
        for y in col.find({},{"_id","recommendations"}):
            try:
                cur.execute('insert into visitors (visitor_id,viewed_before) values (%s,%s)',
                (str(y["_id"]),y["recommendations"]["viewed_before"]))

            except KeyError:
                continue

        # commit inserts 
        conn.commit()
        print("Data Commited")

def get_collection_mongo(collection):
    try:
        client = pym.MongoClient("localhost",27017)
        db = client.huwebwinkel
        col = db[collection]
        print(f"Mongo connection established : {collection}")
        client.close()
        connect_to_rdb(col,collection)
    except:
        print(f"Mongo connection failed : {collection}")

get_collection_mongo("products")
# get_collection_mongo("sessions")
# get_collection_mongo("visitors")

