# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import psycopg2
import numpy as np
import pandas as pd
import time
import json
import tweepy
from tweepy import OAuthHandler
import requests
import os
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.conf import SparkConf
import pyspark.sql.functions as f
import random

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def connect():
    try:
        connection = psycopg2.connect(user = "postgres",
                                      password = "Welcome01",
                                      host = "35.238.29.211",
                                      port = "5432",
                                      database = "dataproject2")
       
        cursor = connection.cursor()
        return cursor, connection
        
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
           


def algoritmo(text_person):
    tg0 = time.time()
    cursor, connection = connect()
    
    playa = ["Valencia", "Barcelona", "Ibiza"]
    ciudad = ["Madrid", "Barcelona", "Valencia", "Sevilla"]
    naturaleza = ["Oviedo", "Bilbao"]
    fiesta = ["Ibiza", "Madrid", "Barcelona"]
    
    scoring = {"Madrid": 0, "Barcelona": 0, "Ibiza": 0, "Valencia": 0, "Sevilla": 0, "Oviedo": 0, "Bilbao": 0}
    
    json_person = json.loads(text_person)
    tweet_text = json_person['full_text']
    tweet_person_id = json_person['id']
    
    split_text = tweet_text.split(" ")
    name = split_text[3] + " " + split_text[4][:-1]
    salary = split_text[8][:-1]
    members = split_text[19]
    beach = split_text[25][-3:-2]
    city = split_text[26][-3:-2]
    nature = split_text[27][-3:-2]
    party = split_text[28][-2:-1]

    t0 = time.time()

    for k in scoring:
        try:
            playa.index(k)
            scoring[k] += int(beach)
        except (Exception) as error :
            print("Not present", error)

        try:
            ciudad.index(k)
            scoring[k] += int(city)
        except (Exception) as error :
            print("Not present", error)

        try:
            naturaleza.index(k)
            scoring[k] += int(nature)
        except (Exception) as error :
            print("Not present", error)

        try:
            fiesta.index(k)
            scoring[k] += int(party)
        except (Exception) as error :
            print("Not present", error)

    t1 = time.time()
    print("Tiempo de scoring: "+ str(t1-t0))

    t0 = time.time()
    cursor.execute("SELECT * FROM casas WHERE cost <= " + str((int(salary)/12)*0.2) + " and (rooms = "
                   + str(members) + " or rooms = " + str(int(members)+1) + ") and c_counter <=4;")
    record = cursor.fetchall()

    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'casas'")
    columns_name = cursor.fetchall()

    cursor.close()

    t1 = time.time()
    print("Tiempo de consulta a base de datos: "+ str(t1-t0))

    """Convert array of arrays to single array"""

    array_columns_name = np.array(columns_name)
    array_columns_name = np.concatenate( array_columns_name, axis=0 )

    #print(array_columns_name)

    """Transform result of query to a pandas dataframe"""

    df = pd.DataFrame(record, columns=array_columns_name)

    #casas_coste = df[df.cost <= ((int(salary)/12)*0.2)]

    #casas_hab = casas_coste.loc[(casas_coste.rooms == members) | ((casas_coste.rooms.astype(int)) == int(members)+1)]

    #casas_libres = casas_hab.loc[casas_hab.c_counter <= 4]
    mejor_casa = df
    mejor_casa['score'] = mejor_casa['city_name'].map(scoring)
    resultado = mejor_casa[mejor_casa.score == max(mejor_casa.score)]

    consumer_key = "ZHwb6JawdsgTUZreb4yZeb2tX"
    consumer_secret = "2UoU6lEpNeTPM847paK6X6z6BVoUENhPq87rIIWcOycnNhGtpd"
    access_token = "1346037018132930560-G3afFHkhsPiYlo1yFO3X4mTmdJhTOY"
    access_token_secret = "jLjtkTK9VQVitacVhLj8QtSdb0mkOto4J9PDDSWR9x9Q0"

    images_url = {"Madrid": "https://www.enforex.com/images/fichas/madrid/ciudad-madrid-2.jpg", 
              "Barcelona": "https://www.alsa.es/documents/21643679/21664598/Barcelona.jpg", 
              "Ibiza": "https://www.iagua.es/sites/default/files/styles/thumbnail-700x700/public/1155x510-ibiza1.jpg?itok=0SafgsGX", 
              "Valencia": "https://static.lasprovincias.es/www/multimedia/202009/30/media/cortadas/valencia-turismo-tarjeta-kfHG-U1203216296067wG-624x385@Las%20Provincias.jpg", 
              "Sevilla": "https://elcorreoweb.es/binrepository/plaza-espana-sevilla_20333351_20200107162131.jpg", 
              "Oviedo": "https://www.iberia.com/ibcomv3/content/landings/OVD.jpg", 
              "Bilbao": "https://www.leonardo-hotels.es/octopus/Upload/images/Pages/bilbao-1920x580.jpg"}
    images_path = {
        "Madrid": ["imgs/madrid_1.jpg","imgs/madrid_2.jpg"],
        "Barcelona": ["imgs/barcelona_1.jpg", "imgs/barcelona_2.jpg"],
        "Ibiza": ["imgs/ibiza_1.jpg","imgs/ibiza_2.png","imgs/ibiza_3.jpg","imgs/ibiza_4.jpg"],
        "Valencia": ["imgs/valencia_1.jpg","imgs/valencia_2.jpg","imgs/valencia_3.jpg"],
        "Sevilla": ["imgs/sevilla_1.jpg","imgs/sevilla_2.jpg","imgs/sevilla_3.jpg"],
        "Oviedo": ["imgs/oviedo_1.jpg","imgs/oviedo_2.jpg","imgs/oviedo_3.jpg"],
        "Bilbao": ["imgs/bilbao_1.jpg", "imgs/bilbao_2.jpg","imgs/bilbao_3.jpg"]
    }

    # Configuración de acceso con las credenciales
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Listos para hacer la conexión con el API
    api = tweepy.API(auth)

    t0 = time.time()
    contador = 0
    for _, i in resultado.iterrows():
        # Ofrecer como maximo 2 casas a una misma persona
        if contador >= 2:
            break;    

        tweet = "Hi! "+ name + ", your perfect house is placed in " + i.city_name + ", with the code: " + str(i.house_id)
        tweet += " https://twitter.com/dlpexercisepro1/status/" + str(i.tweet_id)

        t2 = time.time()
        # Download the image
        #url = images_url.get(i.city_name)
        #filename = '/tmp/temp.jpg'
        #request = requests.get(url, stream=True)
        #if request.status_code == 200:
        #    with open(filename, 'wb') as image:
        #        for chunk in request:
        #            image.write(chunk)
        #else:
        #    return "Unable to download image"

        num_img = random.randint(0, len(images_path.get(i.city_name))-1)
        filename = "/home/jovyan/" + images_path.get(i.city_name)[num_img]
        
        t3 = time.time()
        print("Tiempo de descarga de imagen: " +str(t3-t2))

        # Publish Tweet with image
        api.update_with_media(filename, status=tweet, in_reply_to_status_id = tweet_person_id, auto_populate_reply_metadata = True)

        #os.remove(filename)   
        contador += 1

        # Increment house counter
        cursor, connection = connect()
        cursor.execute("UPDATE casas SET c_counter = c_counter + 1 WHERE tweet_id = " + str(i.tweet_id))
        connection.commit()
        connection.close()
        
    t1 = time.time()
    print("Tiempo de publicación: "+ str(t1-t0))
    tg1 = time.time()
    print("Tiempo total: "+ str(tg1-tg0))
    return "publicado"


def spark():

    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

    conf = SparkConf()
    conf.set("spark.ui.port", "4042")
    
    sc = SparkContext(appName="Twitter_Streaming", conf=conf)
    
    ssc = StreamingContext(sc, 1)
    
    topics = ['este']

    kafkaParams = {'bootstrap.servers': 'broker:29092', 
                   'group.id' : 'test'}
    
    stream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)
    
    stream.map(lambda record : algoritmo(record[1])).pprint()
       
    ssc.start()
    ssc.awaitTermination()
    
    
    
if __name__ == "__main__":
    spark()