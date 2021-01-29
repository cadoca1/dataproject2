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


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

"""[Connect to database](https://pynative.com/python-postgresql-tutorial/)"""


def connect():
    try:
        connection = psycopg2.connect(user = "postgres",
                                      password = "Welcome01",
                                      host = "35.238.29.211",
                                      port = "5432",
                                      database = "dataproject2")
       
        cursor = connection.cursor()
        return cursor
        
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
        


def algoritmo(text_person):
    cursor = connect()
        
    cursor.execute("SELECT * FROM casas;")
    record = cursor.fetchall()
    
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'casas'")
    columns_name = cursor.fetchall()
    
    """Convert array of arrays to single array"""
    
    array_columns_name = np.array(columns_name)
    array_columns_name = np.concatenate( array_columns_name, axis=0 )
    
    #print(array_columns_name)
    
    """Transform result of query to a pandas dataframe"""
    
    df = pd.DataFrame(record, columns=array_columns_name)
    
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
    
    casas_coste = df[df.cost <= ((int(salary)/12)*0.2)]

    #casas_hab1 = casas_coste.loc[casas_coste.rooms == members]
    #casas_hab2 = casas_coste.loc[casas_coste.rooms == int(members)+1]
    
    #casas_hab = pd.concat([casas_hab1,casas_hab2]).drop_duplicates().reset_index(drop=True)

    casas_hab = casas_coste.loc[(casas_coste.rooms == members) | ((casas_coste.rooms.astype(int)) == int(members)+1)]

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
            
            
    casas_libres = casas_hab.loc[casas_hab.c_counter <= 4]
    mejor_casa = casas_libres
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
    
    # Configuración de acceso con las credenciales
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
     
    # Listos para hacer la conexión con el API
    api = tweepy.API(auth)
    
    contador = 0
    for _, i in resultado.iterrows():
        # Ofrecer como maximo 2 casas a una misma persona
        if contador > 2:
            break;    
        
        tweet = "Hi! "+ name + ", your perfect house is placed in " + i.city_name + ", with the code: " + str(i.house_id)
        tweet += "https://twitter.com/dlpexercisepro1/status/" + str(i.tweet_id)
        
        
        # Download the image
        url = images_url.get(i.city_name)
        filename = '/tmp/temp.jpg'
        request = requests.get(url, stream=True)
        if request.status_code == 200:
            with open(filename, 'wb') as image:
                for chunk in request:
                    image.write(chunk)
        else:
            print("Unable to download image")
        
        # Publish Tweet with image
        api.update_with_media(filename, status=tweet, in_reply_to_status_id = tweet_person_id, auto_populate_reply_metadata = True)
        
        os.remove(filename)   
        contador += 1
        
        # Increment house counter
        cursor.execute("UPDATE casas SET c_counter = c_counter + 1 WHERE tweet_id = " + i.tweet_id)
        
    return "publicado: \n" + tweet


def spark():

    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
    
    sc = SparkContext(appName="Twitter_Streaming")
    
    ssc = StreamingContext(sc, 1)
    
    topics = ['este']

    kafkaParams = {'bootstrap.servers': 'broker:29092', 
                   'group.id' : 'test'}
    
    stream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)
    
    stream.map(lambda record : algoritmo(record[1])).pprint()
    
    #stream.map(lambda record: funcion(record[1])).pprint()
    
    ssc.start()
    ssc.awaitTermination()
    
    
if __name__ == "__main__":
    spark()