from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from consumer_config import config_params

from pymongo import MongoClient
from db_utils import insert_image
import gridfs

from fuzzy import fuzzy, abndObj, comparObj, comparTimeObj, sameplace

import requests
import threading
import os
import json
import cv2
import numpy as np
import time
import logging

#To receive notification of delivery success or failure, 
#no delivery notification events will be propagated until poll() is invoked
def delivery_report(err, msg):
    if err:
        logging.error("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        logging.info("msg produced. "+
                    "Topic: {0}".format(msg.topic()) +
                    "Partition: {0}".format(msg.partition()) +
                    "Offset: {0}".format(msg.offset()) +
                    "Timestamp: {0}".format(msg.timestamp()))

def alert_suspAction(suspValue):
    url = """http://fog_streaming:5000/"""
    res = requests.post(url, 
                            json={'action':'SUSPECT',
                                #'imageID':str(imgID),
                                'suspValue':suspValue})
    #if res == '200':
    #print("Action alerted successfully!!!")

class ConsumerThread:
    def __init__(self, config_params, detectiontopics, db, fs):
        self.config_params = config_params
        self.detectiontopics = detectiontopics
        self.db = db
        self.fs = fs
    

    #subscribes to corresponding topic & starts processing msgs
    def read_data(self, detectiontopic):
        consumer = Consumer(self.config_params)
        consumer.subscribe(str(detectiontopic).split())
        self.run(consumer, detectiontopic)


    #starts retrieving records one-by-one
    def run(self, consumer, detectiontopic):
        try:
            old = 0
            timeAbanObject = [0.0]
            prevObj = {}
            #susp = 0    
            while True:
                #poll timeout is 0.1 s if it reaches that, record returned empty(None)
                msg = consumer.poll(0.1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print('End of partition reached {0}/{1}'
                            .format(msg.topic(), msg.partition()))
                    else:
                        raise KafkaException(msg.error())
                else:
                    # get metadata
                    imgFeatures = {}
                    for header in msg.headers():
                        imgFeatures[header[0]] = header[1]
                    
                    host_name = imgFeatures['Host'].decode('utf-8')
                    camera_name = imgFeatures['camera_name'].decode('utf-8')
                    
                    objectss = imgFeatures['frameObjects']
                    #deserializes bytes to a python dict:
                    frameObjectss = json.loads(objectss.decode('utf-8'))

                    nb_people = int(imgFeatures['Nb_people'].decode('utf-8'))
                    speed_change = float(imgFeatures['Speed_change'].decode('utf-8'))
                    frame_no = int(imgFeatures['frame_no'].decode('utf-8')) 
                    frame_time = float(imgFeatures['timestamp'].decode('utf-8'))
                    imgFeatures = {
                        'frame_time':frame_time,
                        'host_name':host_name,
                        'camera_name':camera_name,
                        'frame_objects':frameObjectss,
                        'nb_people':nb_people,
                        'speed_change':speed_change,
                        'frame_no': frame_no,
                    }
                    
                    collection = self.db[host_name]
                    #-------------------fuzzy model-----------------------#
                    #abandond object test        
                    j = 0
                    b = True
                    while b:
                        j+=1
                        old+=1
                        box = "box"+str(j)
                        if box in frameObjectss:
                            if frameObjectss[box]["class"] != 'person' :
                                obj = abndObj(frameObjectss[box],frame_time)
                                if len(prevObj) == 0:
                                    prevObj.update({"box"+str(old):obj})
                                else:
                                    oldObj = [prevObj[x] for x in prevObj if prevObj[x]['name']==obj['name'] and comparObj(prevObj[x],obj)]
                                    if len(oldObj) == 0:
                                        prevObj.update({"box"+str(old):obj})
                                    else:
                                        time_interval = comparTimeObj(oldObj[0], obj)
                                        timeAbanObject.append(time_interval) 
                                        
                        else:
                            b = False

                    #apply fuzzy function
                    suspValue1 = fuzzy(P1, P2, X1, X2)
                    maxT = max(timeAbanObject)
                    #apply last value with new one of object abandoned
                    suspValue2 = fuzzy(suspValue1, maxT, X3, X4)

                    #UPDATE imgFeatures with the percentage of fuzzy!!!!
                    imgFeatures.update({'suspValue':suspValue2})
                    #------------------if suspicious add to db-------------#
                    if suspValue2>"Value":
                        #convert img bytes data to numpy array of dtype uint8
                        imgarr = np.frombuffer(msg.value(), np.uint8)
                        imgFeatures = insert_image(self.fs, collection, imgarr, imgFeatures)
                        #send an alert 
                        imgID = imgFeatures['imageID']
                        if maxT > 36.0:
                            old = 0
                            timeAbanObject = [0.0]
                            prevObj = {}
                        imgID = imgFeatures['imageID']
                        alert_suspAction(imgID, suspValue2)
                    #-------------------------------------------------------#
                    # commit synchronously
                    consumer.commit(asynchronous=False)


        except KeyboardInterrupt:
            print("Detected Keyboard Interrupt. Cancelling.")
            pass

        finally:
            consumer.close()
            print("Finished Consuming from:"+detectiontopic+" ...")
    
    
    def start(self):
        #for each topic we run an instance of the consumer 
        for detectiontopic in self.detectiontopics:
            t = threading.Thread(target=self.read_data(detectiontopic))
            t.daemon = True
            t.start()


if __name__ == "__main__":
    print('starting consumer')
    #----------Kafka connection---------#
    admin_client = AdminClient({
    'bootstrap.servers': 'KAFKA_BROKER_URL'
    })
    #get all broker's topics
    broker_topics = admin_client.list_topics().topics.keys() 
    while not broker_topics:
        broker_topics = admin_client.list_topics().topics.keys() 

    #get the corresponding topics created by producers of diff cameras
    detection_topics = [t for t in broker_topics if(t.startswith('Pi') and t is not None)]
    #---------------------------------------#
    #-----------Database--------------------#
    # Get a MongoClient instance
    mongoClient = MongoClient("mongodb://user&password:27017/",
    maxPoolSize=200,          # connection pool size is 200
    waitQueueTimeoutMS=200,   # how long a thread can wait for a connection
    waitQueueMultiple=500     # when the pool is fully used 200 threads can wait
)
    database = mongoClient.database

    # Get a gridfs instance linked to database
    fs = gridfs.GridFS(database)

    #-----------------------------------------#
    #-----------Start processing each topic------------#
    consumer_thread = ConsumerThread(config_params, detection_topics, database, fs)
    consumer_thread.start()
