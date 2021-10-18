import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import gridfs
import numpy as np

def insert_image(fs, collection, image, image_features):
    frame_no = image_features['frame_no']
    shape = image.shape
    imageString = image.tostring()

    collection.create_index('frame_no', unique=True )
    try:
        if collection.find_one({'frame_no': frame_no}) == None:
            #save the image the gridfs db
            imageID = fs.put(imageString, encoding='utf-8')

            image_features.update({'imageID':imageID, 'shape':shape})
            #insert the images features to the database
            collection.insert(image_features) #...
    
    except DuplicateKeyError:
        print("Document exists")
    except Exception as e:
        print("Error Occured.")
        print(e)
        pass 

    return image_features


def get_image(imageID, shape, fs):
    #get img from gridfs by imageID
    image = fs.get(imageID)
    # convert bytes to nparray
    img = np.frombuffer(image.read(), dtype=np.uint8)
    img = np.reshape(img, shape)
    return img


