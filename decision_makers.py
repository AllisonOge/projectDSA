# DECISION MAKERS.py
from pymongo import MongoClient

def main():

    print "This is the decision maker of the Cognitive radio!!!"
    # database
    myclient = MongoClient('mongodb://127.0.0.1:27017/')
    db = myclient.projectDSA
    if db.get_collection("sensor") is None:
        db.create_collection("sensor")
    # create time occupancy database fro sensor data
    help(db.get_collection('sensor'))
    # help(db)
    # with db.watch() as stream:
    #     for change in stream:
    #         print change



if __name__ == '__main__':
    main()