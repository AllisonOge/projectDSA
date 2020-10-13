# DECISION MAKERS.py
from pymongo import MongoClient

def main():
    print "This is the decision maker of the Cognitive radio!!!"
    # database
    myclient = MongoClient('mongodb://127.0.0.1:27017/')
    db = myclient.projectDSA

    # FIXME use watch method instead to implement realtime update
    while True:
        # create time occupancy database from sensor data
        with db.get_collection('channels').find() as channels:
            for channel in channels:
                try:
                    if db.get_collection("sensor") is None:
                        db.create_collection("sensor")
                    filter = [
                        {'$match': {'signal.channel': channel['_id']}},
                        {'$project': {'idle': {'$lte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, 5]}}},
                        {'$match': {'idle': True}}]
                    # print db.command('aggregate', 'sensor', pipeline=filter, explain=True)
                    # FIXME use time information for channel model and prediction
                    spc = list(db.get_collection("sensor").aggregate(filter))
                    newdc = len(spc) / channel['channel']['counts']
                    query = {'channel.fmin': channel['channel']['fmin'], 'channel.fmax': channel['channel']['fmax']}
                    db.get_collection('channels').find_one_and_update(query, {'$set': {'channel.duty_cycle': newdc}})

                except Exception, e:
                    print e

if __name__ == '__main__':
    main()
