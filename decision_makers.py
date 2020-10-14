# DECISION MAKERS.py
from pymongo import MongoClient

# database
myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA

def update():
    print "Updating the time occupancy of all channels"
    # create time occupancy database from sensor data
    with _db.get_collection('channels').find() as channels:
        if len(list(channels)) > 0:
            for channel in channels:
                try:
                    if _db.get_collection("sensor") is None:
                        _db.create_collection("sensor")
                    filt = [
                        {'$match': {'signal.channel': channel['_id']}},
                        {'$project': {'busy': {'$gte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, 7]}}},
                        {'$match': {'busy': True}}]
                    # print db.command('aggregate', 'sensor', pipeline=filter, explain=True)
                    # FIXME use time information for channel model and prediction
                    spc = list(_db.get_collection("sensor").aggregate(filt))
                    # print len(spc), channel['channel']['counts']
                    newdc = float(len(spc)) / float(channel['channel']['counts'])
                    # print newdc
                    query = {'channel.fmin': channel['channel']['fmin'], 'channel.fmax': channel['channel']['fmax']}
                    _db.get_collection('channels').find_one_and_update(query, {'$set': {'channel.duty_cycle': newdc}})

                except Exception, e:
                    print e
        else:
            print "Channel is empty, could not update occupancy!!!"

def return_radio_chans(chan_id):
    print "Checking result of sensed radio channel", chan_id

