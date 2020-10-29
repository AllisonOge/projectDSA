# DECISION MAKERS.py
from pymongo import MongoClient
import sys

# database
myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA

_threshold_db = 21

def update():
    # create time occupancy database from sensor data
    channels = list(_db.get_collection('channels').find())
    if len(channels) > 0:
        print "Updating the traffic estimate for all channels"
        for channel in channels:
            filt = [
                {'$match': {'signal.channel': channel['_id']}},
                {'$project': {
                    'busy': {'$gte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _threshold_db]}}},
                {'$match': {'busy': True}}]
            # print db.command('aggregate', 'sensor', pipeline=filter, explain=True)
            # FIXME use time information for channel model and prediction
            spc = list(_db.get_collection("sensor").aggregate(filt))
            # print len(spc), channel['channel']['counts']
            newdc = len(spc) / float(channel['channel']['counts'])
            print 'new traffic estimate is ', newdc
            query = {'channel.fmin': channel['channel']['fmin'], 'channel.fmax': channel['channel']['fmax']}
            _db.get_collection('channels').find_one_and_update(query, {'$set': {'channel.duty_cycle': newdc}})
        return True
    else:
        print "Channel is empty, could not update occupancy!!!"
        return False

def return_radio_chans(chan_id):
    print "Checking result of sensed radio channel", chan_id
    filt = [
        {'$match': {'signal.channel': chan_id}},
        {'$project': {'busy': {'$gte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _threshold_db]}}}]
    chan = list(_db.get_collection('sensor').aggregate(filt)).pop(-1)
    print 'is channel busy? ', chan['busy']
    if chan['busy'] is True:
        state = 'busy'
    else:
        state = 'free'
    return {
        'id': chan_id,
        'state': state
    }