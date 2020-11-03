# DECISION MAKERS.py
from pymongo import MongoClient
import sys
import datetime

# database
myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA

_threshold_db = 21

def update():
    """in-band sequence generator for classification"""
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

def update_random():
    """compute the median time availability"""
    channels = list(_db.get_collection('channels').find())
    if len(channels) > 0:
        print "Updating the traffic estimate for all channels"
        for channel in channels:
            filt = [
                {'$match': {'signal.channel': channel['_id']}},
                {'$project': {
                    'date': 1,
                    'idle': {'$lt': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _threshold_db]}}}]
            channel_seq = list(_db.get_collection("sensor").aggregate(filt))
            idle_start = 0
            idle_time = 0
            idle_time_stats = []
            # FIXME convert datetime to float seconds
            for i in range(len(channel_seq)):
                if channel_seq[i]['idle'] == True and idle_start == 0:
                    idle_start = channel_seq[i]['date']
                    print idle_start
                elif channel_seq[i]['idle'] == True and idle_start > 0:
                    idle_time = channel_seq[i]['date'] - idle_start
                    print idle_time
                elif channel_seq[i]['idle'] == False and idle_time > 0:
                    idle_time_stats.append(idle_time)
                    idle_start = 0
                else:
                    print 'you missed this condition'



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