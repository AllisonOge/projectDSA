# DECISION MAKERS.py
from pymongo import MongoClient
from bson.objectid import ObjectId
import numpy as np
import datetime
import functools
import pandas as pd
# from current directory
import utils

# database
myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA

_THRESHOLD_DB = 18
_SAMPLE_LEN = 50
_TRAFFIC_CLASS = 'UNKNOWN'

class BestMedianChannels:
    def __init__(self):
        self.all_idle_times = np.array([])
        self.median_idle_time = 0

    def set_idle_times(self, idle_time_stats):
        self.all_idle_times == np.append(self.all_idle_times, idle_time)

        
    def get_best_channels(self, idle_time_stats):
        # check that the array is not empty
        if len(self.all_idle_times) == 0:
            return 0
        idle_best = 0
        self.median_idle_time =  np.median(self.all_idle_times)
        # get total idle time
        total_idle_time = functools.reduce(flatten, idle_time_stats)
        # print "Total idle time per channel", total_idle_time
        # print all_idle_times, median_time
        for i in range(len(idle_time_stats)):
            if idle_time_stats[i] >= self.median_idle_time:
                idle_best += idle_time_stats[i]
        if total_idle_time > 0:
            time_occ_prob = idle_best / total_idle_time
            # print time_occ
            return time_occ_prob
        else:
            return 0
           

def flatten(prev, curr):
    return prev + curr


def gen_seq(obj):
    if obj['busy'] == True:
        return 1
    else:
        return 0


def db_gen_seq(id):
    filt = [
        {
            '$match': {
                'signal.channel': ObjectId(id),
                'in_band': True
            }
        }, {
            '$project': {
                'busy': {
                    '$gt': [
                        {
                            '$subtract': [
                                '$signal.amplitude', '$noise_floor'
                            ]
                        }, _THRESHOLD_DB
                    ]
                }
            }
        }
    ]
    return list(_db.get_collection('sensor').aggregate(filt))


def gen_class_est(model):
    """in-band sequence generator, classification and occupancy estimation"""
    channels = list(_db.get_collection('channels').find())
    channels_distro = list(_db.get_collection('time_distro').find())
    if len(channels) > 0 and len(channels) == len(channels_distro):
        # print "Updating the traffic estimate for all channels"
        # use only in_band sensing to classify channels
        for channel in channels:
            filt = [
                {'$match': {'signal.channel': channel['_id']}, 'in_band': True},
                {'$project': {
                    'busy': {'$gt': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _THRESHOLD_DB]}}}]
            if _db.get_collection('long_term') is not None:
                bit_seq = list(_db.get_collection('long_term').aggregate(filt)) + db_gen_seq(channel['_id'])
            else:
                bit_seq = db_gen_seq(channel['_id'])
            # classify bit sequence
            bit_seq = list(map(gen_seq, bit_seq))
            # print bit_seq
            if len(bit_seq) >= _SAMPLE_LEN:
                period = 1.0
                if len(bit_seq) % _SAMPLE_LEN == 0:
                    # print "Length of bit sequence", len(bit_seq)
                    for i in range(len(bit_seq) // _SAMPLE_LEN):
                        if 'traffic_classifier' not in locals():
                            traffic_classifier = utils.TrafficClassification(_SAMPLE_LEN)
                        # print("length of sequence is ", len(bit_seq[_SAMPLE_LEN * i:_SAMPLE_LEN * (i + 1)]))
                        traffic_class, period = traffic_classifier.classify(
                            bit_seq[_SAMPLE_LEN * i:_SAMPLE_LEN * (i + 1)])
                        # print ("Hi", bit_seq[_SAMPLE_LEN * i:_SAMPLE_LEN * (i + 1)])
                        bit_seq_pd = pd.DataFrame([bit_seq[_SAMPLE_LEN * i:_SAMPLE_LEN * (i + 1)]], dtype=int)
                        # print(bit_seq_pd)
                        model.predict(bit_seq_pd)
                        if model.predict(bit_seq_pd) == 0:
                            traffic_class = "STOCHASTIC"
                            print("TRAFFIC IS STOCHASTIC")
                        else:
                            traffic_class = "PERIODIC"
                            period = traffic_classifier.get_period()
                            print("TRAFFIC IS PERIODIC with period ", period)
                            
                else:
                    traffic_class = _db.get_collection('time_distro').find_one({'channel_id': channel['_id']})[
                        'traffic_class']
            else:
                # default classifications
                traffic_class = 'UNKNOWN'
                period = 0.0

            spc = functools.reduce(flatten, bit_seq)
            # print len(spc), channel['channel']['counts']
            newdc = float(spc) / float(channel['channel']['counts'])
            # print 'new traffic estimate is ', newdc
            query = {'channel.fmin': channel['channel']['fmin'], 'channel.fmax': channel['channel']['fmax']}
            _db.get_collection('channels').find_one_and_update(query, {'$set': {'channel.occ_estimate': newdc}})
            # update database
            if _db.get_collection("time_distro") is None:
                print ('time distro has not being initialized yet, FIXME!!!')
                pass
            else:
                if traffic_class == 'PERIODIC':
                    _db.get_collection("time_distro").update_one({'channel_id': channel['_id']},
                                                                 {'$set': {'period': period}})
                # print traffic_class
                _db.get_collection("time_distro").update_one({'channel_id': channel['_id']},
                                                             {'$set': {'traffic_class': traffic_class}})

        return True
    else:
        if len(channels) < 1:
            print ("Channel is empty, could not update occupancy!!!")
        else:
            print ("New channel set, could not update occupancy!!!")
        return False


def update_random():
    """predict best 2 channels out of 3"""
    channels = list(_db.get_collection('channels').find())
    if len(channels) > 0:
        # print "idle time prediction of all channels"
        get_best_median_channs = BestMedianChannels()
        for channel in channels:
            filt = [
                {'$match': {'signal.channel': channel['_id'], 'in_band': True}},
                {'$project': {
                    'date': 1,
                    'idle': {'$lte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _THRESHOLD_DB]}}}]
            channel_seq = list(_db.get_collection("sensor").aggregate(filt))
            idle_start = 0
            idle_time = 0
            idle_time_stats = np.array([])
            for ind in range(len(channel_seq)):
                if channel_seq[ind]['idle']:
                    if ind > 0:
                        if channel_seq[ind - 1]['idle'] == False and idle_start != 0:
                            # idle_time = channel_seq[ind]['date'].timestamp() - idle_start
                            idle_time = (channel_seq[ind]['date'] - datetime.datetime(1970, 1,
                                                                                    1)).total_seconds() - idle_start
                            idle_time_stats = np.append(idle_time_stats, idle_time)
                        else:
                            idle_start = 0
                else:
                    if idle_start == 0 and ind == 0:
                        # idle_start = channel_seq[ind]['date'].timestamp()
                        idle_start = (channel_seq[ind]['date'] - datetime.datetime(1970, 1, 1)).total_seconds()
                        print(idle_start)
                    if ind > 0:
                        if channel_seq[ind - 1]['idle']:
                            # idle_start = channel_seq[ind]['date'].timestamp()
                            idle_start = (channel_seq[ind]['date'] - datetime.datetime(1970, 1, 1)).total_seconds()
                            print(idle_start)
                        # idle_time = channel_seq[ind]['date'].timestamp() - idle_start
                        idle_time = (channel_seq[ind]['date'] - datetime.datetime(1970, 1,
                                                                                    1)).total_seconds() - idle_start
            idle_start = 0          
            get_best_median_channs.set_idle_times(idle_time_stats)
            # save to database
            if _db.get_collection("time_distro") is None:
                _db.create_collection("time_distro")
            if _db.get_collection("time_distro").find_one({'channel_id': channel['_id']}) is None:
                _db.get_collection("time_distro").insert_one(
                    {'channel_id': channel['_id'], 'idle_time_stats': idle_time_stats.tolist(),
                     'avg_idle_time': float(np.average(idle_time_stats)), 'traffic_class': _TRAFFIC_CLASS})
            else:
                _db.get_collection("time_distro").update_one({'channel_id': channel['_id']},
                                                             {'$set': {'idle_time_stats': idle_time_stats.tolist(),
                                                                       'avg_idle_time': float(
                                                                           np.average(idle_time_stats))}})
            
        # get the best channels after updating all channels
        for chan in channels:
            its = np.array(_db.get_collection('time_distro').find({'channel_id': chan['_id']})['idle_time_stats'])
            time_occ_prob = get_best_median_channs.get_best_channels(its)

            # update channels collection
            _db.get_collection('channels').find_one_and_update({'_id': chan['_id']}, {'best_channel': time_occ_prob})

        return True
    else:
        print ("Channel is empty, could not predict!!!")
        return False


def return_radio_chans(result):
    diff = float(result[b'signal'][b'amplitude']) - float(result[b'noise_floor'])
    if diff >= _THRESHOLD_DB:
        state = 'busy'
    else:
        state = 'free'
    print ('Channel state ', state)
    return {
        'id': result[b'signal'][b'channel'],
        'state': state
    }


def get_t0(id):
    bit_seq = db_gen_seq(id)
    bit_seq = list(map(gen_seq, bit_seq)[::-1])
    t0 = 0
    for i in range(len(bit_seq)):
        if bit_seq[0] != 0:
            break
        else:
            if i > 0:
                if not (bit_seq[i - 1] and bit_seq[i]):
                    print (bit_seq[i - 1], bit_seq[i])
                    t0 += 1
                else:
                    break
    return t0
