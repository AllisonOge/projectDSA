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
_SAMPLE_LEN = 100
_TRAFFIC_CLASS = 'UNKNOWN'


class BestMedianChannels:
    def __init__(self):
        self.all_idle_times = np.array([])
        self.median_idle_time = 0

    def set_idle_times(self, idle_time_stats):
        self.all_idle_times = np.append(self.all_idle_times, idle_time_stats)

    def get_best_channels(self, idle_time_stats):
        # check that the array is not empty
        if len(self.all_idle_times) == 0:
            return 0
        if len(idle_time_stats) == 0:
            return 0
        idle_best = 0
        self.median_idle_time = np.median(self.all_idle_times)
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

def reset_time_distro():
    if _db.get_collection('time_distro') is None:
        return False

    if _db.get_collection('long_term') is None:
        _db.get_collection('sensor').rename('long_term')
    else:
        if len(list(_db.get_collection('sensor').find())) > 0:
            _db.get_collection('long_term').insert_many(list(_db.get_collection('sensor').find()))
    _db.get_collection('sensor').drop()

    channels = list(_db.get_collection('channels').find())
    for channel in range(channels):
        query = {'channel_id': channel['_id']}
        updated_idle_times = _db.time_distro.find_one(query)['idle_times_long_term']
        updated_idle_times = np.append(updated_idle_times, _db.time_distro.find_one({'channel_id': channel['id']})['idle_time_stats'])
        _db.get_collection('time_distro').find_one_and_update(query,{'idle_times_long_term': updated_idle_times.tolist()})

def gen_class_est(free_channel ,model):
    """in-band sequence generator, classification and occupancy estimation"""
    filt = [
            {'$match': {'signal.channel': free_channel['id'], 'in_band': True}},
            {'$project': {
                'busy': {'$gt': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _THRESHOLD_DB]}}}]
    if _db.get_collection('long_term') is not None:
        bit_seq = list(_db.get_collection('long_term').aggregate(filt)) + db_gen_seq(free_channel['id'])
    else:
        bit_seq = db_gen_seq(free_channel['id'])
    # classify bit sequence
    bit_seq = list(map(lambda x: 1 if x['busy'] == True else 0, bit_seq))

    # use last set of SAMPLE_LEN bit to classisfy channel
    if len(bit_seq[-_SAMPLE_LEN:]) >= _SAMPLE_LEN:
        traffic_classifier = utils.TrafficClassification(_SAMPLE_LEN)
        # print("length of sequence is ", len(bit_seqbit_seq[-_SAMPLE_LEN:]))
        traffic_class, period = traffic_classifier.classify(bit_seq[-_SAMPLE_LEN:])
        bit_seq_pd = pd.DataFrame([bit_seq[-_SAMPLE_LEN:]], dtype=int)
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
        # default classifications
        traffic_class = 'UNKNOWN'
        period = 0.0
        return False

    spc = functools.reduce(lambda x, y: y+x, bit_seq)
    # print len(spc), len(bit_seq)
    newdc = float(spc) / float(len(bit_seq))
    # print 'new traffic estimate is ', newdc
    query = {'_id': free_channel['id']}
    _db.get_collection('channels').find_one_and_update(query, {'$set': {'channel.occ_estimate': newdc}})
    # update database
    if traffic_class == 'PERIODIC':
        _db.get_collection("time_distro").update_one({'channel_id': free_channel['id']},
                                                        {'$set': {'period': period}})
    # print traffic_class
    _db.get_collection("time_distro").update_one({'channel_id': free_channel['id']},
                                                    {'$set': {'traffic_class': traffic_class}})
    return True, {'traffic_class': traffic_class, 'period': period}


def update_random():
    """get idle times of channels"""
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
                if not channel_seq[ind]['idle']:
                    if ind > 0:
                        if channel_seq[ind - 1]['idle'] and idle_start != 0:
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
                        # print(idle_start)
                    if ind > 0:
                        if not channel_seq[ind - 1]['idle']:
                            # idle_start = channel_seq[ind]['date'].timestamp()
                            idle_start = (channel_seq[ind]['date'] - datetime.datetime(1970, 1, 1)).total_seconds()
                            # print(idle_start)
                        # idle_time = channel_seq[ind]['date'].timestamp() - idle_start
                        idle_time = (channel_seq[ind]['date'] - datetime.datetime(1970, 1,
                                                                                  1)).total_seconds() - idle_start
            idle_start = 0
            # print("idle time stats of a channel is ", idle_time_stats)
            get_best_median_channs.set_idle_times(idle_time_stats)
            if len(idle_time_stats) > 0:
                avg_idle_time = float(np.average(idle_time_stats))
            else:
                avg_idle_time = 0
            # save to database
            if _db.get_collection("time_distro") is None:
                _db.create_collection("time_distro")
            if _db.get_collection("time_distro").find_one({'channel_id': channel['_id']}) is None:
                _db.get_collection("time_distro").insert_one(
                    {'channel_id': channel['_id'], 'idle_time_stats': idle_time_stats.tolist(),
                     'avg_idle_time': avg_idle_time, 'traffic_class': _TRAFFIC_CLASS})
            else:
                _db.get_collection("time_distro").update_one({'channel_id': channel['_id']},
                                                             {'$set': {'idle_time_stats': idle_time_stats.tolist(),
                                                                       'avg_idle_time': avg_idle_time}})

        # get the best channels after updating all channels
        for chan in channels:
            its = np.array(_db.get_collection('time_distro').find_one({'channel_id': chan['_id']})['idle_time_stats'])
            # print('idle time stats array is ', its)
            time_occ_prob = get_best_median_channs.get_best_channels(its)
            # update channels collection
            query = {'channel.fmin': chan['channel']['fmin'], 'channel.fmax': chan['channel']['fmax']}
            _db.get_collection('channels').find_one_and_update(query, {'$set': {'best_channel': time_occ_prob}})

        return True
    else:
        print("Channel is empty, could not predict!!!")
        return False


def return_radio_chans(result):
    diff = float(result[b'signal'][b'amplitude']) - float(result[b'noise_floor'])
    if diff >= _THRESHOLD_DB:
        state = 'busy'
    else:
        state = 'free'
    print('Channel state ', state)
    return {
        'id': result[b'signal'][b'channel'],
        'state': state
    }


def get_t0(id):
    bit_seq = db_gen_seq(id)
    bit_seq = list(map(lambda x: 1 if x['busy'] == True else 0, bit_seq)[::-1])
    t0 = 0
    for i in range(len(bit_seq)):
        if bit_seq[0] != 0:
            break
        else:
            if i > 0:
                if not (bit_seq[i - 1] and bit_seq[i]):
                    print(bit_seq[i - 1], bit_seq[i])
                    t0 += 1
                else:
                    break
    return t0
