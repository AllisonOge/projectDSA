# DECISION MAKERS.py
from pymongo import MongoClient
import numpy as np
import datetime
import utils

# database
myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA

_THRESHOLD_DB = 21
_SAMPLE_LEN = 20
_TRAFFIC_CLASS = 'UNKNOWN'


def flatten(prev, curr):
    return prev + curr


def gen_seq(obj):
    if obj['busy'] == True:
        return 1
    else:
        return 0


def db_gen_seq(id):
    filt = [
        {'$match': {'signal.channel': id}},
        {'$project': {
            'busy': {'$gte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _THRESHOLD_DB]}}}]
    return list(_db.get_collection('sensor').aggregate(filt))


def gen_class_est():
    """in-band sequence generator, classification and occupancy estimation"""
    channels = list(_db.get_collection('channels').find())
    if len(channels) > 0:
        # print "Updating the traffic estimate for all channels"
        for channel in channels:
            filt = [
                {'$match': {'signal.channel': channel['_id']}},
                {'$project': {
                    'busy': {'$gte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _THRESHOLD_DB]}}}]
            if _db.get_collection('long_term') is not None:
                bit_seq = list(_db.get_collection('long_term').aggregate(filt)) + db_gen_seq(channel['_id'])
            else:
                bit_seq = db_gen_seq(channel['_id'])
            # classify bit sequence
            bit_seq = map(gen_seq, bit_seq)
            # print bit_seq
            if len(bit_seq) >= _SAMPLE_LEN:
                period = 1.0
                if len(bit_seq) % _SAMPLE_LEN == 0:
                    # print "Length of bit sequence", len(bit_seq)
                    for i in range(len(bit_seq) // _SAMPLE_LEN):
                        if 'traffic_classifier' not in locals():
                            traffic_classifier = utils.TrafficClassification(_SAMPLE_LEN)
                        # print len(bit_seq[_SAMPLE_LEN * i:_SAMPLE_LEN * (i + 1)])
                        traffic_class, period = traffic_classifier.classify(
                            bit_seq[_SAMPLE_LEN * i:_SAMPLE_LEN * (i + 1)])
                else:
                    traffic_class = _db.get_collection('time_distro').find_one({'channel_id': channel['_id']})[
                        'traffic_class']
            else:
                # default classifications
                traffic_class = 'UNKNOWN'
                period = 0.0

            result = map(lambda bit: bit == 1, bit_seq)
            spc = 0
            for i in range(len(result)):
                if result[i]:
                    spc += 1
            # print len(spc), channel['channel']['counts']
            newdc = float(spc) / float(channel['channel']['counts'])
            # print 'new traffic estimate is ', newdc
            query = {'channel.fmin': channel['channel']['fmin'], 'channel.fmax': channel['channel']['fmax']}
            _db.get_collection('channels').find_one_and_update(query, {'$set': {'channel.occ_estimate': newdc}})
            # update database
            if _db.get_collection("time_distro") is None:
                print 'time distro has not being initialized yet, FIXME!!!'
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
        print "Channel is empty, could not update occupancy!!!"
        return False


def update_random():
    """predict best 2 channels out of 3"""
    channels = list(_db.get_collection('channels').find())
    if len(channels) > 0:
        # print "idle time prediction of all channels"
        all_idle_times = np.array([])
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
            for i in range(len(channel_seq)):
                if channel_seq[i]['idle']:
                    if idle_start == 0:
                        # idle_start = datetime.datetime.strptime(channel_seq[i]['date'],
                        #                                         '%Y-%m-%dT%X.%f%z').timestamp()
                        idle_start = (channel_seq[i]['date'] - datetime.datetime(1970, 1, 1)).total_seconds()
                        # print idle_start
                    if i > 0:
                        if channel_seq[i - 1]['idle']:
                            # idle_time = datetime.datetime.strptime(str(channel_seq[i]['date']),
                            #                                        '%Y-%m-%dT%X.%f%z').timestamp() - idle_start
                            idle_time = (channel_seq[i]['date'] - datetime.datetime(1970, 1,
                                                                                    1)).total_seconds() - idle_start
                            # print "Idle time is ", idle_time
                        else:
                            # idle_time = datetime.datetime.strptime(str(channel_seq[i]['date']),
                            #                                        '%Y-%m-%dT%X.%f%z').timestamp() - datetime.datetime.strptime(
                            #     channel_seq[i - 1]['date'], '%Y-%m-%dT%X.%f%z').timestamp()
                            idle_time = (channel_seq[i]['date'] - datetime.datetime(1970, 1, 1)).total_seconds() - (
                                    channel_seq[i - 1]['date'] - datetime.datetime(1970, 1, 1)).total_seconds()
                else:
                    if i > 0:
                        if channel_seq[i - 1]['idle']:
                            # print "Idle time is ", idle_time
                            idle_time_stats = np.append(idle_time_stats, idle_time)
                            idle_start = 0
            # print "Idle time is ", idle_time
            idle_time_stats = np.append(idle_time_stats, idle_time)

            for i in range(len(idle_time_stats)):
                all_idle_times = np.append(all_idle_times, idle_time_stats[i])
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
            # get total idle time
            total_idle_time = reduce(flatten, idle_time_stats)
            # print "Total idle time per channel", total_idle_time
            idle_best = 0
            median_time = np.median(all_idle_times)
            # print all_idle_times, median_time
            for i in range(len(idle_time_stats)):
                if idle_time_stats[i] >= median_time:
                    idle_best += idle_time_stats[i]
            if total_idle_time > 0:
                time_occ = idle_best / total_idle_time
                # print time_occ
                query = {'channel.fmin': channel['channel']['fmin'], 'channel.fmax': channel['channel']['fmax']}
                _db.get_collection('channels').find_one_and_update(query, {'$set': {'best_channel': time_occ}})

        return True
    else:
        print "Channel is empty, could not predict!!!"
        return False


def return_radio_chans(result):
    diff = float(result['signal']['amplitude']) - float(result['noise_floor'])
    if diff >= _THRESHOLD_DB:
        state = 'busy'
    else:
        state = 'free'
    print 'Channel state ', state
    return {
        'id': result['signal']['channel'],
        'state': state
    }


def get_t0(id):
    bit_seq = db_gen_seq(id)
    bit_seq = map(gen_seq, bit_seq)[::-1]
    t0 = 0
    for i in range(len(bit_seq)):
        if bit_seq[0] != 0:
            break
        else:
            if i > 0:
                if not (bit_seq[i - 1] and bit_seq[i]):
                    print bit_seq[i - 1], bit_seq[i]
                    t0 += 1
                else:
                    break
    return t0