# DECISION MAKERS.py
from pymongo import MongoClient
import sys
import numpy as np
import datetime

# database
myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA

_threshold_db = 10


def flatten(prev, curr):
    return prev + curr


def gen_seq(bit):
    if bit == True:
        return 1
    else:
        return 0


def gen_class_est():
    """in-band sequence generator, classification and occupancy estimation"""
    channels = list(_db.get_collection('channels').find())
    if len(channels) > 0:
        print "Updating the traffic estimate for all channels"
        for channel in channels:
            filt = [
                {'$match': {'signal.channel': channel['_id']}},
                {'$project': {
                    'busy': {'$gte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _threshold_db]}}}]
            bit_seq = list(_db.get_collection("sensor").aggregate(filt))
            # classify bit sequence
            bit_seq = map(gen_seq, bit_seq)
            traffic_class = 'stochastic'
            result = map(lambda bit: bit == 1, bit_seq)
            spc = 0
            for i in range(len(result)):
                if result[i] == True:
                    spc += 1
            # print len(spc), channel['channel']['counts']
            newdc = float(spc) / float(channel['channel']['counts'])
            print 'new traffic estimate is ', newdc
            query = {'channel.fmin': channel['channel']['fmin'], 'channel.fmax': channel['channel']['fmax']}
            _db.get_collection('channels').find_one_and_update(query, {'$set': {'channel.occ_estimate': newdc}})
            # update database
            if _db.get_collection("time_distro") is None:
                print 'time distro has not being initialized yet, FIXME!!!'
                pass
            else:
                if traffic_class == 'periodic':
                    _db.get_collection("time_distro").update_one({'channel_id': channel['_id']},
                                                                 {'$set': {'period': 0}})
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
        print "Updating the traffic estimate for all channels"
        all_idle_times = []
        for channel in channels:
            filt = [
                {'$match': {'signal.channel': channel['_id'], 'in_band': True}},
                {'$project': {
                    'date': 1,
                    'idle': {'$lt': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _threshold_db]}}}]
            channel_seq = list(_db.get_collection("sensor").aggregate(filt))
            idle_start = 0
            idle_time = 0
            idle_time_stats = []
            for i in range(len(channel_seq)):
                if channel_seq[i]['idle'] == True and idle_start == 0:
                    idle_start = channel_seq[i]['date']
                elif channel_seq[i]['idle'] == True and type(idle_start) != int:
                    idle_time = (channel_seq[i]['date'] - idle_start).total_seconds()
                elif channel_seq[i]['idle'] == False and idle_time > 0:
                    idle_time_stats.append(idle_time)
                    idle_start = 0
                else:
                    print 'you missed this condition'
            idle_time_stats.append(idle_time)
            for i in range(len(idle_time_stats)):
                all_idle_times.append(idle_time_stats[i])
            # save to database
            if _db.get_collection("time_distro") is None:
                _db.create_collection("time_distro")
            if _db.get_collection("time_distro").find_one({'channel_id': channel['_id']}) is None:
                _db.get_collection("time_distro").insert_one(
                    {'channel_id': channel['_id'], 'idle_time_stats': idle_time_stats[:-2],
                     'mean_it': float(np.average(idle_time_stats)), 'traffic_class': 'unknown'})
            else:
                _db.get_collection("time_distro").update_one({'channel_id': channel['_id']},
                                                             {'$set': {'idle_time_stats': idle_time_stats,
                                                                       'mean_it': float(np.average(idle_time_stats))}})
            # get total idle time
            total_idle_time = reduce(flatten, idle_time_stats)
            print "Total idle time per channel", total_idle_time
            idle_best = 0
            # select 2 out of 3 channels
            ind = int((1.0 / 3.0) * len(all_idle_times))
            median_time = sorted(all_idle_times)[ind]
            # print all_idle_times, median_time
            for i in range(len(idle_time_stats)):
                if idle_time_stats[i] > median_time:
                    idle_best += idle_time_stats[i]
            if total_idle_time != 0:
                time_occ = idle_best / total_idle_time
                # print time_occ
                query = {'channel.fmin': channel['channel']['fmin'], 'channel.fmax': channel['channel']['fmax']}
                _db.get_collection('channels').find_one_and_update(query, {'$set': {'best_channel': time_occ}})

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
