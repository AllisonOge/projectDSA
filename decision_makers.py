# DECISION MAKERS.py
from pymongo import MongoClient
import sys
import numpy as np
import datetime

# database
myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA

_THRESHOLD_DB = 12
_SAMPLE_LEN = 12
_TRAFFIC_CLASS = 'UNKNOWN'


class TrafficClassification:
    """Classify traffic pattern for predictive selection"""

    def __init__(self, sequence_len, lag_samples):
        """initialize class to classify traffic of binary sequence
        of length while correlating the sequence at lag of lower length"""
        self.rxx = np.array([])
        self.tau_max = 0
        self.tau_local = 0
        self.tau_ave_arr = np.zeros((1, sequence_len))
        self.tau_ave = 0
        self.std = 0
        self.seq_pointer = 0
        if lag_samples < sequence_len:
            self.N = sequence_len
            self.m = lag_samples
        else:
            raise AttributeError("lag length has to be less than sequence length")

    def classify(self, bin_seq):
        """method to classify traffic based to time binary sequence"""
        if len(bin_seq) != self.N:
            raise ValueError("length of samples do not match the initialized length")
        self.rxx = np.correlate(bin_seq, bin_seq[self.m:-1], mode='valid')
        if max(self.rxx) > self.tau_max:
            self.tau_max = max(self.rxx)
            self.seq_pointer = self.N - list(self.rxx).index(max(self.rxx))
            # print "global maximum set to ", self.tau_max
        else:
            self.tau_ave_arr = np.append(self.tau_ave_arr,
                                         max(self.rxx) - self.tau_local)
            self.tau_local = max(self.rxx)
            # self.tau_ave_arr = np.append(self.tau_ave_arr,
            #                              self.seq_pointer + list(self.rxx).index(self.tau_local))
        self.seq_pointer += self.N
        # print self.seq_pointer
        # print "length of array is ", len(self.tau_ave_arr)
        self.tau_ave = np.average(self.tau_ave_arr)
        self.std = np.std(self.tau_ave_arr)
        # print self.tau_max, self.tau_ave, self.std
        if self.tau_ave == self.tau_max:
            print "Traffic is periodic with period ", self.tau_max
            return 'PERIODIC', self.tau_max
        elif self.std < 2 * self.tau_ave:
            print "Traffic is periodic with period ", int(self.tau_ave)
            return 'PERIODIC', int(self.tau_ave)
        else:
            # print "Traffic is stochastic"
            return 'STOCHASTIC', None


def flatten(prev, curr):
    return prev + curr


def gen_seq(obj):
    if obj['busy'] == True:
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
                    'busy': {'$gte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _THRESHOLD_DB]}}}]
            bit_seq = list(_db.get_collection("sensor").aggregate(filt))
            # classify bit sequence
            bit_seq = map(gen_seq, bit_seq)
            # print bit_seq
            if len(bit_seq) >= _SAMPLE_LEN:
                if len(bit_seq) % _SAMPLE_LEN == 0:
                    # print "Length of bit sequence", len(bit_seq)
                    for i in range(len(bit_seq) // _SAMPLE_LEN):
                        if 'traffic_classifier' not in locals():
                            traffic_classifier = TrafficClassification(_SAMPLE_LEN, 1)
                        # print len(bit_seq[_SAMPLE_LEN * i:_SAMPLE_LEN * (i + 1)])
                        traffic_class, period = traffic_classifier.classify(
                            bit_seq[_SAMPLE_LEN * i:_SAMPLE_LEN * (i + 1)])
                else:
                    traffic_class = _db.get_collection('time_distro').find_one({'channel_id': channel['_id']})[
                        'traffic_class']
            else:
                # default classifications
                traffic_class = 'UNKNOWN'
                period = 0

            result = map(lambda bit: bit == 1, bit_seq)
            spc = 0
            for i in range(len(result)):
                if result[i] == True:
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
        print "idle time prediction of all channels"
        all_idle_times = []
        for channel in channels:
            filt = [
                {'$match': {'signal.channel': channel['_id'], 'in_band': True}},
                {'$project': {
                    'date': 1,
                    'idle': {'$lt': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _THRESHOLD_DB]}}}]
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
                # else:
                #     print 'you missed this condition'
            idle_time_stats.append(idle_time)
            for i in range(len(idle_time_stats)):
                all_idle_times.append(idle_time_stats[i])
            # save to database
            if _db.get_collection("time_distro") is None:
                _db.create_collection("time_distro")
            if _db.get_collection("time_distro").find_one({'channel_id': channel['_id']}) is None:
                _db.get_collection("time_distro").insert_one(
                    {'channel_id': channel['_id'], 'idle_time_stats': idle_time_stats[:-2],
                     'mean_it': float(np.average(idle_time_stats)), 'traffic_class': _TRAFFIC_CLASS})
            else:
                _db.get_collection("time_distro").update_one({'channel_id': channel['_id']},
                                                             {'$set': {'idle_time_stats': idle_time_stats,
                                                                       'mean_it': float(np.average(idle_time_stats))}})
            # get total idle time
            total_idle_time = reduce(flatten, idle_time_stats)
            # print "Total idle time per channel", total_idle_time
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
        print "Channel is empty, could not predict!!!"
        return False


def return_radio_chans(chan_id):
    # print "Checking result of sensed radio channel", chan_id
    filt = [
        {'$match': {'signal.channel': chan_id}},
        {'$project': {'busy': {'$gte': [{'$subtract': ['$signal.amplitude', '$noise_floor']}, _THRESHOLD_DB]}}}]
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
