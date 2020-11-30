# !/usr/bin/env python3

from pymongo import MongoClient
from bson.code import Code

import sys
import numpy as np
from matplotlib import pyplot as plt
import functools
import math

myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA

_THRESHOLD = 0.6
_WAIT_TIME = 2.0

# mapper = Code("""
#             function () {
#                 emit( 'occ_estimate',this.busy === "true" ? 1 : 0)
#             }
#             """)

# reducer = Code("""
#             function (key, values) {
#                 var total = 0
#                 for(var i = 0; i < values.length; i++){
#                     total += values[i]
#                 }
#                 return total / values.length
#             }
#             """)


def get_nonzeros(prev, x):
    if type(prev) != np.ndarray:
        prev = np.array([prev])
    if float(x) != 0.0:
        prev = np.append(prev, x)
    return prev

class TrafficClassification:
    """Classify traffic pattern for predictive selection"""

    def __init__(self, sequence_len):
        """initialize class to classify traffic of binary sequence
        of length while correlating the sequence edge [0, 1]"""
        self.rxx = np.array([])
        self.corr_stats = np.array([])
        self.tau_ave = 0
        self.std = 0
        self.N = sequence_len

    def classify(self, bin_seq):
        """method to classify traffic based to time binary sequence"""
        if len(bin_seq) != self.N:
            raise ValueError("length of samples do not match the initialized length")
        self.rxx = np.correlate(bin_seq, [0, 1], mode='valid')
        sep = np.array([])
        for i in range(len(self.rxx)):
            if i > 0:
                if self.rxx[i - 1] == 0 and self.rxx[i] == 1:
                    sep = np.append(sep, 1)
                else:
                    if len(sep) > 0:
                        sep[-1] += 1
        # print(sep)
        if self.rxx[i - 1] == 0 and self.rxx[i] == 1:
                    sep = np.append(sep, 0)
        else:
            if len(sep) > 1:
                sep = sep[:-1]
            else:
                sep = np.array([self.N])
                sys.stderr.write('WARNING: Choose a larger sample size for better prediction\n')
        # print(sep)
        sep = functools.reduce(get_nonzeros, sep)
        self.corr_stats = np.append(self.corr_stats, sep)
        
        self.tau_ave = np.average(self.corr_stats)
        self.std = np.std(self.corr_stats)
        if self.tau_ave == 0.0:
            print("Choose a larger sample size")
            sys.exit(1)
        if self.std == 0.0 or self.std < 0.3*self.tau_ave:
            # print (self.tau_ave, self.std, self.corr_stats)
            print ("Traffic is periodic with period ", round(self.tau_ave))
            return 'PERIODIC', round(self.tau_ave)
        else:
            print ("Traffic is stochastic")
            # print (self.tau_ave, self.std)
            return 'STOCHASTIC', None

    def get_corr(self):
        return self.rxx

if __name__ == "__main__":
    # result = _db.get_collection('bit_sequence_1').map_reduce(mapper, reducer, 'occ_estimate')

    # use 50 sequences to query best set of channels
    chan = False
    num_of_channels = 6
    sample_len = 59
    

    names = ['bit_sequence_1', 'bit_sequence_2', 'bit_sequence_3', 'bit_sequence_4', 'bit_sequence_5', 'bit_sequence_6']
    sizes = np.array([])
    for i in range(num_of_channels):
        sizes = np.append(sizes,  _db.get_collection(names[i]).find().count())

    print(sizes)
    num_of_iterations = int(min(sizes)//sample_len)
    # print(num_of_iterations)
   

    for n in range(num_of_iterations):
        deadlock = False
        bit_TF_arr = [[], ] * num_of_channels
        bit_TF_plot_arr = [[], ] * num_of_channels
        bit_seq_arr = [[], ] * num_of_channels
        bit_seq_plot_arr = [[], ] * num_of_channels
        fig, ax = plt.subplots(num_of_channels, 1)
        print("Sensing database...")
        for i in range(num_of_channels):
            bit_TF_arr[i] = list(_db.get_collection(names[i]).find()[n*sample_len:(n+1)*sample_len])
            bit_TF_plot_arr[i] = list(_db.get_collection(names[i]).find()[n*sample_len:(n+1)*sample_len+5])
            # print(bit_TF_arr)
            bit_seq_arr[i] = [1 if bit['busy'] == "true" else 0 for bit in bit_TF_arr[i]]
            bit_seq_plot_arr[i] = [1 if bit['busy'] == "true" else 0 for bit in bit_TF_plot_arr[i]]
            # visualization of smartness
            ax[i].step(range(0, len(bit_seq_plot_arr[i])), bit_seq_plot_arr[i])

        
        # print(bit_seq_arr, len(bit_seq_arr[0]))

        # get occupancy usage of channels
        occ_est_arr = np.array([])
        for i in range(num_of_channels):
            occ_est_arr = np.append(occ_est_arr, functools.reduce(lambda x, y: y + x, bit_seq_arr[i]))
        occ_est_arr = occ_est_arr / sample_len

        while not chan:
            selected_chan = np.where(occ_est_arr < _THRESHOLD)
            print("Long term query gave the following channel set ", selected_chan[0], " using time occupancy threshold of ", _THRESHOLD)
            # print(occ_est_arr , selected_chan)
            # is any channel free
            free_channels = np.array([])
            for i in range(len(selected_chan[0])):
                # print(list(_db.get_collection(names[int(selected_chan[i][0])]).find()[(n+1)*sample_len+1:(n+1)*sample_len+2]))
                ind = (n+1)*sample_len+1
                result = list(_db.get_collection(names[int(selected_chan[0][i])]).find()[ind:ind+1])[0]
                if result['busy'] == "true":
                    print("Channel {} is busy".format(selected_chan[0][i]))
                else:
                    print("Channel {} is free".format(selected_chan[0][i]))
                    free_channels = np.append(free_channels, selected_chan[0][i])
                    # print(len(selected_chan), round(2.0 / 3.0 * len(selected_chan)))
                    chan = True
                    if len(free_channels) >= round(2.0 / 3.0 * len(selected_chan[0])):
                        break
            if _THRESHOLD < 1.0 and chan != True:
                _THRESHOLD += 0.1
                print("Threshold is now ", _THRESHOLD)
            if int(_THRESHOLD) == 1 and chan != True:
                deadlock = True
                break
        if deadlock:
            _THRESHOLD = 0.6
            continue
        if _THRESHOLD > 0.6:
            _THRESHOLD = 0.6
        # channel prediction and selection
        print('My free channels are ', free_channels)
        chan = False
        # check channel classification
        traffic_classifier = TrafficClassification(sample_len)
        idle_prob_arr = np.array([])
        idle_time_arr = np.array([])
        for i in range(len(free_channels)):
            # print(len(bit_seq_arr[int(free_channels[i])]))
            traffic_class, period = traffic_classifier.classify(bit_seq_arr[int(free_channels[i])])
            print(traffic_class, period)
            bit_seq = list(_db.get_collection(names[int(free_channels[i])]).find()[:(n+1)*sample_len+2])[::-1]
            reversed_bit_seq = [1 if bit['busy'] == "true" else 0 for bit in bit_seq]
            t0 = 0
            for j in range(len(reversed_bit_seq)):
                if reversed_bit_seq[j] == 0:
                    if j > 0:
                        if reversed_bit_seq[j-1] == 0:
                            t0 += 1
                else:
                    if j == 0:
                        print("Unexpected!!!")
                    break
            print("Time before channel is sensed free is ", t0, "occupancy estimate is ", occ_est_arr[int(free_channels[i])])
            if traffic_class == "STOCHASTIC":
                # how long do I stay on this channel
                idle_prob = 0
                for j in np.arange(t0, _WAIT_TIME):
                    idle_prob += (1/(_WAIT_TIME*(1-occ_est_arr[int(free_channels[i])]))) * math.exp(-(j/(_WAIT_TIME*(1-occ_est_arr[int(free_channels[i])]))))
                print("Idle probability is ", idle_prob)
                idle_prob_arr = np.append(idle_prob_arr, {'chan_id': int(free_channels[i]), 'idle_time_prob': idle_prob, 'idle_time': _WAIT_TIME*(1-occ_est_arr[int(free_channels[i])])})
                print("Idle probability of free channel is ", idle_prob, " of idle time ", _WAIT_TIME*(1-occ_est_arr[int(free_channels[i])]))
            else: 
                idle_time = (1 - occ_est_arr[int(free_channels[i])]) * period - t0
                idle_time_arr = np.append(idle_time_arr, {'chan_id': int(free_channels[i]), 'idle_time':idle_time})
                print("Idle time for periodic channel is ", idle_time)
        best_channel_periodic = best_channel_stochastic = 0
        if len(idle_time_arr) > 0:
            best_channel_periodic = functools.reduce(lambda x, y: x if x['idle_time'] > y['idle_time'] else y, idle_time_arr)
            print("Channel ", best_channel_periodic['chan_id'], " selected with highest idle time of ", best_channel_periodic['idle_time'])

        if len(idle_prob_arr) > 0:
            best_channel_stochastic = functools.reduce(lambda x, y: x if x['idle_time_prob'] > y['idle_time_prob'] else y, idle_prob_arr)
            print("Channel ", best_channel_stochastic['chan_id'], " selected with highest idle time prob of ", best_channel_stochastic['idle_time_prob'])

        # choice between stochastic and periodic as best channels is based on highest idle time
        if best_channel_periodic and best_channel_stochastic:
            best_channel = functools.reduce(lambda x, y: x if x['idle_time'] > y['idle_time'] else y, np.array([best_channel_periodic, best_channel_stochastic]))
            print("Channel ", best_channel['chan_id'], " selected with highest idle time of ", best_channel['idle_time'])

    # visualization
    plt.show()