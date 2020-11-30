# !/usr/bin/env python3
# a simplified approach of channel selection

import socket
import threading
import time
import pickle
import numpy as np
import decision_makers
import random
import functools
import math
from pymongo import MongoClient
# from current folder
import utils

IP_ADDRESS = '10.0.0.1'
SENSOR_PORT = 12345
RF_PORT_TX = 12347
RF_PORT_RX = 12348

_MAX_DUTY_CYCLE = 0.4

myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA
_freq_array = utils.get_freq()

_DEFAULT_WAIT_TIME = 2.0
HEADERSIZE = 10


# _counts = 12


def initialization():
    """Performs all kind of socket initialization"""
    # create a server INET, STREAMing socket for sensing process
    sensingsckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rf_frontendsckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    myserver_rx = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    myserver_tx = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # bind to sensing socket port
    sensingsckt.bind((IP_ADDRESS, SENSOR_PORT))
    # bind to RF ports
    myserver_rx.bind((IP_ADDRESS, RF_PORT_RX))
    myserver_tx.bind((IP_ADDRESS, RF_PORT_TX))
    myserver_rx.listen(1)
    myserver_tx.listen(1)
    # listen for connection
    sensingsckt.listen(1)
    rf_frontendsckt.listen(1)
    # accept sensing connection
    print ("Waiting for sensor to connect...")
    (sensing, address) = sensingsckt.accept()
    print ("sensor is connected to address", address)
    print('Waiting for rx frontend...')
    rf_frontend_rx, address2 = myserver_rx.accept()
    print("RX connection made to {}".format(address2))
    print('Waiting for tx frontend...')
    rf_frontend_tx, address1 = myserver_tx.accept()
    print("TX connection made to {}".format(address1))
    print ("rf-frontend is connected to address", address2)
    # perform other socket initializations
    return sensing, rf_frontend_tx

# # python 3 version of threading with pause and resume control
# class InbandSensing(threading.Thread):
#     def __init__(self, sense, f):
#         super(InbandSensing, self).__init__(name='inband-sensing')
#         self.__flag = threading.Event()
#         self.__flag.set()
#         self.__running = threading.Event()
#         self.__running.set()
#         self.f = f
#         self.sense = sense
#         self.freq_array = _freq_array
#
#     def run(self):
#         while self.__running.isSet():
#             timestamp = time.time()
#             print ('Scanning frequencies.. ', self.freq_array)
#             for freq in self.freq_array:
#                 self.__flag.wait()
#                 msg = utils.formatmsg(freq)
#                 # print msg
#                 self.sense.sendall(msg.encode('utf-8'))
#                 while self.sense.recv(len(freq)) != freq.encode('utf-8'):
#                     print("hi", self.sense.recv(len(freq)), freq.encode('utf-8'))
#                     self.sense.sendall(msg.encode('utf-8'))
#                     break
#                 print("done")
#
#             sense_time = time.time() - timestamp
#             print ("Sensed {0} channels in {1} seconds".format(len(self.freq_array), sense_time))
#             if len(_freq_array) == len(self.freq_array):
#                 sec_per_bit = sense_time / len(_freq_array)
#                 if _db.get_collection('utils') is None:
#                     _db.create_collection('utils')
#                 if len(list(_db.get_collection('utils').find())) == 0:
#                     _db.get_collection('utils').insert_one({'sec_per_bit': sec_per_bit})
#                 else:
#                     prev_spb = list(_db.get_collection('utils').find())
#                     # print prev_spb
#                     if len(prev_spb) > 0:
#                         prev_spb = prev_spb[0]['sec_per_bit']
#                         query = {'sec_per_bit': prev_spb}
#                         _db.get_collection('utils').update_one(query,
#                                                                {'$set': {'sec_per_bit': sec_per_bit}})
#                     else:
#                         _db.create_collection('utils').insert_one({'sec_per_bit': sec_per_bit})
#             decision_makers.update_random()
#
#     def pause(self):
#         self.__flag.clear()
#
#     def resume(self, f):
#         self.__flag.set()
#         self.f = f
#         if self.f is not None:
#             try:
#                 # print f, freq_array
#                 self.freq_array = list(filter(lambda x: x != str(self.f), self.freq_array))
#                 print ("New set of frequencies are ", self.freq_array)
#             except ValueError:
#                 print ("No such frequency is available in choices")
#         else:
#             self.freq_array = _freq_array
#             print ("Frequency set is ", self.freq_array)
#
#     def stop(self):
#         self.__flag.set()
#         self.__running.clear()

# FIXME use threading stop control instead as socket code is not optimum
class InbandSensing(threading.Thread):
    def __init__(self, sense, f):
        super(InbandSensing, self).__init__(name='inband-sensing')
        self.__running = threading.Event()
        self.__running.set()
        self.__stop = False
        self.f = f
        self.sense = sense
        self.freq_array = _freq_array

    def run(self):
        if self.f is not None:
            # print f, freq_array
            self.freq_array = list(filter(lambda x: x != str(self.f), self.freq_array))
            print ("New set of frequencies are ", self.freq_array)
        while self.__running.isSet():
            timestamp = time.time()
            # print ('Scanning frequencies.. ', self.freq_array)
            for freq in self.freq_array:
                if self.__stop:
                    break
                msg = utils.formatmsg(freq)
                # print msg
                self.sense.sendall(msg.encode('utf-8'))
                while self.sense.recv(len(freq)) != freq.encode('utf-8'):
                    # print( self.sense.recv(len(freq)), freq.encode('utf-8'))
                    self.sense.sendall(msg.encode('utf-8'))
                    break
                # print("done")
            if self.__stop:
                break
            sense_time = time.time() - timestamp
            print ("Sensed {0} channels in {1} seconds".format(len(self.freq_array), sense_time))
            if len(_freq_array) == len(self.freq_array):
                sec_per_bit = sense_time
                if _db.get_collection('utils') is None:
                    _db.create_collection('utils')
                if len(list(_db.get_collection('utils').find())) == 0:
                    _db.get_collection('utils').insert_one({'sec_per_bit': sec_per_bit})
                else:
                    prev_spb = list(_db.get_collection('utils').find())
                    # print prev_spb
                    if len(prev_spb) > 0:
                        prev_spb = prev_spb[0]['sec_per_bit']
                        query = {'sec_per_bit': prev_spb}
                        _db.get_collection('utils').update_one(query,
                                                               {'$set': {'sec_per_bit': sec_per_bit}})
                    else:
                        _db.create_collection('utils').insert_one({'sec_per_bit': sec_per_bit})
            decision_makers.update_random()

    def stop(self):
        self.__stop = True
        self.__running.clear()



def main():
    with open('traffic_classification.pkl', 'rb') as f:
        model1 = pickle.load(f)
    print("classifier model loaded")
    _max_duty_cycle = _MAX_DUTY_CYCLE
    idle = False
    chan = False
    # application initialization
    print("Application is initializing...")
    sense, rf_frontend_tx = initialization()
    # # populate database at start for 30 minutes
    while input("Start DSA: ").lower() != 'y':
        print("Enter a valid input")
    # send prompt to TX_FRONTEND to switch
    msg = utils.formatmsg("SWITCH")
    rf_frontend_tx.sendall(msg.encode('utf-8'))
    while rf_frontend_tx.recv(len("SWITCH")) != "SWITCH".encode('utf-8'):
        rf_frontend_tx.sendall(msg.encode('utf-8'))
    inband_thread = InbandSensing(sense, None)
    inband_thread.start()
    # print 'stated in-band sensing'
    print("populating database for 2 minute to build prediction model")
    time.sleep(2 * 60)
    while idle is False:
        wait_time = time.time()
        inband_thread.stop()
        inband_thread.join()
        decision_makers.reset_time_distro()
        while not chan:
            # query database for set of frequency based on time occupancy usage
            filt = [{'$project': {'channel': 1, 'selected': {'$lte': ['$channel.occ_estimate', _max_duty_cycle]}}},
                    {'$match': {'selected': True}}]
            selected_chan = list(_db.get_collection('channels').aggregate(filt))
            print(len(selected_chan), "channel{} selected".format(utils.txtformat(len(selected_chan))))
            # sense selected channels to build prediction database and for free channel
            selected_freq = np.array([])
            free_channels = np.array([])
            for i in range(len(selected_chan)):
                # print ('sending prompt...')
                msg = utils.formatmsg('check')
                # print(msg)
                sense.sendall(msg.encode('utf-8'))
                while sense.recv(len('check')) != 'check'.encode('utf-8'):
                    sense.sendall(msg.encode('utf-8'))
                # print('prompt sent!')
                centre_freq = (selected_chan[i]['channel']['fmax'] + selected_chan[i]['channel']['fmin']) / 2.0
                msg = utils.formatmsg(str(centre_freq))
                print (msg)
                sense.sendall(msg.encode('utf-8'))
                while sense.recv(len(str(centre_freq))) != str(centre_freq).encode('utf-8'):
                    print (sense.recv(len(str(centre_freq))), str(centre_freq).encode('utf-8'))
                    sense.sendall(msg.encode('utf-8'))

                selected_freq = np.append(selected_freq, centre_freq)
                # get result of sensed channel from sensor
                msglen = sense.recv(HEADERSIZE)
                if msglen != ''.encode('utf-8'):
                    msg = sense.recv(int(msglen.decode('utf-8')))
                    # custom flushing hack
                    try:
                        sense.settimeout(.2)
                        msglen = sense.recv(HEADERSIZE)
                        msg = sense.recv(int(msglen.decode('utf-8')))  # throw value error when empty
                    except socket.timeout:
                        pass
                    sense.settimeout(None)
                    # print msg
                    sense.sendall(msg)
                    msg = pickle.loads(msg, encoding='bytes')
                    # print (msg)
                    result = decision_makers.return_radio_chans(msg)
                    if result['state'] == 'free':
                        chan = True
                        free_channels = np.append(free_channels, result)
                        if len(free_channels) >= int(2.0 / 3.0 * len(selected_chan)):
                            break
            # is any radio channel free
            if not chan and _max_duty_cycle < 1.0:
                # check if no channel is free
                print("No channel available at threshold of ", _max_duty_cycle)
                # increase by 10%
                _max_duty_cycle += 0.1
                print('threshold for first set of channels is now ', _max_duty_cycle)

        if _max_duty_cycle > _MAX_DUTY_CYCLE:
            _max_duty_cycle = _MAX_DUTY_CYCLE
        # traffic classification and prediction
        idle_time_prob_arr = np.array([])
        idle_times_arr = np.array([])
        for i in range(len(free_channels)):
            sec_per_bit = list(_db.get_collection('utils').find())[0]['sec_per_bit']
            # find t0 which is the time between the last sensed bit and the previous bit
            t0 = decision_makers.get_t0(free_channels[i]['id']) * sec_per_bit
            filt = {"channel_id": free_channels[i]['id']}
            successful, result = decision_makers.gen_class_est(free_channels[i], model1)
            chan_distro = _db.get_collection("time_distro").find_one(filt)
            if successful:
                if result['traffic_class'] == "STOCHASTIC":
                    if chan_distro['avg_idle_time'] == 0:
                        free_channels = np.delete(free_channels, {'id': free_channels[i]['id'], 'state': 'free'})
                    # get the probability of wait time distro within time before sensing free channel avg wait time
                    for j in range(t0, chan_distro["avg_idle_time"]):
                        idle_time_prob += (1 / chan_distro['avg_idle_time']) * math.exp(
                            -(j / chan_distro['avg_idle_time']))
                    print("Idle time probability of free channel is ", idle_time_prob, " for channel ", free_channels[i]['id'])
                    idle_time_prob_arr = np.append( idle_time_prob_arr, {'chan_id': free_channels[i]['id'],  'idle_time_prob': idle_time_prob, "idle_time": idle_time_prob*_DEFAULT_WAIT_TIME})
                else:
                    # compute idle time as (1-occ_est)*period - t0
                    occ_est = _db.get_collection('channels').find_one(filt)['channel']['occ_estimate']
                    print("Occupancy estimate is ", occ_est)
                    idle_time_value = (1 - occ_est) * result['period'] - t0
                    print('idle time of free channel is ', idle_time_value, " compared to mean idle time of ", chan_distro['avg_idle_time'])
                    print ('idle time is ', idle_time_value, " for channel ", free_channels[i]['id'])
                    idle_times_arr = np.append(idle_times_arr,
                        {'idle_time': idle_time_value, 'chan_id': free_channels[i]['id']})
            else:
                # accumulate idle times of all free channels
                idle_times_arr = np.append(idle_times_arr, {'chan_id': free_channels[i]['id'], 'mean_idle_time': chan_distro['avg_idle_time']-t0})

        # select best channel from free channels
        if len(idle_times_arr) > 0:
            # print (idle_times_arr)
            best_channel_periodic = functools.reduce(lambda x, y: x if x['idle_time'] > y['idle_time'] else y, idle_times_arr)
            selected_idle_time = best_channel_periodic['idle_time']
            chan_id = best_channel_periodic['chan_id']
        else:
            best_channel_stochastic = functools.reduce(lambda x, y: x if x['idle_time_prob'] > y['idle_time_prob'] else y, idle_time_prob_arr)
            chan_id = best_channel_stochastic['chan_id']
            selected_idle_time = best_channel_stochastic['idle_time'] or _DEFAULT_WAIT_TIME

        print ('Selected channel is ', chan_id)
        if not chan_id:
            chan = None
            break
        # prompt transmission for remaining idle time for a periodic channel or default_wait_time for stochastic channel
        channel = _db.get_collection('channels').find_one({'_id': chan_id})
        new_freq = (channel['channel']['fmax'] + channel['channel']['fmin']) / 2.0

        msg = utils.formatmsg('NEW_FREQ={}'.format(new_freq))
        rf_frontend_tx.send(msg.encode('utf-8'))
        # FIXME ensure a proper socket connection as while loop leads to a deadlock of the server and client for different communnication
        while rf_frontend_tx.recv(len('NEW_FREQ={}'.format(new_freq))) != 'NEW_FREQ={}'.format(new_freq).encode('utf-8'):
            rf_frontend_tx.send(msg.encode('utf-8'))

        print ("Transmitting for {} seconds".format(selected_idle_time))
        if selected_idle_time > 5.0:
            inband_thread = InbandSensing(sense, new_freq)
            inband_thread.start()
            print ('started in-band sensing after {} seconds'.format(time.time() - wait_time))
        time.sleep(selected_idle_time)
        # set long term database query to recently used channel
        _max_duty_cycle = _db.get_collection('channels').find_one({'_id': chan_id})['channel']['occ_estimate']
        db.rf_sensors.insert_one({'channel_id': chan_id, 'usage_time': selected_idle_time})
        if selected_idle_time > 5.0:
            inband_thread.stop()
            inband_thread.join()
            inband_thread = InbandSensing(sense, None)
            inband_thread.start()
            print ("in-band sensing has started")
        chan_id = None
        chan = False
        msg = utils.formatmsg('STOP_COMM')
        # print msg
        rf_frontend_tx.sendall(msg.encode('utf-8'))
        # FIXME ensure a proper socket connection as while loop leads to a deadlock of the server and client for different communnication
        while rf_frontend_tx.recv(len('STOP_COMM')) != 'STOP_COMM'.encode('utf-8'):
            rf_frontend_tx.sendall(msg.encode('utf-8'))
        # check if transmission has ended
        msglen = rf_frontend_tx.recv(HEADERSIZE)
        if len(msglen) > 0:
            msg = rf_frontend_tx.recv(int(msglen.decode('utf-8')))
            rf_frontend_tx.send(msg)
            if msg.split(b'=')[1] == 'True'.encode('utf-8'):
                idle = True
    # halt communication system
    print ("End of Communication..., DONE!")
    inband_thread.stop()
    inband_thread.join()


if __name__ == '__main__':
    try:
        main()
    except (socket.error, KeyboardInterrupt) as e:
        print (e)
        if _db.get_collection('long_term') is None:
            _db.get_collection('sensor').rename('long_term')
        else:
            if len(list(_db.get_collection('sensor').find())) > 0:
                _db.get_collection('long_term').insert_many(list(_db.get_collection('sensor').find()))
        _db.get_collection('sensor').drop()
        # _db.get_collection('time_distro').drop()
        # _db.get_collection('channels').drop()
