# !/usr/bin/env python3
# a simplified approach of channel selection

import socket
import threading
import time
import pickle
import numpy as np
import utils
import decision_makers
import random
import math
from pymongo import MongoClient

IP_ADDRESS = '10.0.0.1'
SENSOR_PORT = 12345
RF_PORT = 12347

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
    # bind to sensing socket port
    sensingsckt.bind((IP_ADDRESS, SENSOR_PORT))
    # bind to rf frontend socket port
    rf_frontendsckt.bind((IP_ADDRESS, RF_PORT))
    # listen for connection
    sensingsckt.listen(1)
    rf_frontendsckt.listen(1)
    # accept sensing connection
    print ("Waiting for sensor to connect...")
    (sensing, address) = sensingsckt.accept()
    print ("sensor is connected to address", address)
    print ("Waiting for rf-frontend to connect...")
    (rf_frontend, address2) = rf_frontendsckt.accept()
    print ("rf-frontend is connected to address", address2)
    # perform other socket initializations
    return sensing, rf_frontend


def inband_sensing(sense, stop, f=None):
    freq_array = _freq_array
    if f is not None:
        try:
            # print f, freq_array
            freq_array = filter(lambda x: x != str(f), freq_array)
            print ("New set of frequencies are ", freq_array)
        except ValueError:
            print ("No such frequency is available in choices")

    while 1:
        timestamp = time.time()
        for freq in freq_array:
            if stop():
                break
            msg = utils.formatmsg(freq)
            # print msg
            sense.sendall(msg.encode('utf-8'))
            while sense.recv(len(freq)) != freq:
                sense.sendall(msg.encode('utf-8'))
        if stop():
            break
        sense_time = time.time() - timestamp
        print ("Sensed {0} channels in {1} seconds".format(len(freq_array), sense_time))
        if len(_freq_array) == len(freq_array):
            sec_per_bit = sense_time / len(_freq_array)
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


def select_max(prev, curr):
    if curr['idle_time'] > prev['idle_time']:
        return curr
    else:
        return prev

def select_best(obj):
    if obj['card']['best_channel'] > 0.6:
        return obj

def select_least(prev, obj):
    # select stochastic channels of idle prob greater than 60%
    if obj['card']['best_channel'] > 0.6:
        # pick the best idle time distro of lesser prob of exponential idle time
        if obj['idle_time_prob'] < prev['idle_time_prob']:
            return obj
        else:
            return prev


def main():
    with open('traffic_classification.pkl', 'rb') as f:
        model1 = pickle.load(f)
    print("classifier model loaded")
    _max_duty_cycle = _MAX_DUTY_CYCLE
    idle = False
    chan = False
    pause = False
    # application initialization
    print("Application is initializing...")
    sense, rf_frontend = initialization()
    inband_thread = threading.Thread(target=inband_sensing, args=[sense, lambda: pause], name='inband-sensing')
    inband_thread.start()
    # print 'stated in-band sensing'
    # # populate database at start for 30 minutes
    # print("populating database for 30 minutes to build prediction model")
    # time.sleep(30 * 60)
    while idle is False:
        while not chan:
            decision_makers.gen_class_est(model1)
            # query database for set of frequency based on time occupancy usage
            filt = [{'$project': {'channel': 1, 'selected': {'$lte': ['$channel.occ_estimate', _max_duty_cycle]}}},
                    {'$match': {'selected': True}}]
            selected_chan = list(_db.get_collection('channels').aggregate(filt))
            # check if set of channel is not empty
            if len(selected_chan) > 0:
                print(len(selected_chan), "channel{} selected".format(utils.txtformat(len(selected_chan))))
                # sense selected channels to build prediction database and for free channel
                selected_freq = []
                chan_result = []
                # pause inband sensing
                pause = True
                inband_thread.join()
                wait_time = time.time()
                # print 'in-band sensing is paused...'
                # FIXME move short term database to storage to reinitialize prediction
                for i in range(len(selected_chan)):
                    # print 'sending prompt...'
                    msg = utils.formatmsg('check')
                    # print msg
                    sense.sendall(msg.encode('utf-8'))
                    while sense.recv(len('check')) != 'check':
                        sense.sendall(msg.encode('utf-8'))
                    # print 'prompt sent!'
                    centre_freq = (selected_chan[i]['channel']['fmax'] + selected_chan[i]['channel']['fmin']) / 2.0
                    msg = utils.formatmsg(str(centre_freq))
                    # print msg
                    sense.sendall(msg.encode('utf-8'))
                    while sense.recv(len(str(centre_freq))) != str(centre_freq):
                        sense.sendall(msg.encode('utf-8'))
                    selected_freq.append(centre_freq)
                    # get result of sensed channel from sensor
                    msglen = sense.recv(HEADERSIZE)
                    while len(msglen) > 0:
                        msg = sense.recv(int(msglen.decode('utf-8')))
                        # print msg
                        sense.sendall(msg)
                        msglen = ''.encode('utf-8')
                        msg = pickle.loads(msg)
                        # print msg
                    check = decision_makers.return_radio_chans(msg)
                    if check['state'] == 'free':
                        chan = True
                        chan_result.append(check)
                        if len(chan_result) >= int(2.0 / 3.0 * len(selected_chan)):
                            break

                decision_makers.gen_class_est(model1)
                # is a radio channel free
                # print chan_result
                if chan == True:
                    break
            else:
                # check if no channel is free
                print("No channel available!!!",)
                # print 'previous threshold is ', _max_duty_cycle,
                if _max_duty_cycle > 0.9:
                    print("Querying the entire database channel set!!!")
                else:
                    # increase by 10%
                    _max_duty_cycle += 0.1
                print('threshold for first set of channels is now ', _max_duty_cycle)

        if _max_duty_cycle > _MAX_DUTY_CYCLE:
            _max_duty_cycle -= 0.1
        # traffic classification and prediction
        odds = []
        idle_times = []
        for i in range(len(chan_result)):
            # print chan_result[i]['id']
            query = {'$match': {'channel_id': chan_result[i]['id']}}
            chan_distro = _db.get_collection('time_distro').find_one(query['$match'])
            # print chan_distro
            if chan_distro['traffic_class'] == 'PERIODIC':
                filt = {'channel_id': chan_result[i]['id']}
                idle_time = _db.get_collection('time_distro').find_one(filt)
                # get time per bit from utils collection
                if _db.get_collection('utils'):
                    sec_per_bit = list(_db.get_collection('utils').find())[0]['sec_per_bit']
                else:
                    # assume 1 second
                    sec_per_bit = 1
                # find t0 which is the time between the last sensed bit and the previous bit
                t0 = decision_makers.get_t0(chan_result[i]['id']) * sec_per_bit
                # compute idle time as (1-occ_est)*period - t0
                occ_est = _db.get_collection('channels').find_one(filt)
                print("Occupancy estimate is ", occ_est)
                occ_est = occ_est['channel']['occ_estimate']
                idle_time_value = (1 - occ_est) * idle_time['period'] - t0
                idle_times.append(
                    {'idle_time': idle_time_value, 'chan_id': chan_result[i]['id']})
                print ('idle time is ', idle_time_value)
            else:
                card = _db.get_collection('channels').find_one({'_id': chan_result[i]['id']})
                # use best_chan to generate wait time exponential distro
                idle_time_prob = 0
                # get the probability of wait time distro within 9 seconds and 1 minute 30 secs
                if chan_distro['avg_idle_time'] > 0:
                    for j in range(9, 90):
                        idle_time_prob += (1 / chan_distro['avg_idle_time']) * math.exp(
                            -(j / chan_distro['avg_idle_time']))
                    print("Idle time probability is ", idle_time_prob, " for channel ", chan_result[i]['id'])
                    odds.append({'card': card, 'idle_time_prob': idle_time_prob, 'chan_id': chan_result[i]['id']})
                else:
                    odds.append({'card': card, 'chan_id': chan_result[i]['id']})
# print odds
        # select longest idle time or any best channel
        if len(idle_times) > 0:
            print (idle_times)
            if len(idle_times) > 1:
                chan_id = reduce(select_max, idle_times)
                selected_idle_time = chan_id['idle_time']
                chan_id = chan_id['chan_id']
            else:
                chan_id = idle_times[0]['idle_time']
        else:
            if len(odds) > 0:
                if len(odds) > 1:
                    result = reduce(select_least, odds)
                    if result:
                        chan_id = result['chan_id']
                        selected_idle_time = _DEFAULT_WAIT_TIME
                    else:
                        result = map(select_best, odds)
                        for i in range(len(result)):
                            if result[i]:
                                chan_id = result[i]['chan_id']
                                selected_idle_time = _DEFAULT_WAIT_TIME
                                break
                            else:
                                chan_id = None
                else:
                    chan_id = odds[0]['chan_id']
                    selected_idle_time = _DEFAULT_WAIT_TIME
            else:
                print ('unexpected as channel set is not empty, FIXME!!!')

        print ('Selected channel is ', chan_id)
        if chan_id is None:
            chan = False
        else:
            # prompt transmission for remaining seconds
            channel = _db.get_collection('channels').find_one({'_id': chan_id})
            new_freq = (channel['channel']['fmax'] + channel['channel']['fmin']) / 2.0

            msg = utils.formatmsg('NEW_FREQ={}'.format(new_freq))
            rf_frontend.send(msg.encode('utf-8'))
            while rf_frontend.recv(len('NEW_FREQ={}'.format(new_freq))) != 'NEW_FREQ={}'.format(new_freq):
                rf_frontend.send(msg.encode('utf-8'))
            timestamp = time.time()
            print ("Transmitting for {} seconds".format(selected_idle_time))
            pause = False
            inband_thread = threading.Thread(target=inband_sensing, args=[sense, lambda: pause, new_freq],
                                             name='inband-sensing')
            inband_thread.start()
            print ('started in-band sensing after {} seconds'.format(time.time() - wait_time))
            time.sleep(selected_idle_time)
            pause = True
            inband_thread.join()
            pause = False
            inband_thread = threading.Thread(target=inband_sensing, args=[sense, lambda: pause],
                                             name='inband-sensing')
            inband_thread.start()
            chan_id = None
            chan = False
            msg = utils.formatmsg('STOP_COMM')
            # print msg
            rf_frontend.sendall(msg.encode('utf-8'))
            while rf_frontend.recv(len('STOP_COMM')) != 'STOP_COMM'.encode('utf-8'):
                rf_frontend.sendall(msg.encode('utf-8'))
            # check if transmission has ended
            msglen = rf_frontend.recv(HEADERSIZE)
            if len(msglen) > 0:
                msg = rf_frontend.recv(int(msglen.decode('utf-8')))
                rf_frontend.send(msg)
                if msg.split('=')[1] == 'True':
                    idle = True
    # halt communication system
    print ("End of Communication..., DONE!")
    pause = True
    inband_thread.join()


if __name__ == '__main__':
    try:
        main()
    except (socket.error, KeyboardInterrupt ) as e:
        print (e)
        if _db.get_collection('long_term') is None:
            _db.get_collection('sensor').rename('long_term')
        else:
            if len(list(_db.get_collection('sensor').find())) > 0:
                _db.get_collection('long_term').insert_many(list(_db.get_collection('sensor').find()))
        _db.get_collection('sensor').drop()
        # _db.get_collection('time_distro').drop()
        # _db.get_collection('channels').drop()
