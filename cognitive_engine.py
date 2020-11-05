# a simplified approach of channel selection

import socket
import threading
import time
import numpy as np
import decision_makers
from pymongo import MongoClient


IP_ADDRESS = '127.0.0.1'
SENSOR_PORT = 12345
RF_PORT = 12347

def get_freq():
    freqs = []
    # start_freq = 898.25e6
    # stop_freq = 901.75e6
    # nchan = 7
    start_freq = 900e6
    stop_freq = 910e6
    nchan = 9
    step = (stop_freq - start_freq) / nchan
    for i in range(nchan):
        if i == 0:
            freqs.append(str(start_freq + step / 2))
        else:
            freqs.append(str(float(freqs[i - 1]) + step))
    print "Default frequencies are ", freqs
    return freqs


myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA
_freq_array = get_freq()

_counts = 12


def initialization():
    """Performs all kind of socket initialization"""
    # create a server INET, STREAMing socket for sensing process
    sensingsckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # rf_frontend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # bind to sensing socket port
    sensingsckt.bind((IP_ADDRESS, SENSOR_PORT))
    # bind to rf frontend socket port
    # rf_frontend.bind((IP_ADDRESS, RF_PORT))
    # listen for connection
    sensingsckt.listen(1)
    # accept sensing connection
    print "Waiting for sensor to connect..."
    (sensing, address) = sensingsckt.accept()
    print "sensor is connected to address", address
    # perform other socket initializations
    return sensing


def inband_sensing(sense, stop):
    while 1:
        timestamp = time.time()
        for freq in _freq_array:
            msg = formatmsg(freq)
            # print msg
            sense.sendall(msg.encode('utf-8'))
            while sense.recv(len(freq)) != freq:
                sense.sendall(msg.encode('utf-8'))
        print "Sensed {0} channels in {1} seconds".format(len(_freq_array), time.time() - timestamp)
        decision_makers.update_random()
        if stop():
            break


def formatmsg(msg):
    packet = '{length:<10}'.format(length=len(msg)) + msg
    return packet


def txtformat(lnght):
    fmt = ''
    if lnght > 1:
        fmt = 's'
    return fmt


def select_max(prev, curr):
    if curr['idle_time'] > prev['idle_time']:
        return curr
    else:
        return prev


def select_best(obj):
    if obj['card']['best_channel'] == 1:
        return obj


def main():
    _max_duty_cycle = 0.4
    idle = False
    chan = False
    pause = False
    # application initialization
    print "Application is initializing..."
    sense = initialization()
    inband_thread = threading.Thread(target=inband_sensing, args=[sense, lambda: pause], name='inband-sensing')
    inband_thread.start()
    print 'stated in-band sensing'
    while idle is False:
        while not chan:
            notempty = decision_makers.gen_class_est()
            # # populate database for the first time
            if notempty is False:
                print "populating database for 10 seconds"
                time.sleep(10)
                # i = 0
                # while i < _counts:
                #     for freq in _freq_array:
                #         msg = formatmsg(freq)
                #         # print msg
                #         sense.sendall(msg.encode('utf-8'))
                #         while sense.recv(len(freq)) != freq:
                #             sense.sendall(msg.encode('utf-8'))
                #     i += 1
                #     print '\rCreating database from default channel set for {c} number of times... {p:.2f}%'.format(
                #         c=_counts,
                #         p=(1.0 * i / _counts) * 100),
                # decision_makers.gen_class_est()
                # print
            # query database for set of frequency based on time occupancy usage
            filt = [{'$project': {'channel': 1, 'selected': {'$lte': ['$channel.occ_estimate', _max_duty_cycle]}}},
                    {'$match': {'selected': True}}]
            selected_chan = list(_db.get_collection('channels').aggregate(filt))
            # check if set of channel is not empty
            if len(selected_chan) > 0:
                print len(selected_chan), "channel{} selected".format(txtformat(len(selected_chan)))
                # sense selected channels to build prediction database and for free channel
                selected_freq = []
                chan_result = []
                # pause inband sensing
                pause = True
                inband_thread.join()
                wait_time = time.time()
                print 'in-band sensing is paused...'
                # FIXME move short term database to storage to reinitialize prediction
                for i in range(len(selected_chan)):
                    print 'sending prompt...'
                    msg = formatmsg('check')
                    # print msg
                    sense.sendall(msg.encode('utf-8'))
                    while sense.recv(len('check')) != 'check':
                        sense.sendall(msg.encode('utf-8'))
                    print 'prompt sent!'
                    centre_freq = selected_chan[i]['channel']['fmin'] + (
                            selected_chan[i]['channel']['fmax'] - selected_chan[i]['channel']['fmin']) / 2.0
                    msg = formatmsg(str(centre_freq))
                    # print msg
                    sense.sendall(msg.encode('utf-8'))
                    while sense.recv(len(str(centre_freq))) != str(centre_freq):
                        sense.sendall(msg.encode('utf-8'))
                    selected_freq.append(centre_freq)
                    # decision maker will return result of sensed channel
                    chan_result.append(decision_makers.return_radio_chans(selected_chan[i]['_id']))
                pause = False
                inband_thread = threading.Thread(target=inband_sensing, args=[sense, lambda: pause],
                                                 name='inband-sensing')
                inband_thread.start()
                print 'started in-band sensing after {} seconds'.format(time.time() - wait_time)
                # is a radio channel free
                # print chan_result
                for i in range(len(chan_result)):
                    try:
                        if chan_result[i]['state'] == 'free':
                            chan = True
                            break
                    except KeyError:
                        pass
            else:
                # check if no channel is free
                print "No channel available!!!"
                print 'previous threshold is ', _max_duty_cycle,
                if _max_duty_cycle < 1.0:
                    # increase by 10%
                    _max_duty_cycle += 0.1
                    print 'threshold for first set of channels is now ', _max_duty_cycle
                else:
                    print "Querying the entire database channel set!!!"
                # select radio channel using prediction database

        # traffic classification and prediction
        odds = []
        idle_times = []
        for i in range(len(chan_result)):
            # print chan_result[i]['id']
            query = {'$match': {'channel_id': chan_result[i]['id']}}
            chan_distro = _db.get_collection('time_distro').find_one(query['$match'])
            # print chan_distro
            if chan_distro['traffic_class'] == 'periodic':
                idle_time = _db.get_collection('time_distro').find_one({'channel_id': chan_result[i]['id']})
                idle_times.append(
                    {'idle_time': idle_time['mean_it'] * idle_time['period'], 'chan_id': chan_result[i]['id']})
            else:
                card = _db.get_collection('channels').find_one({'_id': chan_result[i]['id']})
                odds.append({'card': card, 'chan_id': chan_result[i]['id']})
        # select longest idle time or any best channel
        if len(idle_times) > 0:
            print idle_times
            chan_id = reduce(select_max, idle_times)['chan_id']
        if 'chan_id' not in locals():
            if len(odds) > 0:
                result = map(select_best, odds)
                for i in range(len(result)):
                    if result[i] is not None:
                        chan_id = result[i]['chan_id']
                    else:
                        chan_id = None
            else:
                print 'unexpected, FIXME!!!'

        print 'Selected channel is ', chan_id
        if chan_id is None:
            chan = False
        else:
            break


        # prompt transmission for remaining seconds
        # check if transmission has ended

    # halt communication system


if __name__ == '__main__':
    main()
