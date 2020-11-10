# a simplified approach of channel selection

import socket
import threading
import time
import pickle
import numpy as np
import utils
import decision_makers
from pymongo import MongoClient

IP_ADDRESS = '10.0.0.1'
SENSOR_PORT = 12345
RF_PORT = 12347

_MAX_DUTY_CYCLE = 0.4


def get_freq():
    freqs = []
    start_freq = 898.5e6
    stop_freq = 904.5e6
    nchan = 6
    # start_freq = 900e6
    # stop_freq = 910e6
    # nchan = 9
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

_DEFAULT_WAIT_TIME = 2
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
    print "Waiting for sensor to connect..."
    (sensing, address) = sensingsckt.accept()
    print "sensor is connected to address", address
    print "Waiting for rf-frontend to connect..."
    (rf_frontend, address2) = rf_frontendsckt.accept()
    print "rf-frontend is connected to address", address2
    # perform other socket initializations
    return sensing, rf_frontend


def inband_sensing(sense, stop):
    while 1:
        timestamp = time.time()
        for freq in _freq_array:
            msg = utils.formatmsg(freq)
            # print msg
            sense.sendall(msg.encode('utf-8'))
            while sense.recv(len(freq)) != freq:
                sense.sendall(msg.encode('utf-8'))
        print "Sensed {0} channels in {1} seconds".format(len(_freq_array), time.time() - timestamp)
        decision_makers.update_random()
        if stop():
            break

def select_max(prev, curr):
    if curr['idle_time'] > prev['idle_time']:
        return curr
    else:
        return prev


def select_best(obj):
    if obj['card']['best_channel'] > 0.9:
        return obj


def main():
    _max_duty_cycle = _MAX_DUTY_CYCLE
    idle = False
    chan = False
    pause = False
    # application initialization
    print "Application is initializing..."
    sense, rf_frontend = initialization()
    inband_thread = threading.Thread(target=inband_sensing, args=[sense, lambda: pause], name='inband-sensing')
    inband_thread.start()
    # print 'stated in-band sensing'
    while idle is False:
        print "HI"
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
                print len(selected_chan), "channel{} selected".format(utils.txtformat(len(selected_chan)))
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
                    msg = utils.formatmsg('check')
                    # print msg
                    sense.sendall(msg.encode('utf-8'))
                    while sense.recv(len('check')) != 'check':
                        sense.sendall(msg.encode('utf-8'))
                    print 'prompt sent!'
                    centre_freq = selected_chan[i]['channel']['fmin'] + (
                            selected_chan[i]['channel']['fmax'] - selected_chan[i]['channel']['fmin']) / 2.0
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
                    chan_result.append(decision_makers.return_radio_chans(msg))
                pause = False
                inband_thread = threading.Thread(target=inband_sensing, args=[sense, lambda: pause],
                                                 name='inband-sensing')
                inband_thread.start()
                print 'started in-band sensing after {} seconds'.format(time.time() - wait_time)
                decision_makers.gen_class_est()
                # is a radio channel free
                # print chan_result
                for i in range(len(chan_result)):
                    if chan_result[i]['state'] == 'free':
                        chan = True
                        break
            else:
                # check if no channel is free
                print "No channel available!!!",
                # print 'previous threshold is ', _max_duty_cycle,
                if _max_duty_cycle > 0.9:
                    print "Querying the entire database channel set!!!"
                else:
                    # increase by 10%
                    _max_duty_cycle += 0.1
                print 'threshold for first set of channels is now ', _max_duty_cycle

        _max_duty_cycle = _MAX_DUTY_CYCLE
        # traffic classification and prediction
        odds = []
        idle_times = []
        for i in range(len(chan_result)):
            # print chan_result[i]['id']
            query = {'$match': {'channel_id': chan_result[i]['id']}}
            chan_distro = _db.get_collection('time_distro').find_one(query['$match'])
            # print chan_distro
            if chan_distro['traffic_class'] == 'PERIODIC':
                idle_time = _db.get_collection('time_distro').find_one({'channel_id': chan_result[i]['id']})
                idle_times.append(
                    {'idle_time': idle_time['mean_it'] * idle_time['period'], 'chan_id': chan_result[i]['id']})
                print 'idle time is ', idle_time['mean_it'] * idle_time['period']
            else:
                card = _db.get_collection('channels').find_one({'_id': chan_result[i]['id']})
                odds.append({'card': card, 'chan_id': chan_result[i]['id']})
                # print odds
        # select longest idle time or any best channel
        if len(idle_times) > 0:
            print idle_times
            chan_id = reduce(select_max, idle_times)
            selected_idle_time = chan_id['idle_time']
            chan_id = chan_id['chan_id']
        if 'chan_id' not in locals():
            if len(odds) > 0:
                result = map(select_best, odds)
                for i in range(len(result)):
                    if result[i] is not None:
                        chan_id = result[i]['chan_id']
                        selected_idle_time = _DEFAULT_WAIT_TIME
                        break
                    else:
                        chan_id = None
            else:
                print 'unexpected, FIXME!!!'

        print 'Selected channel is ', chan_id
        if chan_id is None:
            chan = False
        else:
            # prompt transmission for remaining seconds
            channel = _db.get_collection('channels').find_one({'_id': chan_id})
            print channel['channel']['fmin'], channel['channel']['fmax']
            new_freq = channel['channel']['fmin'] + (
                    channel['channel']['fmax'] - channel['channel']['fmin']) / 2.0
            msg = utils.formatmsg('NEW_FREQ={}'.format(new_freq))
            rf_frontend.send(msg.encode('utf-8'))
            while rf_frontend.recv(len('NEW_FREQ={}'.format(new_freq))) != 'NEW_FREQ={}'.format(new_freq):
                rf_frontend.send(msg.encode('utf-8'))
            timestamp = time.time()
            print "Transmitting for {} seconds".format(selected_idle_time)
            while (time.time() - timestamp) < selected_idle_time:
                pass
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
    print "End of Communication..., DONE!"
    pause = True
    inband_thread.join()


if __name__ == '__main__':
    try:
        main()
    except socket.error or KeyboardInterrupt, e:
        print e
        if _db.get_collection('long_term') is None:
            _db.get_collection('sensor').rename('long_term')
        else:
            if len(list(_db.get_collection('sensor').find())) > 0:
                _db.get_collection('long_term').insert_many(list(_db.get_collection('sensor').find()))
        _db.get_collection('sensor').drop()
        # _db.get_collection('time_distro').drop()
        # _db.get_collection('channels').drop()
