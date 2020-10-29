# a simplified approach of channel selection

import socket
import threading
import time
import numpy as np
import decision_makers
from pymongo import MongoClient


def get_freq():
    freqs = []
    start_freq = 898.25e6
    stop_freq = 901.75e6
    nchan = 7
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
    # sensingsckt.setblocking(0)
    # bind the sensing socket to localhost
    sensingsckt.bind(('localhost', 12345))
    # listen for connection
    sensingsckt.listen(1)
    # accept sensing connection
    print "Waiting for sensing module to connect..."
    (sensing, address) = sensingsckt.accept()
    print "Sensing module connected to address", address
    # perform other socket initializations
    return sensing


def formatmsg(msg):
    packet = '{length:<10}'.format(length=len(msg)) + msg
    return packet


def txtformat(lnght):
    fmt = ''
    if lnght > 1:
        fmt = 's'
    return fmt


def main():
    idle = False
    # application initialization
    print "Application is initializing..."
    sense = initialization()
    while idle is False:

        while 1:
            notempty = decision_makers.update()
            # populate database for the first time
            if notempty is False:
                i = 0
                while i < _counts:
                    for freq in _freq_array:
                        msg = formatmsg(freq)
                        # print msg
                        sense.sendall(msg.encode('utf-8'))
                        while sense.recv(len(freq)) != freq:
                            sense.sendall(msg.encode('utf-8'))
                    i += 1
                    print '\rCreating database from default channel set for {c} number of times... {p:.2f}%'.format(
                        c=_counts,
                        p=(1.0 * i / _counts) * 100),
                print
            # query database for set of frequency based on time occupancy usage
            # if starting:
            #     filt = [{'$project': {'channel': 1, 'selected': {'$lte': ['$channel.duty_cycle', 0.7]}}},
            #             {'$match': {'selected': True}}]
            #     starting = False
            # else:
            #     filt = [{'$project': {'channel': 1, 'selected': {'$gt': ['$nselected', 0]}}},
            #         {'$match': {'selected': True}}]
            filt = [{'$project': {'channel': 1, 'selected': {'$lte': ['$channel.duty_cycle', 0.4]}}},
                    {'$match': {'selected': True}}]
            selected_chan = list(_db.get_collection('channels').aggregate(filt))
            # check if set of channel is not empty
            if len(selected_chan) != 0:
                print len(selected_chan), "channel{} selected".format(txtformat(len(selected_chan)))
                # sense selected channels to build prediction database and for free channel
                selected_freq = []
                chan_result = []
                for i in range(len(selected_chan)):
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
                # is a radio channel free
                try:
                    chan_result.index('free')
                    break
                except ValueError:
                    pass
            else:
                # check if no channel is free
                print "No channel available!!!"
                # select radio channel using prediction database
        # store sensed result for traffic classification and prediction
        for i in range(len(chan_result)):
            query = {'$match': {'_id': chan_result[i]['id']}}
            nselected = _db.get_collection('channels').find_one(query)
            nselected += 1
            _db.get_collection('channels').update_one(query, nselected)
        # decide and use prediction method for the classes of traffic

        # prompt transmission for remaining seconds
        # check if transmission has ended

    # halt communication system



if __name__ == '__main__':
    main()
