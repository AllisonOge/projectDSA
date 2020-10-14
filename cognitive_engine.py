# a simplified approach of channel selection

import socket
import threading
import time
import decision_makers
from pymongo import MongoClient

myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA
_freq_array = [b'825e6', b'830e6', b'837e6', b'840e6', b'849e6', b'900e6', b'908e6', b'924e6', b'930e6', b'1710e6',
               b'1980e6', b'1989e6']
_counts = 3


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
    # application initialization
    print "Application is initializing..."
    sense = initialization()
    while True:
        decision_makers.update()
        # query database for set of frequency based on time occupancy usage
        filt = [{'$project': {'channel': 1, 'selected': {'$lte': ['$channel.duty_cycle', 0.7]}}},
                {'$match': {'selected': True}}]
        selected_chan = list(_db.get_collection('channels').aggregate(filt))
        # check if set of channel is not empty
        if len(selected_chan) != 0:
            # sense selected channels to build prediction database and for free channel
            selected_freq = []
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
                decision_makers.return_radio_chans(selected_chan[i]['_id'])
            print len(selected_freq), "channel{} selected".format(txtformat(len(selected_freq)))
        else:
            # check if no channel is free
            # query database for set of frequency based on time occupancy usage
            print "No channel available!!!"
            # select radio channel using prediction database
            # prompt transmission for remaining seconds
            # check if transmission has ended
            # halt communication system
            # if transmission has not ended
            # sense selected database channels to build prediction database and repeat process of selection

            i = 0
            while i < _counts:
                for freq in _freq_array:
                    print '\rCreating database from default channel set for {c} number of times... {p}%'.format(c=_counts,
                                                                                                                p=( 1.0 * i / _counts) * 100),
                    msg = formatmsg(freq)
                    # print msg
                    sense.sendall(msg.encode('utf-8'))
                    while sense.recv(len(freq)) != freq:
                        sense.sendall(msg.encode('utf-8'))
                i += 1

            print

if __name__ == '__main__':
    main()
