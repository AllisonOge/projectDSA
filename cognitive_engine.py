# a simplified approach of channel selection

import socket
import threading
import time
from pymongo import MongoClient

myclient = MongoClient('mongodb://127.0.0.1:27017/')
_db = myclient.projectDSA
_freq_array = [b'824e6', b'825e6', b'880e6', b'915e6', b'924e6', b'960e6', b'1710e6', b'1980e6', b'1989e6']

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

def main():
    # application initialization
    print "Application is initializing..."
    sense = initialization()
    # FOR TESTING
    for freq in _freq_array:
        msg = formatmsg(freq)
        # print msg
        sense.sendall(msg.encode('utf-8'))
        while sense.recv(10) != freq:
            sense.sendall(msg.encode('utf-8'))
    # query database for set of frequency based on time occupancy usage
    selected_chan = list(_db.channels.aggregate({'$project': {'channel': 1, 'selected': {'$gte': ['$channel.duty_cycle', 0.7]}}}))
    # check if set of channel is not empty
    if len(selected_chan) != 0:
        print selected_chan
    # sense selected channels to build prediction database and for free channel

    # update database information

    # check if no channel is free
    # query database for set of frequency based on time occupancy usage
    # select radio channel using prediction database
    # prompt transmission for remaining seconds
    # check if transmission has ended
    # halt communication system
    # if transmission has not ended
    # sense selected database channels to build prediction database and repeat process of selection

if __name__ == '__main__':
    main()