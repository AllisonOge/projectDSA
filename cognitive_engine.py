# a simplified approach of channel selection

import socket
import threading
import time


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

def main():
    # application initialization
    print "Application is initializing..."
    sense = initialization()
    freq_array = [b'905e6', b'880e6', b'915e6', b'924e6', b'989e6']
    for freq in freq_array:
        msg = '{length:<10}'.format(length=len(freq)) + freq
        # print msg
        sense.sendall(msg.encode('utf-8'))
        while sense.recv(10) != freq:
            sense.sendall(msg.encode('utf-8'))
    # query database for set of frequency based on time occupancy usage
    print "Can I do the next thing..."
    # check if set of channel is not empty
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