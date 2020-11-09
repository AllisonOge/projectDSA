def formatmsg(msg):
    packet = '{length:<10}'.format(length=len(msg)) + msg
    return packet