def formatmsg(msg):
    packet = '{length:<10}'.format(length=len(msg)) + msg
    return packet


def txtformat(lnght):
    fmt = ''
    if lnght > 1:
        fmt = 's'
    return fmt