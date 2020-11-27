import numpy as np
import sys
import functools

def formatmsg(msg):
    packet = '{length:<10}'.format(length=len(msg)) + msg
    return packet


def txtformat(lnght):
    fmt = ''
    if lnght > 1:
        fmt = 's'
    return fmt

def get_freq():
    freqs = []
    # start_freq = 900e6
    # stop_freq = 910e6
    # nchan = 9
    # # TESTING
    # start_freq = 2390e6
    # stop_freq = 2408e6
    # nchan = 9
    # PROJECT CHANNELS
    start_freq = 2389.5e6
    stop_freq = 2407.5e6
    nchan = 6
    step = (stop_freq - start_freq) / nchan
    for i in range(nchan):
        if i == 0:
            freqs.append(str(start_freq + step / 2))
        else:
            freqs.append(str(float(freqs[i - 1]) + step))
    print ("Default frequencies are ", freqs)
    return freqs


def get_nonzeros(prev, x):
    if type(prev) != np.ndarray:
        prev = np.array([prev])
    if float(x) != 0.0:
        prev = np.append(prev, x)
    return prev


class TrafficClassification:
    """Classify traffic pattern for predictive selection"""

    def __init__(self, sequence_len):
        """initialize class to classify traffic of binary sequence
        of length while correlating the sequence edge [0, 1]"""
        self.rxx = np.array([])
        self.corr_stats = np.array([])
        self.tau_ave = 0
        self.std = 0
        self.N = sequence_len

    def classify(self, bin_seq):
        """method to classify traffic based to time binary sequence"""
        if len(bin_seq) != self.N:
            raise ValueError("length of samples do not match the initialized length")
        self.rxx = np.correlate(bin_seq, [0, 1], mode='valid')
        sep = np.array([])
        for i in range(len(self.rxx)):
            if i > 0:
                if self.rxx[i - 1] == 0 and self.rxx[i] == 1:
                    sep = np.append(sep, 1)
                else:
                    if len(sep) > 0:
                        sep[-1] += 1
        # print(sep)
        if self.rxx[i - 1] == 0 and self.rxx[i] == 1:
            sep = np.append(sep, 0)
        else:
            if len(sep) > 1:
                sep = sep[:-1]
            else:
                sep = np.array([self.N])
                sys.stderr.write('WARNING: Choose a larger sample size for better prediction\n')
        # print(sep)
        sep = functools.reduce(get_nonzeros, sep)
        self.corr_stats = np.append(self.corr_stats, sep)

        self.tau_ave = np.average(self.corr_stats)
        self.std = np.std(self.corr_stats)
        if self.tau_ave == 0.0:
            print ("Choose a larger sample size")
            sys.exit(1)
        if self.std == 0.0 or self.std < 0.3 * self.tau_ave:
            # print (self.tau_ave, self.std, self.corr_stats)
            # print "Traffic is periodic with period ", round(self.tau_ave)
            return ('PERIODIC', round(self.tau_ave))
        else:
            # print "Traffic is stochastic"
            # print (self.tau_ave, self.std)
            return 'STOCHASTIC', None

    def get_period(self):
        return round(self.tau_ave)