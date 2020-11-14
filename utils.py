import numpy as np

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
    start_freq = 2390e6
    stop_freq = 2408e6
    nchan = 9
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

class TrafficClassification:
    """Classify traffic pattern for predictive selection"""

    def __init__(self, sequence_len, lag_samples):
        """initialize class to classify traffic of binary sequence
        of length while correlating the sequence at lag of lower length"""
        self.rxx = np.array([])
        self.tau_max = 0
        self.tau_local = 0
        self.tau_ave_arr = np.zeros((1, sequence_len))
        self.tau_ave = 0
        self.std = 0
        # self.seq_pointer = 0
        if lag_samples < sequence_len:
            self.N = sequence_len
            self.m = lag_samples
        else:
            raise AttributeError("lag length has to be less than sequence length")

    def classify(self, bin_seq):
        """method to classify traffic based to time binary sequence"""
        if len(bin_seq) != self.N:
            raise ValueError("length of samples do not match the initialized length")
        self.rxx = np.correlate(bin_seq, bin_seq[self.m:-1], mode='valid')
        if max(self.rxx) >= self.tau_max:
            self.tau_max = max(self.rxx)
            self.tau_ave_arr = np.append(self.tau_ave_arr, self.tau_max)
            # self.seq_pointer = self.N - list(self.rxx).index(max(self.rxx))
            # print "global maximum set to ", self.tau_max
        else:
            self.tau_ave_arr = np.append(self.tau_ave_arr,
                                         (max(self.rxx) - self.tau_local))
            self.tau_local = max(self.rxx)
            # self.tau_ave_arr = np.append(self.tau_ave_arr,
            #                              self.seq_pointer + list(self.rxx).index(self.tau_local))
        # self.seq_pointer += self.N
        # print self.seq_pointer
        # print "length of array is ", len(self.tau_ave_arr)
        self.tau_ave = np.average(self.tau_ave_arr)
        self.std = np.std(self.tau_ave_arr)
        # print self.tau_max, self.tau_ave, self.std
        if self.tau_ave == self.tau_max:
            print "Traffic is periodic with period ", self.tau_max
            return 'PERIODIC', self.tau_max
        elif self.std < 3 * self.tau_ave:
            # print self.tau_ave, self.std, self.tau_max
            print "Traffic is periodic with period ", round(self.tau_ave)
            return 'PERIODIC', round(self.tau_ave)
        else:
            # print "Traffic is stochastic"
            # print self.tau_ave, self.std, self.tau_max
            return 'STOCHASTIC', None