#!/usr/bin/python
#
# Copyright 2005,2007,2011 Free Software Foundation, Inc.
#
# This file is part of GNU Radio
#
# GNU Radio is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3, or (at your option)
# any later version.
#
# GNU Radio is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with GNU Radio; see the file COPYING.  If not, write to
# the Free Software Foundation, Inc., 51 Franklin Street,
# Boston, MA 02110-1301, USA.
#

from gnuradio import gr, eng_notation
from gnuradio import blocks
from gnuradio import audio
from gnuradio import filter
from gnuradio import fft
from gnuradio import uhd
from gnuradio.eng_option import eng_option
from optparse import OptionParser
import sys
import math
import struct
import threading
from datetime import datetime
import time
import socket
from pymongo import MongoClient

HEADERSIZE = 10

sys.stderr.write(
    "Warning: this may have issues on some machines+Python version combinations to seg fault due to the callback in bin_statitics.\n\n")


class ThreadClass(threading.Thread):
    def run(self):
        return


class tune(gr.feval_dd):
    """
    This class allows C++ code to callback into python.
    """

    def __init__(self, tb):
        gr.feval_dd.__init__(self)
        self.tb = tb

    def eval(self, ignore):
        """
        This method is called from blocks.bin_statistics_f when it wants
        to change the center frequency.  This method tunes the front
        end to the new center frequency, and returns the new frequency
        as its result.
        """

        try:
            # We use this try block so that if something goes wrong
            # from here down, at least we'll have a prayer of knowing
            # what went wrong.  Without this, you get a very
            # mysterious:
            #
            #   terminate called after throwing an instance of
            #   'Swig::DirectorMethodException' Aborted
            #
            # message on stderr.  Not exactly helpful ;)

            # continiously retune in the same frequency untill a different
            # frequency is passed
            new_freq = self.tb.center_freq

            # wait until msgq is empty before continuing
            while (self.tb.msgq.full_p()):
                # print "msgq full, holding.."
                time.sleep(0.1)

            return new_freq

        except Exception, e:
            print "tune: Exception: ", e


class parse_msg(object):
    def __init__(self, msg):
        self.center_freq = msg.arg1()
        self.vlen = int(msg.arg2())
        assert (msg.length() == self.vlen * gr.sizeof_float)

        # FIXME consider using NumPy array
        t = msg.to_string()
        self.raw_data = t
        self.data = struct.unpack('%df' % (self.vlen,), t)


class my_top_block(gr.top_block):

    def __init__(self):
        gr.top_block.__init__(self)

        usage = "usage: %prog [options] min_freq max_freq"
        parser = OptionParser(option_class=eng_option, usage=usage)
        parser.add_option("-a", "--args", type="string", default="",
                          help="UHD device device address args [default=%default]")
        parser.add_option("", "--spec", type="string", default=None,
                          help="Subdevice of UHD device where appropriate")
        parser.add_option("-A", "--antenna", type="string", default=None,
                          help="select Rx Antenna where appropriate")
        parser.add_option("-s", "--samp-rate", type="eng_float", default=1e6,
                          help="set sample rate [default=%default]")
        parser.add_option("-g", "--gain", type="eng_float", default=None,
                          help="set gain in dB (default is midpoint)")
        parser.add_option("", "--tune-delay", type="eng_float",
                          default=0.25, metavar="SECS",
                          help="time to delay (in seconds) after changing frequency [default=%default]")
        parser.add_option("", "--dwell-delay", type="eng_float",
                          default=0.25, metavar="SECS",
                          help="time to dwell (in seconds) at a given frequency [default=%default]")
        parser.add_option("-b", "--channel-bandwidth", type="eng_float",
                          default=6.25e3, metavar="Hz",
                          help="channel bandwidth of fft bins in Hz [default=%default]")
        parser.add_option("-l", "--lo-offset", type="eng_float",
                          default=0, metavar="Hz",
                          help="lo_offset in Hz [default=%default]")
        parser.add_option("-q", "--squelch-threshold", type="eng_float",
                          default=None, metavar="dB",
                          help="squelch threshold in dB [default=%default]")
        parser.add_option("-F", "--fft-size", type="int", default=None,
                          help="specify number of FFT bins [default=samp_rate/channel_bw]")
        parser.add_option("", "--real-time", action="store_true", default=False,
                          help="Attempt to enable real-time scheduling")

        (options, args) = parser.parse_args()
        if len(args) != 1:
            parser.print_help()
            sys.exit(1)

        self.channel_bandwidth = options.channel_bandwidth

        self.center_freq = eng_notation.str_to_num(args[0])
        # self.max_freq = eng_notation.str_to_num(args[1])

        # if self.min_freq > self.max_freq:
        # swap them
        # self.min_freq, self.max_freq = self.max_freq, self.min_freq

        if not options.real_time:
            realtime = False
        else:
            # Attempt to enable realtime scheduling
            r = gr.enable_realtime_scheduling()
            if r == gr.RT_OK:
                realtime = True
            else:
                realtime = False
                print "Note: failed to enable realtime scheduling"

        # build graph
        self.u = uhd.usrp_source(device_addr=options.args,
                                 stream_args=uhd.stream_args('fc32'))

        # Set the subdevice spec
        if (options.spec):
            self.u.set_subdev_spec(options.spec, 0)

        # Set the antenna
        if (options.antenna):
            self.u.set_antenna(options.antenna, 0)

        self.u.set_samp_rate(options.samp_rate)
        self.usrp_rate = usrp_rate = self.u.get_samp_rate()

        self.lo_offset = options.lo_offset

        if options.fft_size is None:
            self.fft_size = int(self.usrp_rate / self.channel_bandwidth)
        else:
            self.fft_size = options.fft_size

        self.squelch_threshold = options.squelch_threshold

        s2v = blocks.stream_to_vector(gr.sizeof_gr_complex, self.fft_size)

        mywindow = filter.window.blackmanharris(self.fft_size)
        ffter = fft.fft_vcc(self.fft_size, True, mywindow, True)
        power = 0
        for tap in mywindow:
            power += tap * tap
        self.norm_fac = power / self.fft_size

        c2mag = blocks.complex_to_mag_squared(self.fft_size)

        # FIXME the log10 primitive is dog slow
        # log = blocks.nlog10_ff(10, self.fft_size,
        #                       -20*math.log10(self.fft_size)-10*math.log10(power/self.fft_size))

        # Set the freq_step to 75% of the actual data throughput.
        # This allows us to discard the bins on both ends of the spectrum.

        # self.freq_step = self.nearest_freq((0.75 * self.usrp_rate), self.channel_bandwidth)
        # self.min_center_freq = self.min_freq + (self.freq_step/2)
        # nsteps = math.ceil((self.max_freq - self.min_freq) / self.freq_step)
        # self.max_center_freq = self.min_center_freq + (nsteps * self.freq_step)

        # self.next_freq = self.center_freq

        tune_delay = max(0, int(round(options.tune_delay * usrp_rate / self.fft_size)))  # in fft_frames
        dwell_delay = max(1, int(round(options.dwell_delay * usrp_rate / self.fft_size)))  # in fft_frames

        self.msgq = gr.msg_queue(1)
        self._tune_callback = tune(self)  # hang on to this to keep it from being GC'd
        stats = blocks.bin_statistics_f(self.fft_size, self.msgq,
                                        self._tune_callback, tune_delay,
                                        dwell_delay)

        # FIXME leave out the log10 until we speed it up
        # self.connect(self.u, s2v, ffter, c2mag, log, stats)
        self.connect(self.u, s2v, ffter, c2mag, stats)

        if options.gain is None:
            # if no gain was specified, use the mid-point in dB
            g = self.u.get_gain_range()
            options.gain = float(g.start() + g.stop()) / 2.0

        self.set_gain(options.gain)
        print "gain =", options.gain

    # def set_next_freq(self):
    # target_freq = self.next_freq
    # self.next_freq = self.next_freq + self.freq_step
    # if self.next_freq >= self.max_center_freq:
    # self.next_freq = self.min_center_freq

    # if not self.set_freq(target_freq):
    # print "Failed to set frequency to", target_freq
    # sys.exit(1)

    # return target_freq

    def set_freq(self, target_freq):
        """
        Set the center frequency we're interested in.

        Args:
            target_freq: frequency in Hz
        @rypte: bool
        """

        r = self.u.set_center_freq(uhd.tune_request(target_freq, rf_freq=(target_freq + self.lo_offset),
                                                    rf_freq_policy=uhd.tune_request.POLICY_MANUAL))
        if r:
            return True

        return False

    def set_gain(self, gain):
        self.u.set_gain(gain)

    def nearest_freq(self, freq, channel_bandwidth):
        freq = round(freq / channel_bandwidth, 0) * channel_bandwidth
        return freq


def main_loop(tb):
    # database initialization
    client = MongoClient('mongodb://127.0.0.1:27017/')
    db = client.projectDSA

    print "Default frequency is ", tb.center_freq

    def bin_freq(i_bin, center_freq):
        # hz_per_bin = tb.usrp_rate / tb.fft_size
        freq = center_freq - (tb.usrp_rate / 2) + (tb.channel_bandwidth * i_bin)
        # print "freq original:",freq
        # freq = nearest_freq(freq, tb.channel_bandwidth)
        # print "freq rounded:",freq
        return freq

    bin_start = int(tb.fft_size * ((1 - 0.75) / 2))
    bin_stop = int(tb.fft_size - bin_start)

    # create a INET, STREAMing socket
    mysckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    mysckt.connect(('localhost', 12345))
    parse_msg(tb.msgq.delete_head())
    while 1:
        try:
            # check for request
            # receive header
            msglen = mysckt.recv(HEADERSIZE)
            # print msglen
            msg = mysckt.recv(int(msglen.decode('utf-8'))) # throw value error when empty
            mysckt.send(msg)
            # ensure you parse only when request is made
            new_freq = float(msg.decode('utf-8')) # throw value error when empty
            print "Tuning to new frequency", new_freq
            tb.center_freq = new_freq
            # Get the next message sent from the C++ code (blocking call).
            while parse_msg(tb.msgq.delete_head()).center_freq != new_freq:
                m = parse_msg(tb.msgq.delete_head())

            # modified data
            mod_data = list()
            for i in range(tb.fft_size):
                mod_data.append(m.data[i] / (tb.norm_fac * tb.fft_size))
            mod_data = tuple(mod_data)

            # Scanning rate
            # if timestamp == 0:
            # timestamp = time.time()
            # centerfreq = m.center_freq
            # if m.center_freq < centerfreq:
            # sys.stderr.write("scanned %.1fMHz in %.1fs\n" % ((centerfreq - m.center_freq)/1.0e6, time.time() - timestamp))
            # timestamp = time.time()
            # centerfreq = m.center_freq

            # noise_floor_db = -174 + 10*math.log10(tb.channel_bandwidth)
            # noise_floor_db = 10*math.log10(min(m.data)/tb.usrp_rate)
            noise_floor_db = 10 * math.log10(min(mod_data) / tb.usrp_rate)
            fmin = bin_freq(bin_start, m.center_freq)
            fmax = bin_freq(bin_stop, m.center_freq)
            # save channels used
            if db.get_collection("channels") is None:
                db.create_collection("channels")
            if db.get_collection('channels').find_one({'channel.fmin': fmin, 'channel.fmax': fmax, 'channel.bw': tb.usrp_rate}) is None:
                channel_id = db.get_collection('channels').insert_one({'channel': {'fmin': fmin, 'fmax': fmax, 'bw': tb.usrp_rate, 'counts': 1, 'duty_cycle': 1.0}}).inserted_id
            else:
                query = {'channel.fmin': fmin, 'channel.fmax': fmax, 'channel.bw': tb.usrp_rate}
                channel = db.get_collection('channels').find_one(query)
                channel_id = channel['_id']
                channel['channel']['counts'] += 1
                db.get_collection('channels').update_one(
                    query, {'$set': {'channel.counts': channel['channel']['counts']}})
            for i_bin in range(bin_start, bin_stop):
                center_freq = m.center_freq
                freq = bin_freq(i_bin, center_freq)
                # power_db = 10*math.log10(mod_data[i_bin]/tb.usrp_rate) - noise_floor_db
                amp_db = 10 * math.log10(mod_data[i_bin] / tb.usrp_rate)
                power_db = amp_db - noise_floor_db

            # FIXME run on a separate thread as database will grow greatly
            if power_db > tb.squelch_threshold:
                print datetime.now(), "center_freq", center_freq, "freq", freq, "power_db", power_db, "noise_floor_db", noise_floor_db
                # save sensor data to database
                if db.get_collection('sensor') is None:
                    db.create_collection('sensor')
                # choose highest signal amplitude
                db.get_collection('sensor').insert_one(
                    {'noise_floor': noise_floor_db, 'signal': {'amplitude': amp_db, 'channel': channel_id},
                     'date': datetime.now(), })

        except ValueError:
            # free message queue till next tune cognitive engine request
            parse_msg(tb.msgq.delete_head())
            pass

if __name__ == '__main__':
    t = ThreadClass()
    t.start()

    tb = my_top_block()
    try:
        tb.start()
        main_loop(tb)

    except KeyboardInterrupt:
        pass
