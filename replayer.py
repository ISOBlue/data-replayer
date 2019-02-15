#!/usr/bin/env python

from GpsFileReplayer import GpsFileReplayer

if __name__ == '__main__':

    replayer = GpsFileReplayer(server='cloudradio39.ecn.purdue.edu:9092', \
                          gpsSchemaPath='./schema/gps.avsc', \
                          hbSchemaPath='./schema/d_hb.avsc', \
                          topic='remote', \
                          gpsFilesDir='./gps-logs-test', \
                          loop=True, \
                          oadaServerAddr='https://localhost:8080', \
                          contentType='isoblue.stuff', \
                          authorization='Bearer abc')

    replayer.replay()
