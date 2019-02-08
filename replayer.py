#!/usr/bin/env python

from GpsFileReplayer import GpsFileReplayer

if __name__ == '__main__':

    gfr = GpsFileReplayer(server='cloudradio39.ecn.purdue.edu:9092', \
                          gpsSchemaPath='./schema/gps.avsc', \
                          hbSchemaPath='./schema/d_hb.avsc', \
                          topic='remote')

    gfr.replay('./gps-logs')
