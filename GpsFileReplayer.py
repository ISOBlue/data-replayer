import io
import glob
import time
import json
import random
import sys

import avro
import avro.schema
import avro.io

import requests

from time import sleep
from StringIO import StringIO

from kafka import KafkaProducer

class GpsFileReplayer:
    def __init__(self, server, hbSchemaPath, gpsSchemaPath, topic, \
            oadaServerAddr, contentType, authorization, \
            gpsFilesDir='./gps-logs', loop=True):
        self.server = server
        self.gpsSchemaPath = gpsSchemaPath
        self.hbSchemaPath = hbSchemaPath
        self.topic = topic
        self.gpsFilesDir = gpsFilesDir
        self.gpsSchema = self._readSchema(gpsSchemaPath)
        self.hbSchema = self._readSchema(hbSchemaPath)
        self.producer = self._kafkaProducerPreps(server)
        self.loop = loop
        self.oadaServerAddr = oadaServerAddr
        self.contentType = contentType
        self.authorization = authorization

    def _readSchema(self, schemaPath):
        try:
            f = open(schemaPath)
            schema = avro.schema.parse(f.read())
            f.close()
            return schema
        except IOError:
            print('cannot open schema file.')
            raise

    def _kafkaProducerPreps(self, server):
        producer = KafkaProducer(bootstrap_servers=self.server)
        return producer

    def _deleteReplayData(self, ibId):
        print 'Deleting data for ' + ibId + ' in the OADA server ...'
        return
        delNode = self.oadaServerAddr + '/bookmarks/isoblue/device-index/' \
            + ibId

        try:
            r = requests.delete(\
                delNode, \
                headers={
                    'Authorization': self.authorization,
                    'Content-Type': self.contentType
                }, \
                verify=False)
        except requests.exceptions.Timeout:
            print 'Delete request timeout!'
            #raise

    def replay(self):
        # get all the files in the given directory
        files = glob.glob(self.gpsFilesDir + '/*.log')

        # create isoblue id and file name mapping
        ibIds = ['ibt0{}'.format(i) for i in range(1, len(files)+1)]
        ibIdDict = dict(zip(files, ibIds))

        print ibIdDict

        # read all the files simultaneously
        fileHandles = {filename: open(filename, 'r') for filename in files}

        while fileHandles:
            sleep(1)
            print ' '

            for filename, f in fileHandles.items():
                line = next(f, None)
                if line is not None: # the file is not at the end yet
                    line = line.rstrip('\n').split(' ')
#                    print filename, line
#                    print filename, line[1], line[2]

                    ts = int(time.time())

                    # fill in the gps datum
                    datum = {
                        'object_name': 'TPV',
                        'object': {
                            'lat': float(line[1]),
                            'lon': float(line[2]),
                            'time': ts,
                            'status': None,
                            'alt': None,
                            'epx': None,
                            'epy': None,
                            'epv': None,
                            'track': None,
                            'speed': None,
                            'climb': None,
                            'epd': None,
                            'eps': None,
                            'epc': None
                        }
                    }

                    print filename, json.loads(json.dumps(datum))

                    gpsDatum = avro.io.DatumWriter(self.gpsSchema)
                    bytesWriter = io.BytesIO()
                    encoder = avro.io.BinaryEncoder(bytesWriter)

                    gpsDatum.write(json.loads(json.dumps(datum)), encoder)
                    gpsBuf = bytesWriter.getvalue()
                    self.producer.send(\
                            self.topic, key='gps:' + ibIdDict[filename], \
                            value=gpsBuf)

                    datum = {}
                    datum = {
                        'timestamp': ts,
                        'wifins': random.randint(-80, -70),
                        'cellns': random.randint(-90, -80),
                        'netled': True,
                        'statled': True
                    }

                    hbDatum = avro.io.DatumWriter(self.hbSchema)
                    bytesWriter = io.BytesIO()
                    encoder = avro.io.BinaryEncoder(bytesWriter)

                    hbDatum.write(json.loads(json.dumps(datum)), encoder)
                    hbBuf = bytesWriter.getvalue()
                    self.producer.send(\
                            'debug', key='hb:' + ibIdDict[filename], \
                            value=hbBuf)

                    print filename, json.loads(json.dumps(datum))

                else: # the file is at the end
                    if self.loop is True:
                        f.seek(0) # seek to the beginning and replay again
                        print filename \
                            + ' replay ends. Loop is true, so replay again!'
                        self._deleteReplayData(ibIdDict[filename])
                    else:
                        print filename \
                            + ' replay ends. Loop is false. No replay ' \
                            + 'for ' + filename
                        f.close()
                        fileHandles.pop(filename)
                        self._deleteReplayData(ibIdDict[filename])

        # while ends
        print 'All the logs are exhausted ... loop is false ... exit!'
        sys.exit()
