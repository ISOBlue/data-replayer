import io
import glob
import time
import json
import random

import avro
import avro.schema
import avro.io

from kafka import KafkaProducer
from time import sleep
from StringIO import StringIO

class GpsFileReplayer:
    def __init__(self, server, hbSchemaPath, gpsSchemaPath, topic):
        self.server = server
        self.gpsSchemaPath = gpsSchemaPath
        self.hbSchemaPath = hbSchemaPath
        self.topic = topic
        self.gpsSchema = self._readSchema(gpsSchemaPath)
        self.hbSchema = self._readSchema(hbSchemaPath)
        self.producer = self._kafkaProducerPreps(server)

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

    def replay(self, gpsFilesDir):
        # get all the files in the given directory
        files = glob.glob(gpsFilesDir + '/*.log')

        # create isoblue id and file name mapping
        ibIds = ['ibt0{}'.format(i) for i in range(1, len(files)+1)]
        ibIdDict = dict(zip(files, ibIds))

        print ibIdDict

        # read all the files simultaneously
        file_handles = {filename: open(filename, 'r') for filename in files}
        while file_handles:
            sleep(1)
            print ' '

            for filename, f in file_handles.items():
                line = next(f, None)
                if line is not None:
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
                            self.topic, key='hb:' + ibIdDict[filename], \
                            value=hbBuf)

                    print filename, json.loads(json.dumps(datum))

                else:
                    f.close()
                    file_handles.pop(filename)
