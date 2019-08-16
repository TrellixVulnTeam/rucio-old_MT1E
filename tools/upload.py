# This script will take a json file containing pfns and register in Rucio
# It will calculate the adler32 checksum and filesize
# 2 sys args are required to run
#       1. The path to the json file containing the pfns
#       2. The name of the RSE to register the replicas on
# An example of the command:
#   python upload.py /home/msnyder/files.txt RUCIOTEST
# An example of the json format:
# {
#   "files": [
#     {
#       "scope": "nsls2",
#       "pfn": "file:///home/msnyder/data/test18.txt"
#     },
#     {
#       "scope": "nsls2",
#       "pfn": "file:///home/msnyder/data/test19.txt"
#     }
#   ]
# }
#
# The scope and pfn are required

import json
import logging
from rucio.client.replicaclient import ReplicaClient
from rucio.client.rseclient import RSEClient
from rucio.common.utils import adler32 as rucio_adler32
import sys
import urllib2
from urlparse import urlparse
import zlib

logging.basicConfig(stream=sys.stdout,
                    level='DEBUG',
                    format='%(asctime)s\t%(process)d\t%(levelname)s\t%(message)s')

def __zlib_csum(url, func):
    # adapted from http://j2py.blogspot.com/2014/09/python-generates-crc32-and-adler32.html
    # TODO: test this approach for "large" files; supposed to be memory optimized for "large" files
    if isinstance(url, basestring if sys.version_info[0] < 3 else str):
        url = urllib2.Request(url)
    f = urllib2.urlopen(url)
    csum = None
    clength = 0
    try:
        chunk = f.read(1024)
        clength = len(chunk)
        if clength > 0:
            csum = func(chunk)
            while True:
                chunk = f.read(1024)
                clength = clength + len(chunk)
                if len(chunk) > 0:
                    csum = func(chunk, csum)
                else:
                    break
    finally:
        f.close()
    logging.debug('csum: %s' % csum)
    logging.debug('clength: %s' % clength)
    # if csum is not None:
    #     csum = csum & 0xffffffff
    # backflip on 32bit
    if csum < 0:
        csum = csum + 2 ** 32

    return str('%08x' % csum), clength

def crc32(url):
    return __zlib_csum(url, zlib.crc32)

def adler32(url):
    return __zlib_csum(url, zlib.adler32)

def __get_globus_file(file):
    rseclient = RSEClient()
    rse = rseclient.list_rse_attributes('RUCIOTEST')
    globus_endpoint_id = rse['globus_endpoint_id']

def filehandler(file):
    o = urlparse(file['pfn'])
    scheme = o.scheme

    if scheme == 'file':
        scope = file['scope']
        name = file['name']
        pfn = file['pfn']
        #rucio_a32 = rucio_adler32(o.path) # using the adler32 function from Rucio
        result = adler32(file['pfn']) # using function above
        mem_a32 = result[0] # adler32 result
        bytes = result[1] # file size in bytes
        logging.debug('mem_a32: %s' % mem_a32)
        replica = {'scope': scope, 'name': name, 'bytes': bytes, 'adler32': mem_a32, 'pfn': pfn}
        return replica
    elif scheme == 'globus':
        __get_globus_file(file)

    else:
        logging.error('%s is an unsupported scheme.' % scheme)

if __name__ == '__main__':
    filelist = sys.argv[1]
    rse = sys.argv[2]
    logging.debug('filelist: %s' % filelist)
    with open(filelist, 'r') as f:
        files = json.load(f)
        logging.debug('files: %s' % files)
        replicas = []
        for file in files['files']:
            replica = filehandler(file)
            replicas.append(replica)
            logging.debug('replicas: %s' % replicas)
        replicaclient = ReplicaClient()
        # returns True on successful registration
        r = replicaclient.add_replicas(rse = rse, files = replicas)
        if r:
            logging.debug('Successful')
        else:
            logging.debug('Unsuccessful')
