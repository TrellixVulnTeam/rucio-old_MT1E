"""
This script will take a json file containing pfns and register in Rucio
If md5 checksum and filesize attributes are not included it will attempt to calculate if storage is reachable
The scope and pfn are required
2 sys args are required to run
      1. The path to the json file containing the pfns
      2. The name of the RSE to register the replicas on
An example of the command:
  python upload.py /home/msnyder/files.txt RUCIOTEST
An example of the json format:
{
  [
    {
      "scope": "nsls2",
      "name": "EIGER_16M_Nov2015.tar.bz2",
      "bytes": 29,
      "adler32": "9de60a53",
      "pfn": "globus:///~/u02/DectrisExampleData/EIGER_16M_Nov2015.tar.bz2",
      "dataset": "insulin_ff"
    }
  ]
}
"""

import json
import logging
import os
from rucio.client.replicaclient import ReplicaClient
from rucio.client.rseclient import RSEClient
from rucio.client.didclient import DIDClient
from rucio.common.config import config_get
from rucio.common.utils import adler32 as rucio_adler32, md5 as rucio_md5
import sys
import urllib2
from urlparse import urlparse
import zlib

logging.basicConfig(stream=sys.stdout,
                    level=getattr(logging,
                                  config_get('common', 'loglevel',
                                             raise_exception=False,
                                             default='DEBUG').upper()),
                    format='%(asctime)s\t%(process)d\t%(levelname)s\t%(message)s')

try:
    from ConfigParser import NoOptionError  # py2
except Exception:
    from configparser import NoOptionError  # py3

try:
    schemes = config_get('conveyor', 'scheme')
except NoOptionError:
    scheme = None


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
    # logging.debug('csum: %s' % csum)
    # logging.debug('clength: %s' % clength)
    # if csum is not None:
    #     csum = csum & 0xffffffff
    # backflip on 32bit
    if csum < 0:
        csum = csum + 2 ** 32

    return str('%08x' % csum), clength

def adler32(url):
    return __zlib_csum(url, zlib.adler32)

def __get_globus_file(file):
    rseclient = RSEClient()
    rse = rseclient.list_rse_attributes('RUCIOTEST')
    globus_endpoint_id = rse['globus_endpoint_id']
    #TODO:

def filehandler(file):
    pfn = file['pfn']
    scope = file['scope']
    name = pfn.split('/')[-1]
    scheme = file['scheme']

    if 'md5' and 'bytes' in file.keys():
        md5 = file['md5']
        bytes = file['bytes']
    elif scheme == 'file':
        path = file['path']
        bytes = os.path.getsize(path)
        md5 = rucio_md5(path) # using the md5 function from Rucio
        # adler32 = rucio_adler32(path) # using the adler32 function from Rucio
        #result = adler32(file['pfn']) # using function above; appears to optimize computational time
        # adler32 = result[0] # adler32 result
        # bytes = result[1] # file size in bytes
        logging.debug('adler32: %s' % adler32)
    else:
        logging.debug('Unsupported schema or not enough attributes supplied.')

    replica = {'scope': scope, 'name': name, 'bytes': bytes, 'md5': md5, 'pfn': pfn}
    return replica

if __name__ == '__main__':
    filelist = sys.argv[1]
    rse = sys.argv[2]
    logging.debug('filelist: %s' % filelist)
    with open(filelist, 'r') as f:
        files = json.load(f)
        logging.debug('files: %s' % files)
        replicaclient = ReplicaClient()
        didclient = DIDClient()
        registerlog = []
        for file in files:
            replicas = []
            o = urlparse(file['pfn'])
            scheme = o.scheme
            file['path'] = o.path
            file['scheme'] = scheme
            if scheme in schemes:
                replica = filehandler(file)
                replicas.append(replica)
                logging.debug('replicas: %s' % replicas)
                # returns True on successful registration
                try:
                    r = replicaclient.add_replicas(rse = rse, files = replicas)
                    logging.debug('Successful replica creation')
                    if 'dataset' in file.keys():
                        dids = []
                        did = {'scope': file['scope'], 'name': file['name']}
                        dids.append(did)
                        try:
                            d = didclient.attach_dids(scope = file['scope'], name = file['dataset'], dids = dids)
                            logging.debug('Successfully attached did')
                        except:
                            errorstring = 'Exception attaching did'
                            logging.error(errorstring)
                            file['error'] = errorstring
                            registerlog.append(file)
                except:
                    errorstring = 'Exception creating replica'
                    logging.error(errorstring)
                    file['error'] = errorstring
                    registerlog.append(file)
            else:
                logging.error('Scheme not supported in rucio.cfg.  Adding file to exception log registerlog.json')
                registerlog.append(file)

    if len(registerlog) > 0:
        # write to log file
        with open('registerlog.json', 'w') as f:
            json.dump(registerlog, f)
    logging.debug('Job complete.')
