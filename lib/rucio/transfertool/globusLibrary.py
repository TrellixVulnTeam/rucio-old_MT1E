import logging
import yaml
from globus_sdk import NativeAppAuthClient, RefreshTokenAuthorizer, AccessTokenAuthorizer, TransferClient, TransferData
from datetime import datetime

# TODO: use new RucioGlobusTest application to centralize the globus token calls
# RucioGlobusTest is NOT a NativeApp
def getTransferClient():
    # cfg = yaml.safe_load(open("./config.yml"))
    cfg = yaml.safe_load(open("/opt/rucio/lib/rucio/transfertool/config.yml"))
    client_id = cfg['globus']['apps']['SDK Tutorial App']['client_id']
    auth_client = NativeAppAuthClient(client_id)
    refresh_token = cfg['globus']['apps']['SDK Tutorial App']['refresh_token']

    # logging.info('authorizing token...')
    authorizer = RefreshTokenAuthorizer(refresh_token = refresh_token, auth_client = auth_client)
    access_token = authorizer.access_token

    # logging.info('initializing TransferClient...')
    tc = TransferClient(authorizer=authorizer)
    return tc

def getTransferData():
    # cfg = yaml.safe_load(open("./config.yml"))
    cfg = yaml.safe_load(open("/opt/rucio/lib/rucio/transfertool/config.yml"))
    client_id = cfg['globus']['apps']['SDK Tutorial App']['client_id']
    auth_client = NativeAppAuthClient(client_id)
    refresh_token = cfg['globus']['apps']['SDK Tutorial App']['refresh_token']
    source_endpoint_id = cfg['globus']['apps']['SDK Tutorial App']['win10_endpoint_id']
    destination_endpoint_id = cfg['globus']['apps']['SDK Tutorial App']['sdccfed_endpoint_id']
    # logging.info('authorizing token...')
    authorizer = RefreshTokenAuthorizer(refresh_token = refresh_token, auth_client = auth_client)
    access_token = authorizer.access_token

    # logging.info('initializing TransferClient...')
    tc = TransferClient(authorizer=authorizer)
    # as both endpoints are expected to be Globus Server endpoints, send auto-activate commands for both globus endpoints
    a = auto_activate_endpoint(tc, source_endpoint_id)
    b = auto_activate_endpoint(tc, destination_endpoint_id)

    job_label = str(datetime.now())
    # from Globus... sync_level=checksum means that before files are transferred, Globus will compute checksums on the source and destination files, and only transfer files that have different checksums are transferred. verify_checksum=True means that after a file is transferred, Globus will compute checksums on the source and destination files to verify that the file was transferred correctly.  If the checksums do not match, it will redo the transfer of that file.
    tdata = TransferData(tc, source_endpoint_id, destination_endpoint_id, label = job_label, sync_level="checksum", verify_checksum=True)

    return tdata

def auto_activate_endpoint(tc, ep_id):
    r = tc.endpoint_autoactivate(ep_id, if_expires_in=3600)
    if r['code'] == 'AutoActivationFailed':
        logging.info('Endpoint({}) Not Active! Error! Source message: {}'.format(ep_id, r['message']))
        # sys.exit(1) # TODO: don't want to exit; hook into graceful exit
    elif r['code'] == 'AutoActivated.CachedCredential':
            logging.info('Endpoint({}) autoactivated using a cached credential.'.format(ep_id))
    elif r['code'] == 'AutoActivated.GlobusOnlineCredential':
            logging.info(('Endpoint({}) autoactivated using a built-in Globus credential.').format(ep_id))
    elif r['code'] == 'AlreadyActivated':
            logging.info('Endpoint({}) already active until at least {}'.format(ep_id, 3600))
    return r['code']


def submit_xfer(source_endpoint_id, destination_endpoint_id, source_path, dest_path, job_label, recursive=False):

    tc = getTransferClient()
    # as both endpoints are expected to be Globus Server endpoints, send auto-activate commands for both globus endpoints
    a = auto_activate_endpoint(tc, source_endpoint_id)
    b = auto_activate_endpoint(tc, destination_endpoint_id)


    # from Globus... sync_level=checksum means that before files are transferred, Globus will compute checksums on the source and destination files, and only transfer files that have different checksums are transferred. verify_checksum=True means that after a file is transferred, Globus will compute checksums on the source and destination files to verify that the file was transferred correctly.  If the checksums do not match, it will redo the transfer of that file.
    tdata = TransferData(tc, source_endpoint_id, destination_endpoint_id, label = job_label, sync_level="checksum", verify_checksum=True)
    tdata.add_item(source_path, dest_path, recursive = recursive)

    # logging.info('submitting transfer...')
    transfer_result = tc.submit_transfer(tdata)
    # logging.info("task_id =", transfer_result["task_id"])

    return transfer_result["task_id"]

def bulk_submit_xfer(submitjob, recursive=False):

    # cfg = yaml.safe_load(open("./config.yml"))
    cfg = yaml.safe_load(open("/opt/rucio/lib/rucio/transfertool/config.yml"))
    client_id = cfg['globus']['apps']['SDK Tutorial App']['client_id']
    auth_client = NativeAppAuthClient(client_id)
    refresh_token = cfg['globus']['apps']['SDK Tutorial App']['refresh_token']
    # source_endpoint_id = cfg['globus']['apps']['SDK Tutorial App']['win10_endpoint_id']
    # destination_endpoint_id = cfg['globus']['apps']['SDK Tutorial App']['sdccfed_endpoint_id']
    source_endpoint_id = submitjob[0].get('metadata').get('source_globus_endpoint_id')
    destination_endpoint_id = submitjob[0].get('metadata').get('dest_globus_endpoint_id')
    # logging.info('authorizing token...')
    authorizer = RefreshTokenAuthorizer(refresh_token = refresh_token, auth_client = auth_client)
    access_token = authorizer.access_token

    # logging.info('initializing TransferClient...')
    tc = TransferClient(authorizer=authorizer)
    # as both endpoints are expected to be Globus Server endpoints, send auto-activate commands for both globus endpoints
    a = auto_activate_endpoint(tc, source_endpoint_id)
    b = auto_activate_endpoint(tc, destination_endpoint_id)

    # make job_label for task a timestamp
    x = datetime.now()
    job_label = x.strftime('%Y%m%d%H%M%s')

    # from Globus... sync_level=checksum means that before files are transferred, Globus will compute checksums on the source and destination files, and only transfer files that have different checksums are transferred. verify_checksum=True means that after a file is transferred, Globus will compute checksums on the source and destination files to verify that the file was transferred correctly.  If the checksums do not match, it will redo the transfer of that file.
    tdata = TransferData(tc, source_endpoint_id, destination_endpoint_id, label = job_label, sync_level="checksum", verify_checksum=True)

    for file in submitjob:
        source_path = file.get('sources')[0]
        dest_path = file.get('destinations')[0]
        # TODO: support passing a recursive parameter to Globus
        tdata.add_item(source_path, dest_path, recursive = False)

    # logging.info('submitting transfer...')
    transfer_result = tc.submit_transfer(tdata)
    # logging.info("task_id =", transfer_result["task_id"])

    return transfer_result["task_id"]

def check_xfer(task_id):
    tc = getTransferClient()
    transfer = tc.get_task(task_id)
    status = str(transfer["status"])
    return status

def bulk_check_xfers(task_ids):
    tc = getTransferClient()

    logging.debug('task_ids: %s' % task_ids)

    responses = {}

    for task_id in task_ids:
        transfer = tc.get_task(str(task_id))
        logging.debug('transfer: %s' % transfer)
        status = str(transfer["status"])
        # task_ids[str(task_id)]['file_state'] = status
        responses[str(task_id)] = status

    logging.debug('responses: %s' % responses)

    return responses
