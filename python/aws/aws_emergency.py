import botocore.exceptions as botoex
from common import *
from file_utils import *
logger = logging.getLogger(__name__)

# Only used if we cannot get the txlog from local or s3
class AwsGlacierEmergencyManager:

  def __init__ (self, boto_session):
    glacier = boto_session.resource('glacier')
    vault_name = get_conf().aws.glacier_vault
    self.vault = glacier.Vault(AwsGlacierManager.DEFAULT_ACCOUNT, vault_name)

  # In case you cannot retrieve the txlog from a local copy or s3 !!
  def download_last_txlog (self, search_key, fileout):
    inventory = self.check_for_recent_inventory_job()
    if not inventory:
      inventory = self.retrieve_vault_inventory()
    archive_id, size = self.find_most_recent_tx_log(inventory, search_key)

    fileseg = Fileseg(fileout, None, archive_id, (0,size))
    if not self.download_from_existing_job(fileseg):
      self.single_shot_download (fileseg)
    return fileseg

  def retrieve_vault_inventory (self):
    retrieval_job = retry_operation (
      lambda : self.vault.initiate_inventory_retrieval(),
      botoex.ClientError
    )  

    logger.info("Submitted inventory retrieval job %r", retrieval_job.id)
    self.wait_for_job_completion(retrieval_job)
    output = retry_operation (
      lambda : retrieval_job.get_output(),
      botoex.ClientError
    )  

    assert int(output['status']) == 200
    inventory = convert_json_bytes_to_dict(output['body'].read())
    logger.info("Got inventory with %d items", len(inventory['ArchiveList']))
    return inventory

  def find_most_recent_tx_log (self, inventory, search_key):
    candidates = [ arc for arc in inventory['ArchiveList'] 
                   if arc['ArchiveDescription'].find(search_key) > -1 ]

    most_recent = next(sorted( candidates, reverse=True, key=lambda x:x['CreationDate'] ))
    logger.info("Most recent txlog found : %r", most_recent)
    return most_recent['ArchiveId'], int(most_recent['Size'])

  def check_for_recent_inventory_job (self):
    # in case we crash before retrieving the txlog we do not start many other jobs
    job = next( (j for j in self.vault.jobs.all() 
                if j.status_code in ('InProgress', 'Succeeded') and j.action == 'InventoryRetrieval'),
                None )
    if not job:
      return None

    logger.info("Found existing inventory retrieval job %r", job.id)
    self.wait_for_job_completion(job)
    output = retry_operation (
      lambda : job.get_output(),
      botoex.ClientError
    )  

    assert int(output['status']) == 200
    inventory = convert_json_bytes_to_dict(output['body'].read())
    logger.info("Got inventory with %d items", len(inventory['ArchiveList']))
    return inventory

  def wait_for_job_completion (self, aws_job):
    # no job timeout handling ...
    if aws_job.status_code == 'Succeeded': return

    while True:
      logger.info('Checking if glacier job %r has completed', aws_job.id)
      fresh_stx = next( j for j in self.vault.jobs.all() if j.id == aws_job.id )
      if fresh_stx.status_code == 'Succeeded': return
      assert fresh_stx.status_code == 'InProgress'
      wait_for_polling_period()

  def download_from_existing_job (self, fileseg):
    job = next( (j for j in self.vault.jobs.all() 
                if j.status_code in ('InProgress', 'Succeeded') and j.archive_id == fileseg.archive_id),
                None )
    if not job:
      return False

    logger.info("Found existing job %r for %r", job.id, fileseg.archive_id)
    self.wait_fetch_write(fileseg, job)
    return True

  def single_shot_download (self, fileseg):
    archive = self.vault.Archive(fileseg.archive_id)
    job = retry_operation (
      lambda : archive.initiate_inventory_retrieval(**kwargs),
      botoex.ClientError
    )
    logger.info("Submitted download job %r", job.id)
    self.wait_fetch_write(fileseg, job)
 
  def wait_fetch_write (self, fileseg, job): 
    self.wait_for_job_completion(job)
    output = retry_operation (
      lambda : job.get_output(),
      botoex.ClientError
    )  

    assert int(output['status']) == 200
    logger.info("Writing into %r : %r", fileseg.fileout, output['ResponseMetadata']['HTTPHeaders'])
    body_bytes = job_output['body'].read()
    truncate_and_write_fileseg(fileseg, body_bytes)
  
## END AwsGlacierEmergencyManager


