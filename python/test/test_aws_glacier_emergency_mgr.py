import unittest as ut, concurrent.futures
from common import *
from routines_for_test import *
from aws_glacier_txlog import *
from aws_session import *
from aws_mock import *
logger = logging.getLogger(__name__)

@deco_setup_each_test
class TestAwsGlacierEmergencyMgr (ut.TestCase):

  def setUp(self):
    self.session = DummySession.create_dummy_session()
    self.glacier_res = self.session.resource('glacier')
    self.emergency_mgr = AwsGlacierEmergencyMgr(self.session)
    self.vault = self.glacier_res.Vault('', get_conf().aws.glacier_vault)
    DummyJob.CompleteAsSoonAsCreated = True

  def tearDown(self):
    if self.vault: self.vault._clear()

  #@ut.skip("For quick validation")
  def test_txlog_download_nominal(self):
    search_key = get_conf().app.transaction_log
    back_logfile = give_stage_filepath()
    archive = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 24)
    fileseg = self.emergency_mgr.download_last_txlog(search_key, back_logfile)

    self.assertTrue( fileseg.done )
    self.assertTrue( fileseg.archive_id == archive.id )
    self.assertTrue( fileseg.archive_id in self.vault._archives )
    self.assertTrue( os.path.isfile(fileseg.fileout) )
    with open(fileseg.fileout, 'rb') as fileobj:
      self.assertEqual( fileobj.seek(0, os.SEEK_END), fileseg.range_bytes[1] )

  #@ut.skip("For quick validation")
  def test_txlog_download_nominal_job_delay(self):
    wait_secs = get_conf().aws.polling_period_secs * 1.5
    DummyJob.CompleteAsSoonAsCreated = False
    search_key = get_conf().app.transaction_log
    back_logfile = give_stage_filepath()
    archive = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 24)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
      future = executor.submit(self.emergency_mgr.download_last_txlog, search_key, back_logfile)
      while not future.done():
        for job in list(self.vault.jobs.all()): 
          if job.status_code == DummyJob.InProgress: job._complete()
        time.sleep(wait_secs)
      fileseg = future.result(timeout=wait_secs)

    self.assertTrue( fileseg.archive_id == archive.id )
    self.assertTrue( fileseg.archive_id in self.vault._archives )
    self.assertTrue( os.path.isfile(fileseg.fileout) )

  #@ut.skip("For quick validation")
  def test_txlog_download_most_recent(self):
    search_key = get_conf().app.transaction_log
    back_logfile = give_stage_filepath()
    archive1 = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 24, 7)
    archive2 = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_2', 16, 45)
    archive3 = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_3', 79, 15)
    fileseg = self.emergency_mgr.download_last_txlog(search_key, back_logfile)

    self.assertEqual( 3, len(self.vault._archives) )
    self.assertTrue( fileseg.archive_id == archive1.id )
    self.assertTrue( fileseg.archive_id in self.vault._archives )
    self.assertTrue( os.path.isfile(fileseg.fileout) )

  #@ut.skip("For quick validation")
  def test_txlog_download_bad_hash(self):
    DummyJob.CompleteAsSoonAsCreated = False
    search_key = get_conf().app.transaction_log
    back_logfile = give_stage_filepath()
    archive = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 24)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
      future = executor.submit(self.emergency_mgr.download_last_txlog, search_key, back_logfile)
      while not future.done():
        for job in list(self.vault.jobs.all()): 
          if job.status_code == DummyJob.InProgress: 
            job.calculate_hash_and_rewind = lambda f : b'coucou'
            job._complete()
      fileseg = future.result(timeout=1)

    # this should fail but for the moment we do nto check the hash, gpg anyway should fail to decrypt
    self.assertTrue( fileseg.archive_id == archive.id )

  #@ut.skip("For quick validation")
  def test_txlog_download_failures(self):
    search_key = get_conf().app.transaction_log
    back_logfile = give_stage_filepath()
    archive = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 24)

    DummySession.behaviour = always_ko_behaviour()
    DummySession.blowup_on_fail = True
    with self.assertRaises(Exception):
      self.emergency_mgr.download_last_txlog(search_key, back_logfile)

    DummySession.behaviour = fail_at_first_then_ok(1, black=[DummyVault])
    DummySession.blowup_on_fail = False
    with self.assertRaises(Exception):
      self.emergency_mgr.download_last_txlog(search_key, back_logfile)

    DummySession.behaviour = fail_at_first_then_ok(1, black=[DummyVault])
    DummySession.blowup_on_fail = True
    fileseg = self.emergency_mgr.download_last_txlog(search_key, back_logfile)
    self.assertTrue( fileseg.archive_id == archive.id )

    DummySession.behaviour = fail_at_first_then_ok(1, black=[DummyJob])
    DummySession.blowup_on_fail = True
    fileseg = self.emergency_mgr.download_last_txlog(search_key, back_logfile)
    self.assertTrue( fileseg.archive_id == archive.id )

  #@ut.skip("For quick validation")
  def test_txlog_download_but_nofile(self):
    search_key = get_conf().app.transaction_log
    back_logfile = give_stage_filepath()

    archive = DummyArchive.create_archive_with_random_data(self.vault, 'this_is_not_the_search_key_1', 24)
    fileseg = self.emergency_mgr.download_last_txlog(search_key, back_logfile)
    self.assertEqual( None, fileseg.archive_id )

    self.vault._clear()
    fileseg = self.emergency_mgr.download_last_txlog(search_key, back_logfile)
    self.assertEqual( None, fileseg.archive_id )

  #@ut.skip("For quick validation")
  def test_resume_txlog_download_job_valid(self):
    search_key = get_conf().app.transaction_log
    inventory = self.emergency_mgr.check_for_recent_inventory_job()
    self.assertTrue( not inventory )

    archive = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 24)
    inventory1 = self.emergency_mgr.retrieve_vault_inventory()
    inventory2 = self.emergency_mgr.check_for_recent_inventory_job()
    self.assertEqual( inventory1['InventoryDate'], inventory2['InventoryDate'] )
    self.assertEqual( len(inventory1['ArchiveList']), len(inventory2['ArchiveList']) )

    self.vault._clear()
    archive = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 29)
    back_logfile = give_stage_filepath()
    fileseg = Fileseg(back_logfile, None, archive.id, (0,29*1024))

    fileseg2 = self.emergency_mgr.download_from_existing_job(fileseg)
    self.assertTrue( not fileseg2 )

    self.emergency_mgr.single_shot_download (fileseg)
    back_logfile = give_stage_filepath()
    fileseg2 = Fileseg(back_logfile, None, archive.id, (0,29*1024))

    fileseg2 = self.emergency_mgr.download_from_existing_job(fileseg2)
    self.assertTrue( fileseg2 )
    with open(fileseg.fileout, 'rb') as fileobj1:
      with open(fileseg2.fileout, 'rb') as fileobj2:
        self.assertEqual( fileobj1.seek(0, os.SEEK_END), fileobj2.seek(0, os.SEEK_END) )

  #@ut.skip("For quick validation")
  def test_resume_txlog_download_job_unrelated(self):
    search_key = get_conf().app.transaction_log
    archive1 = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 29)
    back_logfile = give_stage_filepath()
    fileseg1 = Fileseg(back_logfile, None, archive1.id, (0,29*1024))

    self.emergency_mgr.single_shot_download (fileseg1)
    self.assertTrue( len(self.vault.succeeded_jobs) )

    archive2 = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 29)
    back_logfile = give_stage_filepath()
    fileseg2 = Fileseg(back_logfile, None, archive2.id, (0,29*1024))

    result = self.emergency_mgr.download_from_existing_job(fileseg2)
    self.assertTrue( not result )

  #@ut.skip("For quick validation")
  def test_resume_txlog_download_job_failed(self):
    search_key = get_conf().app.transaction_log
    archive = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 24)
    inventory1 = self.emergency_mgr.retrieve_vault_inventory()
    for job in list(self.vault.jobs.all()): job._fail()

    inventory2 = self.emergency_mgr.check_for_recent_inventory_job()
    self.assertTrue( not inventory2 and inventory1 )

    self.vault._clear()
    archive = DummyArchive.create_archive_with_random_data(self.vault, search_key + '_1', 29)
    back_logfile = give_stage_filepath()
    fileseg1 = Fileseg(back_logfile, None, archive.id, (0,29*1024))

    fileseg1 = self.emergency_mgr.single_shot_download (fileseg1)
    for job in list(self.vault.jobs.all()): job._fail()

    back_logfile = give_stage_filepath()
    fileseg2 = Fileseg(back_logfile, None, archive.id, (0,29*1024))
    fileseg2 = self.emergency_mgr.download_from_existing_job(fileseg2)
    self.assertTrue( not fileseg2 and fileseg1 )

### END TestAwsGlacierEmergencyMgr

if __name__ == "__main__":
  conf_for_test()
  ut.main()

