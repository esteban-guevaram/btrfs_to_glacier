import unittest as ut
from common import *
from routines_for_test import *
from aws_glacier_mgr import *
from aws_session import *
from aws_mock import *
logger = logging.getLogger(__name__)

@deco_setup_each_test
class TestAwsGlacierMgr (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  def setUp(self):
    get_conf().aws.chunk_size_in_mb = 1
    self.session = DummySession.create_dummy_session()
    self.glacier_res = self.session.resource('glacier')
    self.glacier_mgr = AwsGlacierManager(self.session)
    self.up_session = AwsGlobalSession(Record.SESSION_UPLD)
    self.down_session = AwsGlobalSession(Record.SESSION_DOWN)
    self.vault = self.glacier_res.Vault('', get_conf().aws.glacier_vault)

  def tearDown(self):
    if self.vault: self.vault._clear()

  #@ut.skip("For quick validation")
  def test_single_shoot_upload(self):
    fileseg = add_rand_file_to_staging(256)
    archive = self.glacier_mgr.upload(self.up_session, fileseg)
    count_per_type = calculate_record_type_count()

    assert len(self.vault._archives) == 1
    assert len(self.up_session.filesegs) == 1
    assert self.up_session.filesegs[fileseg.key()].range_bytes[1] == 256 * 1024
    assert len(get_txlog()) == 2
    assert count_per_type[Record.FILESEG_START] > 0
    assert count_per_type[Record.FILESEG_END] > 0

  #@ut.skip("For quick validation")
  def test_single_shoot_upload_out_of_session(self):
    fileseg = add_rand_file_to_staging(256)
    archive = self.glacier_mgr.upload_out_of_session(fileseg)

    assert len(self.vault._archives) == 1
    assert len(get_txlog()) == 0

    # no out of session uploads for big files
    fileseg = add_rand_file_to_staging(1025)
    with self.assertRaises(Exception):
      archive = self.glacier_mgr.upload_out_of_session(fileseg)

  #@ut.skip("For quick validation")
  def test_single_shoot_upload_failures(self):
    fileseg = add_rand_file_to_staging(256)

    DummySession.behaviour = always_ko_behaviour()
    with self.assertRaises(Exception):
      archive = self.glacier_mgr.upload(self.up_session, fileseg)

    self.vault._clear()
    self.up_session = AwsGlobalSession(Record.SESSION_UPLD)
    DummySession.behaviour = fail_at_first_then_ok(1)
    DummySession.blowup_on_fail = True

    archive = self.glacier_mgr.upload(self.up_session, fileseg)
    assert archive.id in self.vault._archives
    archive._close()

    self.vault._clear()
    self.up_session = AwsGlobalSession(Record.SESSION_UPLD)
    DummySession.behaviour = fail_at_first_then_ok(1)
    DummySession.blowup_on_fail = False
    with self.assertRaises(Exception):
      archive = self.glacier_mgr.upload(self.up_session, fileseg)

  #@ut.skip("For quick validation")
  def test_multipart_upload(self):
    fileseg = add_rand_file_to_staging(1256)
    archive = self.glacier_mgr.upload(self.up_session, fileseg)
    assert len(self.up_session.filesegs) == 1
    assert self.up_session.filesegs[fileseg.key()].range_bytes[1] == 1256 * 1024
    assert len(self.up_session.filesegs[fileseg.key()].chunks) == 2

    fileseg = add_rand_file_to_staging(4096)
    archive = self.glacier_mgr.upload(self.up_session, fileseg)
    assert len(self.up_session.filesegs) == 2
    assert self.up_session.filesegs[fileseg.key()].range_bytes[1] == 4096 * 1024
    assert len(self.up_session.filesegs[fileseg.key()].chunks) == 4

    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 2
    assert count_per_type[Record.FILESEG_END] == 2
    assert count_per_type[Record.CHUNK_END] >= 2

  #@ut.skip("For quick validation")
  def test_multipart_upload_failures_all_ok(self):
    fileseg = add_rand_file_to_staging(1024 * 3)
    DummySession.behaviour = always_ko_behaviour()
    with self.assertRaises(Exception):
      archive = self.glacier_mgr.upload(self.up_session, fileseg)
    count_per_type = calculate_record_type_count()
    assert not count_per_type.get(Record.FILESEG_START)
    assert not count_per_type.get(Record.FILESEG_END)
    assert not count_per_type.get(Record.CHUNK_END)

  #@ut.skip("For quick validation")
  def test_multipart_upload_failure_transient_multipart(self):
    fileseg = add_rand_file_to_staging(1024 * 3)
    DummySession.behaviour = fail_at_first_then_ok(1, black=[DummyMultiPart])
    DummySession.blowup_on_fail = True

    archive = self.glacier_mgr.upload(self.up_session, fileseg)
    archive._close()

    count_per_type = calculate_record_type_count()
    assert archive.id in self.vault._archives
    assert count_per_type[Record.FILESEG_START] == 1
    assert count_per_type[Record.FILESEG_END] == 1
    assert count_per_type[Record.CHUNK_END] >= 1

  #@ut.skip("For quick validation")
  def test_multipart_upload_failure_transient_vault(self):
    fileseg = add_rand_file_to_staging(1024 * 3)
    DummySession.behaviour = fail_at_first_then_ok(1, black=[DummyVault])
    DummySession.blowup_on_fail = True
    archive = self.glacier_mgr.upload(self.up_session, fileseg)
    assert archive.id in self.vault._archives, '%r, %r' % (archive.id, self.vault._archives)

  #@ut.skip("For quick validation")
  def test_multipart_upload_failure_transient_hard_multipart(self):
    fileseg = add_rand_file_to_staging(1024 * 3)
    DummySession.behaviour = fail_at_first_then_ok(1, black=[DummyMultiPart])
    DummySession.blowup_on_fail = False
    with self.assertRaises(Exception):
      archive = self.glacier_mgr.upload(self.up_session, fileseg)
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1

  #@ut.skip("For quick validation")
  def test_singleshot_resume_job(self):
    fileseg = add_rand_file_to_staging(256)
    self.up_session.start_fileseg_single_chunk(fileseg)
    archive = self.glacier_mgr.finish_pending_upload(self.up_session)

    assert len(self.vault._archives) == 1, repr(self.vault._archives)
    assert len(self.up_session.filesegs) == 1
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_singleshot_resume_job_session_started(self):
    fileseg = add_rand_file_to_staging(666)
    self.up_session.start_fileseg_single_chunk(fileseg)
    archive = self.glacier_mgr.finish_pending_upload(self.up_session)

    assert len(self.vault._archives) == 1, repr(self.vault._archives)
    assert len(self.up_session.filesegs) == 1
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert not count_per_type.get(Record.CHUNK_END), repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_singleshot_resume_job_fail(self):
    fileseg = add_rand_file_to_staging(256)
    DummySession.behaviour = always_ko_behaviour()
    with self.assertRaises(Exception):
      self.glacier_mgr.upload(self.up_session, fileseg)

    DummySession.behaviour = fail_at_first_then_ok(1)
    archive = self.glacier_mgr.finish_pending_upload(self.up_session)

    assert len(self.vault._archives) == 1, repr(self.vault._archives)
    assert len(self.up_session.filesegs) == 1
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_multipart_upload_resume(self):
    size_kb = 2048 + 256
    first_chunk = get_rand_data(1024)
    fileseg = add_rand_file_to_staging(size_kb)

    job = self.vault.initiate_multipart_upload(
      archiveDescription=fileseg.fileout,
      partSize=len(first_chunk),
    )
    fileseg.aws_id = job.id

    job.upload_part(
      body = first_chunk,
      range = build_mime_range((0, len(first_chunk))),
      checksum = TreeHasher().digest_single_shot_as_hexstr(first_chunk),
    )
    
    self.up_session.start_fileseg(fileseg)
    self.up_session.close_chunk(fileseg.key(), (0,len(first_chunk)))

    archive = self.glacier_mgr.finish_pending_upload(self.up_session)
    archive._close()

    assert len(self.vault._archives) == 1, repr(self.vault._archives)
    assert len(self.up_session.filesegs) == 1
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert count_per_type[Record.CHUNK_END] == 3, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_multipart_upload_resume_empty_session(self):
    size_kb = 1024 + 71
    fileseg = add_rand_file_to_staging(size_kb)

    job = self.vault.initiate_multipart_upload(
      archiveDescription=fileseg.fileout,
      partSize=1024**2,
    )
    fileseg.aws_id = job.id
    self.up_session.start_fileseg(fileseg)

    archive = self.glacier_mgr.finish_pending_upload(self.up_session)
    archive._close()

    assert len(self.vault._archives) == 1, repr(self.vault._archives)
    assert len(self.up_session.filesegs) == 1
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert count_per_type[Record.CHUNK_END] == 2, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_multipart_upload_expired_job(self):
    size_kb = 1024 + 71
    fileseg = add_rand_file_to_staging(size_kb)

    job = self.vault.initiate_multipart_upload(
      archiveDescription=fileseg.fileout,
      partSize=1024**2,
    )
    job._expire()
    fileseg.aws_id = job.id
    self.up_session.start_fileseg(fileseg)

    archive = self.glacier_mgr.finish_pending_upload(self.up_session)
    archive._close()

    assert len(self.vault._archives) == 1, repr(self.vault._archives)
    assert len(self.up_session.filesegs) == 1
    count_per_type = calculate_record_type_count()
    # since we have 2 upload jobs there will be 2 fileseg starts
    assert count_per_type[Record.FILESEG_START] == 2, repr(count_per_type)
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert count_per_type[Record.CHUNK_END] == 2, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_multipart_upload_expired_with_session(self):
    size_kb = 2048 + 256
    first_chunk = get_rand_data(1024)
    fileseg = add_rand_file_to_staging(size_kb)

    job = self.vault.initiate_multipart_upload(
      archiveDescription=fileseg.fileout,
      partSize=len(first_chunk),
    )
    fileseg.aws_id = job.id

    job.upload_part(
      body = first_chunk,
      range = build_mime_range((0, len(first_chunk))),
      checksum = TreeHasher().digest_single_shot_as_hexstr(first_chunk),
    )
    job._expire()
    
    self.up_session.start_fileseg(fileseg)
    self.up_session.close_chunk(fileseg.key(), (0,len(first_chunk)))

    archive = self.glacier_mgr.finish_pending_upload(self.up_session)
    archive._close()

    # we will have 1 repeated chunk since we canot alter the session
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.CHUNK_END] == 4, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_multipart_upload_resume_checksum_not_match(self):
    size_kb = 2048 + 256
    first_chunk = get_rand_data(1024)
    fileseg = add_rand_file_to_staging(size_kb)

    job = self.vault.initiate_multipart_upload(
      archiveDescription=fileseg.fileout,
      partSize=len(first_chunk),
    )
    fileseg.aws_id = job.id

    job.upload_part(
      body = first_chunk,
      range = build_mime_range((0, len(first_chunk))),
      checksum = TreeHasher().digest_single_shot_as_hexstr(b'this is wrong'),
    )
    
    self.up_session.start_fileseg(fileseg)
    self.up_session.close_chunk(fileseg.key(), (0,len(first_chunk)))

    with self.assertRaises(Exception):
      archive = self.glacier_mgr.finish_pending_upload(self.up_session)
      archive._close()

  #@ut.skip("For quick validation")
  def test_multipart_upload_resume_failures(self):
    size_kb = 2048 + 256
    first_chunk = get_rand_data(1024)
    fileseg = add_rand_file_to_staging(size_kb)

    job = self.vault.initiate_multipart_upload(
      archiveDescription=fileseg.fileout,
      partSize=len(first_chunk),
    )
    fileseg.aws_id = job.id

    job.upload_part(
      body = first_chunk,
      range = build_mime_range((0, len(first_chunk))),
      checksum = TreeHasher().digest_single_shot_as_hexstr(first_chunk),
    )
    
    self.up_session.start_fileseg(fileseg)
    self.up_session.close_chunk(fileseg.key(), (0,len(first_chunk)))

    DummySession.behaviour = always_ko_behaviour()
    with self.assertRaises(Exception):
      archive = self.glacier_mgr.finish_pending_upload(self.up_session)
      archive._close()

    DummySession.behaviour = fail_at_first_then_ok(1)
    DummySession.blowup_on_fail = True
    archive = self.glacier_mgr.finish_pending_upload(self.up_session)
    archive._close()

    assert len(self.vault._archives) == 1, repr(self.vault._archives)
    assert len(self.up_session.filesegs) == 1
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert count_per_type[Record.CHUNK_END] == 3, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_get_all_down_jobs_in_vault_by_stx(self):
    DummyJob.CompleteAsSoonAsCreated = False
    size_kb = 1024 + 71
    fileseg = add_rand_file_to_staging(size_kb)
    fileseg.archive_id = 'lskdjflsdkf'

    up_job = self.vault.initiate_multipart_upload(
      archiveDescription=fileseg.fileout,
      partSize=1024**2,
    )
    down_job1 = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    down_job2 = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    down_job3 = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    down_job2._complete()
    down_job3._fail()

    meta_job = self.vault.initiate_inventory_retrieval(
      Type = 'inventory-retrieval',
      Tier='banana',
    )

    job_stx = self.glacier_mgr.get_all_down_jobs_in_vault_by_stx()
    count_per_type = { s:len(l) for s,l in job_stx.items() }
    assert count_per_type[DummyJob.Succeeded] == 1, repr(count_per_type)
    assert count_per_type[DummyJob.Failed] == 1, repr(count_per_type)
    assert count_per_type[DummyJob.InProgress] == 1, repr(count_per_type)
    assert sum( c for c in count_per_type.values() ) == 3

  #@ut.skip("For quick validation")
  def test_get_all_down_jobs_in_vault_more_than_max(self):
    DummyJob.CompleteAsSoonAsCreated = False
    get_conf().aws.glacier_max_jobs_in_flight = 2
    size_kb = 1024 + 71
    fileseg = add_rand_file_to_staging(size_kb)
    fileseg.archive_id = 'lskdjflsdkf'

    down_job1 = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    down_job2 = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    down_job3 = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)

    job_stx = self.glacier_mgr.get_all_down_jobs_in_vault_by_stx()
    count_per_type = { s:len(l) for s,l in job_stx.items() }
    assert count_per_type.get(DummyJob.InProgress) == 3, repr(count_per_type)
    assert sum( c for c in count_per_type.values() ) == 3

  #@ut.skip("For quick validation")
  def test_load_hasher_with_uploaded_chunks_checksums_multiplage(self):
    multijob = DummyMultiPartMultiPage()
    hasher = TreeHasher()
    count = self.glacier_mgr.load_hasher_with_uploaded_chunks_checksums(multijob, hasher)
    assert count == 60, count

  #@ut.skip("For quick validation")
  def test_simple_job_download(self):
    fileseg = add_rand_file_to_staging(4096)
    fileseg.archive_id = 'lskdjflsdkf'
    job = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    job._complete()

    fileseg = self.glacier_mgr.get_job_ouput_to_fs(self.down_session, job, fileseg)
    assert fileseg.range_bytes == (0, 4096*1024)
    assert len(fileseg.chunks) == 4
    assert fileseg.done and all( c.done for c in fileseg.chunks )

    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert count_per_type[Record.CHUNK_END] == 4, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_fail_init_arch_retrieval(self):
    fileseg = add_rand_file_to_staging(4096)
    fileseg.archive_id = 'lskdjflsdkf'
    DummySession.behaviour = always_ko_behaviour()
    DummySession.blowup_on_fail = True

    with self.assertRaises(Exception):
      job = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)

  #@ut.skip("For quick validation")
  def test_fail_init_arch_retrieval_transient(self):
    fileseg = add_rand_file_to_staging(4096)
    fileseg.archive_id = 'lskdjflsdkf'
    DummySession.behaviour = fail_at_first_then_ok(1)
    DummySession.blowup_on_fail = True

    job = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)
    assert next(r for r in get_txlog().reverse_iterate_through_records() if r.r_type == Record.FILESEG_START).aws_id == job.id

  #@ut.skip("For quick validation")
  def test_simple_job_download_all_fail(self):
    fileseg = add_rand_file_to_staging(4096)
    fileseg.archive_id = 'lskdjflsdkf'
    job = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    job._complete()

    DummySession.behaviour = always_ko_behaviour()
    DummySession.blowup_on_fail = True
    with self.assertRaises(Exception):
      self.glacier_mgr.get_job_ouput_to_fs(self.down_session, job, fileseg)

    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)

    DummySession.blowup_on_fail = False
    with self.assertRaises(Exception):
      self.glacier_mgr.get_job_ouput_to_fs(self.down_session, job, fileseg)

    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_simple_job_download_transient(self):
    fileseg = add_rand_file_to_staging(3096 + 256)
    fileseg.archive_id = 'lskdjflsdkf'
    job = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    job._complete()

    DummySession.behaviour = fail_at_first_then_ok(1)
    DummySession.blowup_on_fail = True
    self.glacier_mgr.get_job_ouput_to_fs(self.down_session, job, fileseg)

    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)

    DummySession.blowup_on_fail = False
    with self.assertRaises(Exception):
      self.glacier_mgr.get_job_ouput_to_fs(self.down_session, job, fileseg)

    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_START] == 1, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_start_before_job_complete(self):
    DummyJob.CompleteAsSoonAsCreated = False
    fileseg = add_rand_file_to_staging(4096)
    fileseg.archive_id = 'lskdjflsdkf'
    job = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)

    # this must fail because the job is not complete
    with self.assertRaises(Exception):
      self.glacier_mgr.get_job_ouput_to_fs(self.down_session, job, fileseg)

  #@ut.skip("For quick validation")
  def test_download_bad_hash(self):
    fileseg = add_rand_file_to_staging(4096)
    fileseg.archive_id = 'lskdjflsdkf'
    job = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    job.calculate_hash_and_rewind = lambda f : 'coucou'
    job._complete()

    with self.assertRaises(Exception):
      self.glacier_mgr.get_job_ouput_to_fs(self.down_session, job, fileseg)

    count_per_type = calculate_record_type_count()
    assert not count_per_type.get(Record.FILESEG_END), repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_download_failed_job(self):
    fileseg = add_rand_file_to_staging(4096)
    fileseg.archive_id = 'lskdjflsdkf'
    job = self.glacier_mgr.initiate_archive_retrieval(self.down_session, fileseg)
    job._fail()

    with self.assertRaises(Exception):
      self.glacier_mgr.get_job_ouput_to_fs(self.down_session, job, fileseg)

  #@ut.skip("For quick validation")
  def test_dowload_resume_half_done_job(self):
    fileseg = add_rand_file_to_staging(4096)
    fileseg.archive_id = 'lskdjflsdkf'
    fileseg.aws_id = 'lskdjflsdkf'
    
    self.down_session.start_fileseg(fileseg)
    self.down_session.close_chunk(fileseg.key(), (0,1024**2))
    self.down_session.close_chunk(fileseg.key(), (1024**2, 2*1024**2))
    assert len(fileseg.chunks) == 2

    job = DummyJob('retrieval_job', self.vault, DummyJob.ArchiveRetrieval, 0, 4096*1024)
    self.vault.jobs_in_progress[job.id] = job
    job._complete()
    new_fileseg = self.glacier_mgr.finish_job_output_to_fs(self.down_session, job)

    count_per_type = calculate_record_type_count()
    assert new_fileseg.range_bytes == (0, 4096*1024)
    assert len(new_fileseg.chunks) == 4 and all( c.done for c in new_fileseg.chunks )
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert count_per_type[Record.CHUNK_END] == 4, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_dowload_resume_no_previous_chunks(self):
    size_kb = 3*1024 + 256
    fileseg = add_rand_file_to_staging(size_kb)
    fileseg.archive_id = 'lskdjflsdkf'
    fileseg.aws_id = 'lskdjflsdkf'
    
    self.down_session.start_fileseg(fileseg)

    job = DummyJob('retrieval_job', self.vault, DummyJob.ArchiveRetrieval, 0, size_kb*1024)
    self.vault.jobs_in_progress[job.id] = job
    job._complete()
    new_fileseg = self.glacier_mgr.finish_job_output_to_fs(self.down_session, job)

    count_per_type = calculate_record_type_count()
    assert new_fileseg.range_bytes == (0, (3*1024 + 256)*1024)
    assert len(new_fileseg.chunks) == 4 and all( c.done for c in new_fileseg.chunks )
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert count_per_type[Record.CHUNK_END] == 4, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_dowload_resume_all_done(self):
    size_kb = 2048
    fileseg = add_rand_file_to_staging(size_kb)
    fileseg.archive_id = 'lskdjflsdkf'
    fileseg.aws_id = 'lskdjflsdkf'
    
    self.down_session.start_fileseg(fileseg)
    self.down_session.close_chunk(fileseg.key(), (0,1024**2))
    self.down_session.close_chunk(fileseg.key(), (1024**2, 2*1024**2))

    job = DummyJob('retrieval_job', self.vault, DummyJob.ArchiveRetrieval, 0, size_kb*1024)
    self.vault.jobs_in_progress[job.id] = job
    job._complete()
    new_fileseg = self.glacier_mgr.finish_job_output_to_fs(self.down_session, job)

    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert count_per_type[Record.CHUNK_END] == 2, repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_dowload_resume_but_nothing_in_session(self):
    job = DummyJob('retrieval_job', self.vault, DummyJob.ArchiveRetrieval, 0, 4096*1024)
    self.vault.jobs_in_progress[job.id] = job
    job._complete()
    with self.assertRaises(Exception):
      self.glacier_mgr.finish_job_output_to_fs(self.down_session, job)

    count_per_type = calculate_record_type_count()
    assert not count_per_type.get(Record.FILESEG_END), repr(count_per_type)

  #@ut.skip("For quick validation")
  def test_dowload_resume_all_fail(self):
    size_kb = 2048 + 256
    fileseg = add_rand_file_to_staging(size_kb)
    fileseg.archive_id = 'lskdjflsdkf'
    fileseg.aws_id = 'lskdjflsdkf'
    
    self.down_session.start_fileseg(fileseg)
    self.down_session.close_chunk(fileseg.key(), (0,1024**2))

    job = DummyJob('retrieval_job', self.vault, DummyJob.ArchiveRetrieval, 0, size_kb*1024)
    self.vault.jobs_in_progress[job.id] = job
    job._complete()

    DummySession.behaviour = always_ko_behaviour()
    with self.assertRaises(Exception):
      self.glacier_mgr.finish_job_output_to_fs(self.down_session, job)

  #@ut.skip("For quick validation")
  def test_dowload_resume_transient_fail(self):
    size_kb = 2048 + 256
    fileseg = add_rand_file_to_staging(size_kb)
    fileseg.archive_id = 'lskdjflsdkf'
    fileseg.aws_id = 'lskdjflsdkf'
    
    self.down_session.start_fileseg(fileseg)
    self.down_session.close_chunk(fileseg.key(), (0,1024**2))

    job = DummyJob('retrieval_job', self.vault, DummyJob.ArchiveRetrieval, 0, size_kb*1024)
    self.vault.jobs_in_progress[job.id] = job
    job._complete()

    DummySession.behaviour = fail_at_first_then_ok(1)
    DummySession.blowup_on_fail = True

    self.glacier_mgr.finish_job_output_to_fs(self.down_session, job)
    count_per_type = calculate_record_type_count()
    assert count_per_type[Record.FILESEG_END] == 1, repr(count_per_type)
    assert count_per_type[Record.CHUNK_END] == 3, repr(count_per_type)

### END TestAwsGlacierMgr

if __name__ == "__main__":
  conf_for_test()
  ut.main()

