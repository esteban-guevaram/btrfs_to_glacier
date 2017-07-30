import uuid, shutil, contextlib
import unittest.mock as mock
from routines_for_test_base import *
from common import *
from aws_session import *
from aws_mock import *
from file_utils import *
from btrfs_commands import *
from btrfs_subvol_list import *
from btrfs_backup_orchestrator import *
from btrfs_restore_orchestrator import *
import btrfs_restore_orchestrator
from transaction_log import *
from txlog_consistency import *
logger = logging.getLogger(__name__)

class DummyBtrfsNode (object):

  def __init__ (self):
    self.name = None
    self.path = None
    self.uuid = None
    self.puuid = None
    self.ruuid = None
    self.creation_utc = None
    self.is_snap = False
    self.is_read = False
 
  @staticmethod
  def build ():
    dummy = DummyBtrfsNode()
    volid = uuid.uuid4()
    dummy.name = volid.hex
    dummy.path = '/unexisting_dir/' + volid.hex
    dummy.uuid = volid.bytes
    dummy.creation_utc = datetime.datetime.now()
    return dummy

  @staticmethod
  def receive (rvol, predecessor=None):
    dummy = DummyBtrfsNode.build()
    dummy.ruuid = rvol.uuid
    if predecessor:
      dummy.puuid = predecessor.uuid
    dummy.is_read = True
    return dummy

  @staticmethod
  def snap (parent):
    dummy = DummyBtrfsNode.build()
    dummy.puuid = parent.uuid
    dummy.is_snap = True
    dummy.is_read = True
    return dummy

  def is_snapshot (self): return self.is_snap   
  def is_readonly (self): return self.is_read   

#################################################################################
# Transaction log

def fake_backup_file_tx (fileout, snap, predecessor=None):
  fileout = get_conf().app.staging_dir + "/" + fileout
  hashstr = uuid.uuid4().hex
  get_txlog().record_snap_to_file(fileout, hashstr, snap, predecessor)

def add_fake_backup_to_txlog (with_session=False):
  vol1 = DummyBtrfsNode.build()
  vol2 = DummyBtrfsNode.build()
  snap1 = DummyBtrfsNode.snap(vol1)
  snap2 = DummyBtrfsNode.snap(vol2)

  if with_session: get_txlog().record_backup_start()
  get_txlog().record_snap_creation(snap1)
  fake_backup_file_tx('fs1', snap1)
  get_txlog().record_snap_creation(snap2)
  fake_backup_file_tx('fs2', snap2)

  snap12 = DummyBtrfsNode.snap(vol1)
  snap22 = DummyBtrfsNode.snap(vol2)
  get_txlog().record_snap_creation(snap12)
  fake_backup_file_tx('fs12', snap12, snap1)
  get_txlog().record_snap_creation(snap22)
  fake_backup_file_tx('fs22', snap22, snap2)

  get_txlog().record_subvol_delete(snap1)
  get_txlog().record_subvol_delete(snap2)
  if with_session: get_txlog().record_backup_end()

def add_fake_restore_to_txlog (with_session=False):
  vol1 = DummyBtrfsNode.build()
  vol2 = DummyBtrfsNode.build()
  rest1 = DummyBtrfsNode.receive(vol1, None)
  rest2 = DummyBtrfsNode.receive(vol2, None)
  rest12 = DummyBtrfsNode.receive(vol1, rest1)
  rest22 = DummyBtrfsNode.receive(vol2, rest2)

  if with_session: get_txlog().record_restore_start()
  get_txlog().record_file_to_snap(get_conf().app.staging_dir + '/fs1', rest1, vol1.uuid)
  get_txlog().record_file_to_snap(get_conf().app.staging_dir + '/fs2', rest2, vol2.uuid)
  get_txlog().record_file_to_snap(get_conf().app.staging_dir + '/fs12', rest12, vol1.uuid)
  get_txlog().record_file_to_snap(get_conf().app.staging_dir + '/fs22', rest22, vol2.uuid)
  get_txlog().record_subvol_delete(rest1)
  get_txlog().record_subvol_delete(rest2)
  if with_session: get_txlog().record_restore_end()

def add_fake_upload_to_txlog (with_session=False):
  fs1 = Fileseg.build_from_fileout(get_conf().app.staging_dir + '/fs1', (0,2048))
  fs1.aws_id = 'multipart_upload_id1'
  fs2 = Fileseg.build_from_fileout(get_conf().app.staging_dir + '/fs2', (0,1024))
  fs2.aws_id = 'multipart_upload_id2'

  if with_session: get_txlog().record_aws_session_start(Record.SESSION_UPLD)
  get_txlog().record_fileseg_start(fs1)
  get_txlog().record_chunk_end([0,1024])
  get_txlog().record_chunk_end([1024,2048])
  get_txlog().record_fileseg_end('archive_id_1')
  get_txlog().record_fileseg_start(fs2)
  get_txlog().record_fileseg_end('archive_id_2')
  if with_session: get_txlog().record_aws_session_end(Record.SESSION_UPLD)

def add_fake_download_to_txlog (with_session=False):
  fs1 = Fileseg.build_from_fileout(get_conf().app.staging_dir + '/fs1', (0,2048))
  fs1.aws_id = 'download_id1'
  fs2 = Fileseg.build_from_fileout(get_conf().app.staging_dir + '/fs2', (0,1024))
  fs2.aws_id = 'download_id2'

  if with_session: get_txlog().record_aws_session_start(Record.SESSION_DOWN)
  get_txlog().record_fileseg_start(fs1)
  get_txlog().record_chunk_end([0,1024])
  get_txlog().record_chunk_end([1024,2048])
  get_txlog().record_fileseg_end()
  get_txlog().record_fileseg_start(fs2)
  get_txlog().record_fileseg_end()
  if with_session: get_txlog().record_aws_session_end(Record.SESSION_DOWN)

def calculate_record_type_count(txlog=None):
  txlog = txlog or get_txlog()
  record_type_count = {}
  for record in txlog.iterate_through_records():
    if record.r_type not in record_type_count:
      record_type_count[record.r_type] = 0
    record_type_count[record.r_type] += 1
  return record_type_count  

#################################################################################
# Session routines

def pull_last_childs_from_session (session):
  return { k:v[-1] for k,v in session.child_subvols.items() }

def all_child_sv_in_session (session):
  return [ sv for sv_list in session.child_subvols.values() for sv in sv_list ]

def get_send_file_and_snap_from (backup_session, puuid):
  fileout = backup_session.btrfs_sv_files[puuid][-1]
  snap = backup_session.child_subvols[puuid][-1]
  return fileout, snap

#################################################################################
# Tests setup

def deco_setup_each_test (klass):
  user_set_up = None
  if hasattr(klass, 'setUp'):
    user_set_up = klass.setUp

  def setUp(self):
    logger.info("*** Running : %r", self.id())
    reset_conf() # this has to come first, before restoring other components
    clean_tx_log()
    clean_send_file_staging()
    change_timestamp()
    DummySession.behaviour = always_ok_behaviour()
    DummySession.blowup_on_fail = True
    DummyJob.CompleteAsSoonAsCreated = True
    user_set_up and user_set_up(self)
  
  klass.setUp = setUp
  return klass

def setup_filesystem(extra_options, subvol_paths):
  avoid_shoot_in_the_foot()
  script_path = os.path.dirname(os.path.realpath(__file__)) + '/' + get_conf().test.btrfs_setup_script
  subvol_names = [ os.path.basename(n) for n in subvol_paths ]

  setup_cmd = [ script_path, '-d', get_conf().test.btrfs_device ]
  setup_cmd.extend(extra_options)
  setup_cmd.extend(subvol_names)

  logger.info("Reset filesystem : %r", setup_cmd)
  with ProcessGuard(setup_cmd, stdin=None, stdout=sp.PIPE, stderr=sp.PIPE) as proc:
    (out, err) = proc.communicate()
  assert proc.returncode == 0

def clean_send_file_staging ():
  stage_dir = get_conf().app.staging_dir
  # Uncomment to save the staging dirs for each test
  #safe_copy = os.path.dirname(stage_dir) + '/stage_' + uuid.uuid4().hex
  #shutil.move(stage_dir, safe_copy)
  if os.path.isdir(stage_dir):
    shutil.rmtree(stage_dir)
  os.mkdir(stage_dir)

def restore_txlog_from_file(backup_path):
  # we delete both the in memory txlog and its file backed copy
  if os.path.isfile(get_conf().app.transaction_log):
    os.remove(get_conf().app.transaction_log)
  reset_txlog()
  TransactionLog.restore_from_crypted_file(backup_path)

def clean_tx_log():
  if os.path.isfile(get_conf().app.transaction_log):
    os.remove(get_conf().app.transaction_log)
  reset_txlog()  

ts_state = 0
def change_timestamp():
  # We need to modify the timestamp or there will be filename collision
  global ts_state
  now = datetime.datetime.now()
  str_now = now.strftime('%Y%m%d%H%M%S')
  timestamp.str = "%s_%d" % (str_now, ts_state)
  ts_state += 1

def avoid_shoot_in_the_foot ():
  device = os.path.basename( get_conf().test.btrfs_device )
  root_fs = get_conf().test.root_fs
  targets = get_conf().btrfs.target_subvols
  snap_path = get_conf().btrfs.backup_subvol
  restore_path = get_conf().btrfs.restore_path

  assert device and root_fs and targets and snap_path and restore_path
  assert root_fs.lower().find('test') >= 0
  assert snap_path.lower().find('test') >= 0
  assert restore_path.lower().find('test') >= 0
  assert all( p.lower().find('test') >= 0 for p in targets )

  with open('/sys/block/' + device + '/removable', 'r') as fileobj:
    dev_removable = int( fileobj.read().strip() ) 
  with open('/sys/block/' + device + '/size', 'r') as fileobj:
    dev_size = int( fileobj.read().strip() ) 

  assert dev_size * 512 < 8 * 1024**3 and 7 * 1024**3 < dev_size * 512
  assert dev_removable == 1

#################################################################################
# Mocks setup

@contextlib.contextmanager
def use_mock_logger_on_module(module):
  real_logger = module.logger
  mock_logger = mock.Mock()
  module.logger = mock_logger
  try: yield mock_logger
  finally: module.logger = real_logger

