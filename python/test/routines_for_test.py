import uuid, shutil
from common import *
from transaction_log import reset_txlog, get_txlog
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
    dummy.path = '/root/' + volid.hex
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

def fake_backup_file_tx (snap, predecessor):
  fileout = "/root/sendfile/" + snap.name
  hashstr = uuid.uuid4().hex
  get_txlog().record_snap_to_file(fileout, hashstr, snap, predecessor)

def add_fake_backup_to_txlog ():
  vol1 = DummyBtrfsNode.build()
  vol2 = DummyBtrfsNode.build()
  snap1 = DummyBtrfsNode.snap(vol1)
  snap2 = DummyBtrfsNode.snap(vol2)

  get_txlog().record_snap_creation(vol1, snap1)
  fake_backup_file_tx(snap1, None)
  get_txlog().record_snap_creation(vol2, snap2)
  fake_backup_file_tx(snap2, None)

  snap12 = DummyBtrfsNode.snap(vol1)
  snap22 = DummyBtrfsNode.snap(vol2)
  get_txlog().record_snap_creation(vol1, snap12)
  fake_backup_file_tx(snap12, snap1)
  get_txlog().record_snap_creation(vol2, snap22)
  fake_backup_file_tx(snap22, snap2)

  get_txlog().record_subvol_delete(snap1)
  get_txlog().record_subvol_delete(snap2)
  #get_txlog().record_txlog_to_file()

def add_fake_restore_to_txlog ():
  vol1 = DummyBtrfsNode.build()
  vol2 = DummyBtrfsNode.build()
  rest1 = DummyBtrfsNode.receive(vol1, None)
  rest2 = DummyBtrfsNode.receive(vol2, None)
  rest12 = DummyBtrfsNode.receive(vol1, rest1)
  rest22 = DummyBtrfsNode.receive(vol2, rest2)

  get_txlog().record_file_to_snap('/root/sendfile/' + rest1.name, rest1)
  get_txlog().record_file_to_snap('/root/sendfile/' + rest2.name, rest2)
  get_txlog().record_file_to_snap('/root/sendfile/' + rest12.name, rest12)
  get_txlog().record_file_to_snap('/root/sendfile/' + rest22.name, rest22)
  get_txlog().record_subvol_delete(rest1)
  get_txlog().record_subvol_delete(rest2)

def clean_send_file_staging ():
  stage_dir = get_conf().btrfs.send_file_staging
  safe_copy = os.path.dirname(stage_dir) + '/stage_' + uuid.uuid4().hex
  shutil.move(stage_dir, safe_copy)
  os.mkdir(stage_dir)

def clean_tx_log():
  if os.path.isfile(get_conf().app.transaction_log):
    os.remove(get_conf().app.transaction_log)
  reset_txlog()  

ts_state = 0
def change_timestamp():
  # We need to modify the timestamp or there will be filename collision
  global ts_state
  now = datetime.datetime.now()
  str_now = now.strftime('%Y%m%d%H%M')
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

def calculate_record_type_count():
  record_type_count = {}
  for record in get_txlog().iterate_through_records():
    if record.r_type not in record_type_count:
      record_type_count[record.r_type] = 0
    record_type_count[record.r_type] += 1
  return record_type_count  

def add_rand_file_to_all_targets(targets):
  for target in targets:
    add_rand_file_to_dir(target.path)

def add_rand_file_to_dir(path):
  with tempfile.NamedTemporaryFile(mode='w', dir=path, delete=False) as fileobj:
    logger.debug("Writing %s", fileobj.name)
    os.chmod(fileobj.name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
    for i in range(1024):
      fileobj.write("%d\n" % random.randint(0,1024*1024*1024))

def setup_filesystem(extra_options, subvol_paths):
  avoid_shoot_in_the_foot()
  script_path = os.path.dirname(os.path.realpath(__file__)) + '/' + get_conf().test.btrfs_setup_script
  subvol_names = [ os.path.basename(n) for n in subvol_paths ]

  setup_cmd = [ script_path, '-d', get_conf().test.btrfs_device ]
  setup_cmd.extend(extra_options)
  setup_cmd.extend(subvol_names)

  logger.info("Reset filesystem : %r", setup_cmd)
  sp.check_call( setup_cmd )

def compare_all_in_dir(left, right, depth=0):
  if not depth:
    logger.info("[%d] Comparing %s vs %s", depth, left, right)
  else:  
    logger.debug("[%d] Comparing %s vs %s", depth, left, right)

  if not os.path.isdir(right):
    return -1
  if not os.path.isdir(left):
    return 1

  cur_dir, dirs, files = next( os.walk(left) )
  for dirname in dirs:
    result = compare_all_in_dir(left + '/' + dirname, right + '/' + dirname, depth+1)
    if result: return result
  for filename in files:
    result = compare_text_files(left + '/' + filename, right + '/' + filename)
    if result: return result
  return 0  

def compare_text_files(left, right):
  logger.debug("Comparing %s vs %s", left, right)

  if not os.path.isfile(right):
    return -1
  if not os.path.isfile(left):
    return 1

  with open(left, 'rb') as left_file:
    with open(right, 'rb') as right_file:
      left_text = left_file.read()
      right_text = right_file.read()
      if left_text == right_text:
        return 0
      return 1  

def modify_random_byte_in_file (filein, min_offset=0):
  size = os.path.getsize(filein)
  assert size > min_offset
  offset = random.randint(min_offset, size-1)
  fileout = get_conf().btrfs.send_file_staging + '/' + uuid.uuid4().hex
  assert not os.path.exists(fileout)
  shutil.copyfile(filein, fileout)
  with open(fileout, 'r+b') as fileobj:
    fileobj.seek(offset, os.SEEK_SET) 
    data = fileobj.read(1)[0]
    new_data = bytes([ (data+1) % 256 ])
    fileobj.seek(offset, os.SEEK_SET) 
    fileobj.write( new_data )
    logger.debug("Changing %r=>%r @ %d", data, new_data, offset)
  return fileout

