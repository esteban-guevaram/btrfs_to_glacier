from common import *
from transaction_log import reset_txlog
logger = logging.getLogger(__name__)

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

def add_rand_file_to_all_targets(targets):
  for target in targets:
    add_rand_file_to_dir(target.path)

def add_rand_file_to_dir(path):
  with tempfile.NamedTemporaryFile(mode='w', dir=path, delete=False) as fileobj:
    logger.debug("Writing %s", fileobj.name)
    os.chmod(fileobj.name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
    for i in xrange(1024):
      fileobj.write("%d\n" % random.randint(0,1024*1024*1024))

def setup_filesystem(extra_options, subvol_paths):
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

  with open(left, 'r') as left_file:
    with open(right, 'r') as right_file:
      left_text = left_file.read()
      right_text = right_file.read()
      return cmp(left_text, right_text)

