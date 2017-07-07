from config import get_conf, conf_for_test, reset_conf
import logging, config_log, random, setuserid
import sys, os, re, stat, datetime, copy, tempfile, binascii, base64, json, time, itertools, signal, hashlib
import subprocess as sp
logger = logging.getLogger(__name__)

class timestamp (object):
  now = datetime.datetime.now()
  str = now.strftime('%Y%m%d%H%M%S')

  def new (self):
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S')

class tx_handler (object):
  STACK_DEPTH = 0
  INT_COUNT = 0

  def __enter__ (self):
    if tx_handler.INT_COUNT:
      sys.exit(1)
    tx_handler.STACK_DEPTH += 1

  def __exit__ (self, exc_type, exc_val, exc_tb): 
    tx_handler.STACK_DEPTH -= 1
    if tx_handler.INT_COUNT:
      sys.exit(1)

  @staticmethod
  def interrupt_handler (signum, frame):
    if tx_handler.STACK_DEPTH:
      logger.warning('Signal taken into account, waiting for tx to finish')
      tx_handler.INT_COUNT += 1
    else:
      sys.exit(signum)

  @staticmethod
  def wrap (func):
    def __closure__ (*args, **kwargs):
      tx_handler.STACK_DEPTH += 1
      try: return func(*args, **kwargs)  
      except: raise
      finally: 
        tx_handler.STACK_DEPTH -= 1
        if tx_handler.INT_COUNT:
          sys.exit(1)
    return __closure__      

signal.signal(signal.SIGINT, tx_handler.interrupt_handler)
## END tx_handler

def retry_operation (closure, exception):
  for attempt in range(2):
    try:
      return closure()
    except exception as err:
      # exception may also be a tuple
      logger.warning("Attempt %d, operation failed : %r", attempt, err)
      time.sleep(0.5)
  raise Exception('Permanent failure')

def call (cmd, interactive=None, as_root=False):
  dryrun = get_conf().app.dryrun
  if type(cmd) == str: 
    cmd = cmd.split()

  logger.debug("Running (dryrun=%r):\n %r", dryrun, cmd)
  if dryrun: return ''

  if interactive == None:
    interactive = get_conf().app.interactive
  if interactive:
    annoying_confirm_prompt(cmd)
  return sync_simple_call(cmd, as_root)

def annoying_confirm_prompt(cmd):
  for attempt in range(3):
    logger.warning('\nAre you sure you want to run : %r', cmd)
    ok_token = random.randint(100, 1000)
    answer = raw_input('[%d]  Type %d to confirm >> ' % (attempt, ok_token))
    if int(answer) != ok_token:
      raise Exception('Aborted by user')

def sync_simple_call (cmd, as_root=False):
  with ProcessGuard(cmd, stdin=None, stdout=sp.PIPE, stderr=sp.PIPE, as_root=as_root, universal_newlines=True) as proc:
    (out, err) = proc.communicate()

  logger.debug('Output :\n%s', out)  
  if len(err):
    logger.warning('Error :\n%s', err)
  if proc.returncode:
    raise Exception('%r failed with error %d' % (cmd, proc.returncode))
  return out  

def build_arch_retrieve_range (range_bytes):
  return '%d-%d' % (range_bytes[0], range_bytes[1]-1)

# range formats are different for upload and download ...
def build_mime_range (range_bytes):
  return '%d-%d/*' % (range_bytes[0], range_bytes[1]-1)

_mime_rex = re.compile(r'^(\d+)-(\d+)(/\*)?$')
def build_range_from_mime (mimestr):
  match = _mime_rex.match(mimestr)
  assert match, "Invalid mime string '%s'" % mimestr
  return (int(match.group(1)), int(match.group(2)) + 1)

def len_range_from_mime (mimestr):
  match = _mime_rex.match(mimestr)
  assert match, "Invalid mime string '%s'" % mimestr
  return int(match.group(2)) - int(match.group(1)) + 1

def range_bytes_it (full_range, step):
  if full_range[1] <= full_range[0]:
    return []

  last = full_range[0]
  for end in range(last+step, full_range[1], step):
    yield (last, end)
    last = end
  
  assert full_range[1] - last <= step
  yield (last, full_range[1])

def convert_hexstr_to_bytes (hexstr):
  return binascii.unhexlify(hexstr)

def convert_bytes_to_hexstr (byte_array):
  return binascii.hexlify(byte_array)

def calculate_md5_base64_encoded (byte_array):
  hash_bytes = hashlib.md5(byte_array).digest()
  return base64.b64encode(hash_bytes).decode()

def truncate_and_write_fileseg (fileseg, body_bytes):
  with open(fileseg.fileout, 'wb') as fileobj:
    fileobj.write(body_bytes)
    assert fileobj.tell() == fileseg.range_bytes[1]

def write_fileseg (fileseg, chunk_range, data):
  assert chunk_range[1] - chunk_range[0] == len(data), "%r / %d" % (chunk_range, len(data))
  if not os.path.isfile(fileseg.fileout):
    with open(fileseg.fileout, 'wb'): pass

  with open(fileseg.fileout, 'rb+') as fileobj:
    fileobj.seek( chunk_range[0], os.SEEK_SET )
    fileobj.write(data)
    assert fileobj.tell() == chunk_range[1], "Chunk %r is not valid for %s" % (chunk_range, fileseg.fileout)

def read_fileseg (fileseg, chunk_range=None):
  if not chunk_range:
    chunk_range = fileseg.range_bytes

  with open(fileseg.fileout, 'rb') as fileobj:
    fileobj.seek( chunk_range[0], os.SEEK_SET )
    byte_array = fileobj.read(chunk_range[1] - chunk_range[0])
    assert len(byte_array) == chunk_range[1] - chunk_range[0]
  return byte_array

def parse_json_date (dct):
  for s in dct.keys():
    if s.endswith('Date'):
      dct[s] = dateutil.parser.parse(dct[s])
  return dct

def convert_json_bytes_to_dict (byte_array):
  return json.loads(byte_array.decode('utf-8'), object_hook=parse_json_date)
  
def wait_for_polling_period ():
  secs = get_conf().aws.polling_period_secs
  logger.debug('Going to sleep for %r secs', secs)
  time.sleep(secs)

def flatten_dict (key_to_list, *keys):
  if not keys:
    return itertools.chain( *key_to_list.keys() )
  lists = [ key_to_list[k] for k in keys ]
  return itertools.chain(*lists)
    
def sum_fs_size (filesegs):
  return sum( fs.len() for fs in filesegs )

def merge_ranges (filesegs):
  # we expect the ranges to form a continuous segment
  sorted_ranges = sorted( fs.range_bytes for fs in filesegs )
  return (sorted_ranges[0][0], sorted_ranges[-1][1])

def is_between(value, low, up):
  return value >= low and value < up

def range_contains(contained, container):
  return container[0] <= contained[0] and container[1] >= contained[1]

def range_substraction (source, to_remove):
  if source[0] >= to_remove[1] or source[1] <= to_remove[0]:
    return source # disjoint
  if source[0] < to_remove[0]:
    assert source[1] <= to_remove[1], 'source contains to_remove'
    return (source[0], to_remove[0])
  if source[0] < to_remove[1]:
    if source[1] <= to_remove[1]:
      return None
    return (to_remove[1], source[1])
  assert False  


class ProcessGuard:
  def __init__(self, cmd, stdin, stdout, stderr, as_root=None, interactive=None, universal_newlines=False):
    self.cmd = cmd
    self.inf = stdin
    self.err = stderr
    self.out = stdout
    self.proc = None
    self.interactive = interactive
    self.as_root = as_root
    self.uni_lines = universal_newlines
    if type(cmd) == str: 
      self.cmd = cmd.split()

  def __enter__(self):
    dryrun = get_conf().app.dryrun
    logger.debug("Running (dryrun=%r):\n %r", dryrun, self.cmd)
    if dryrun: return None

    if self.interactive == None:
      self.interactive = get_conf().app.interactive
    if self.interactive:
      annoying_confirm_prompt(self.cmd)

    if self.as_root:
      with setuserid.PriviledgeGuard():
        self.proc = sp.Popen(self.cmd, stdin=self.inf, stdout=self.out, stderr=self.err, universal_newlines=self.uni_lines)
    else:
      setuserid.assert_unpriviledged_user()
      self.proc = sp.Popen(self.cmd, stdin=self.inf, stdout=self.out, stderr=self.err, universal_newlines=self.uni_lines)

    assert self.proc.pid
    logger.debug('Spawned process %d = %r', self.proc.pid, self.cmd)
    return self.proc

  def __exit__(self, exc_type, exc_val, exc_tb): 
    killed = False
    if self.proc != None:
      if self.proc.poll() == None:
        self.proc.kill()
        self.proc.wait()
        killed = True

      if self.proc.stdin: self.proc.stdin.close()
      if self.proc.stdout: self.proc.stdout.close()
      if self.proc.stderr: self.proc.stderr.close()

    if killed:
      raise Exception("Killed process %r", self.cmd)

### ProcessGuard

