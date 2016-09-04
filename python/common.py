from config import get_conf, conf_for_test, reset_conf
import logging, config_log, random
import os, re, stat, datetime, tempfile, binascii, base64
import subprocess as sp
logger = logging.getLogger(__name__)

class timestamp (object):
  now = datetime.datetime.now()
  str = now.strftime('%Y%m%d%H%M')

def annoying_confirm_prompt(cmd):
  for attempt in range(3):
    logger.warning('\nAre you sure you want to run : %r', cmd)
    ok_token = random.randint(100, 1000)
    answer = raw_input('[%d]  Type %d to confirm >> ' % (attempt, ok_token))
    if int(answer) != ok_token:
      raise Exception('Aborted by user')

def sync_simple_call (cmd):
  proc = sp.Popen(cmd, stdin=None, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines=True)
  (out, err) = proc.communicate()

  logger.debug('Output :\n%s', out)  
  if len(err):
    logger.warning('Error :\n%s', err)
  if proc.returncode:
    raise Exception('%r failed with error %d' % (cmd, proc.returncode))
  return out  

def call (cmd, interactive=None):
  dryrun = get_conf().app.dryrun
  if type(cmd) == str: 
    cmd = cmd.split()

  logger.debug("Running (dryrun=%r):\n %r", dryrun, cmd)
  if dryrun: return ''

  if interactive == None:
    interactive = get_conf().app.interactive
  if interactive:
    annoying_confirm_prompt(cmd)
  return sync_simple_call(cmd)

def rsync(source, dest):
  rsync_exc = get_conf().rsync.exclude
  cmd = 'rsync --archive --safe-links --itemize-changes --one-file-system --delete'.split()
  cmd.extend( '--exclude=' + pat for pat in rsync_exc )
  cmd.extend([ source, dest ])
  assert os.path.isdir(source) and os.path.isdir(dest), "Bad arguments (%s, %s)" % (source, dest)
  call(cmd)

def retry_operation (closure, exception):
  for attempt in range(2):
    try:
      return closure()
    except exception as err:
      logger.warn("Attempt %d, operation failed : %r", attempt, err)
  raise Exception('Permanent failure')

def build_mime_range (range_bytes):
  return '%d-%d/*' % (range_bytes[0], range_bytes[1]-1)

_mime_rex = re.compile(r'(\d+)-(\d+)/\*')
def build_range_from_mime (mimestr):
  match = _mime_rex.match(mimestr)
  assert match, "Invalid mime string '%s'" % mimestr
  return (int(match.group(1)), int(match.group(2)) + 1)

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
  return binascii.hexify(hexstr)

def convert_bytes_to_hexstr (byte_array):
  return binascii.unhexify(byte_array)

def calculate_md5_base64_encoded (byte_array):
  hash_bytes = hashlib.md5(byte_array).digest()
  return base64.b64encode(hash_bytes).decode()

def read_fileseg (fileseg, chunk_range=None):
  if not chunk_range:
    chunk_range = fileseg.range_bytes

  with open(fileseg.fileout, 'rb') as fileobj:
    fileobj.seek( chunk_range[0], os.SEEK_SET )
    byte_array = fileobj.read(chunk_range[1] - chunk_range[0])
    assert len(byte_array) == chunk_range[1] - chunk_range[0]
  return byte_array

