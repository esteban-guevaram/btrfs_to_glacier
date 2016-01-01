from config import get_conf, conf_for_test
import logging, config_log, random, getpass
import os, re, stat, datetime, tempfile
import subprocess as sp
logger = logging.getLogger(__name__)

class timestamp (object):
  now = datetime.datetime.now()
  str = now.strftime('%Y%m%d%H%M')

def annoying_confirm_prompt(cmd):
  for attempt in range(3):
    logger.warn('\nAre you sure you want to run : %r', cmd)
    ok_token = random.randint(100, 1000)
    answer = raw_input('[%d]  Type %d to confirm >> ' % (attempt, ok_token))
    if int(answer) != ok_token:
      raise Exception('Aborted by user')

def __call_helper__ (executor, cmd, password, interactive):
  if type(cmd) == str: cmd = cmd.split()
  dryrun = get_conf().app.dryrun
  logger.debug("Running (dryrun=%r):\n %r", dryrun, cmd)
  if dryrun: return ''

  if interactive == None:
    interactive = get_conf().app.interactive
  if interactive:
    annoying_confirm_prompt(cmd)
  return executor(cmd, password)

def __call_helper_sudo__ (executor, cmd, interactive):
  pass_file = get_conf().app.pass_file
  if os.path.isfile(pass_file):
    with open(pass_file, 'r') as pfile:
      assert os.stat(pass_file).st_mode & (stat.S_IRWXO | stat.S_IRWXG) == 0
      pwd = pfile.readline().strip()
  else: 
    pwd = getpass.getpass()

  assert pwd
  if type(cmd) == str: cmd = cmd.split()
  real_cmd = ['sudo', '-kSp', ''] + cmd
  return __call_helper__(executor, real_cmd, pwd + '\n', interactive)

def sync_simple_call (cmd, password):
  proc = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)
  (out, err) = proc.communicate(password)

  logger.debug('Output :\n%s', out)  
  if len(err):
    logger.warn('Error :\n%s', err)
  if proc.returncode:
    raise Exception('%r failed with error %d' % (cmd, proc.returncode))
  return out  

def async_piped_call (cmd, password):
  err_file = tempfile.TemporaryFile(mode='rw')
  out_file = tempfile.TemporaryFile(mode='rw')
  proc = sp.Popen(cmd, stdin=sp.PIPE, stdout=out_file, stderr=err_file)
  assert proc.pid
  logger.debug('Spawned process %d', proc.pid)
  if password: proc.stdin.write(password)

  class Guard:
    def __init__(self, proc, out_file, err_file): 
      self.proc = proc
      self.err_file = err_file
      self.out_file = out_file
    def __enter__(self):
      return self
    def __exit__(self, exc_type, exc_val, exc_tb): 
      self.proc.wait()
      self.err_file.close()
      self.out_file.close()
  return Guard(proc, out_file, err_file)  

def sudo_call (cmd, interactive=None):
  return __call_helper_sudo__(sync_simple_call, cmd, interactive)

def sudo_async_call (cmd, interactive=None):
  return __call_helper_sudo__(async_piped_call, cmd, interactive)

def call (cmd, interactive=None):
  return __call_helper__(sync_simple_call, cmd, None, interactive)

def async_call (cmd, interactive=None):
  return __call_helper__(async_piped_call, cmd, None, interactive)

def sha256file(filename):
  assert os.path.isfile(filename)
  out = call(['sha256sum', filename])
  hashstr = out.split()[0];
  assert hashstr
  return hashstr

def rsync(source, dest):
  rsync_exc = get_conf().rsync.exclude
  cmd = 'rsync --archive --safe-links --itemize-changes --one-file-system --delete'.split()
  cmd.extend( '--exclude=' + pat for pat in rsync_exc )
  cmd.extend([ source, dest ])
  assert os.path.isdir(source) and os.path.isdir(dest), "Bad arguments (%s, %s)" % (source, dest)
  call(cmd)

