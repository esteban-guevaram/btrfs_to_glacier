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

def sync_simple_call (cmd):
  proc = sp.Popen(cmd, stdin=None, stdout=sp.PIPE, stderr=sp.PIPE)
  (out, err) = proc.communicate()

  logger.debug('Output :\n%s', out)  
  if len(err):
    logger.warn('Error :\n%s', err)
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

