from config import get_conf
import logging, config_log, os, re, getpass, datetime, subprocess as sp
logger = logging.getLogger(__name__)

class timestamp (object):
  now = datetime.datetime.now()
  str = now.strftime('%d%m%Y')

def call (cmd, stdin=None):
  if type(cmd) == str: cmd = cmd.split()
  dryrun = get_conf().app.dryrun
  logger.info("Running (dryrun=%r):\n %r", dryrun, cmd)
  if dryrun: return ''

  proc = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)
  (out, err) = proc.communicate(stdin)

  logger.debug('Output :\n%s', out)  
  if len(err):
    logger.warn('Error :\n%s', err)
  if proc.returncode:
    raise Exception('%r failed with error %d' % (cmd, proc.returncode))
  return out  

def __sudo__ (dummy):
  ctx = {}
  def closure (cmd):
    if 'pwd' not in ctx: 
      ctx['pwd'] = getpass.getpass()

    if type(cmd) == str: cmd = cmd.split()
    real_cmd = ['sudo', '-kSp', ''] + cmd
    return call(real_cmd, ctx['pwd'] + '\n')
  return closure  

@__sudo__
def sudo (cmd): pass

def rsync(source, dest):
  rsync_exc = get_conf().rsync.exclude
  cmd = 'rsync --archive --safe-links --itemize-changes --one-file-system --delete'.split()
  cmd.extend( '--exclude=' + pat for pat in rsync_exc )
  cmd.extend([ source, dest ])
  assert os.path.isdir(source) and os.path.isdir(dest), "Bad arguments (%s, %s)" % (source, dest)
  call(cmd)

