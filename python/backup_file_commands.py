import hashlib
from common import *
from transaction_log import get_txlog, Record
logger = logging.getLogger(__name__)

class BackupFileCommands (object):

  @staticmethod
  def decrypt_decompress_cmd():
    if get_conf().app.encrypt:
      return 'gpg --decrypt'
    else:
      return 'gzip -dc'

  @staticmethod
  def encrypt_compress_cmd():
    if get_conf().app.encrypt:
      return 'gpg --compress-algo zlib --cipher-algo CAMELLIA256 --symmetric'
    else:
      return 'gzip -c'

  @staticmethod
  def write_send_file(btrfs_cmd, fileout):
    hasher = hashlib.sha256()
    dump_cmd = BackupFileCommands.encrypt_compress_cmd()

    with ProcessGuard(btrfs_cmd, None, sp.PIPE, None) as btrfs_proc:
      with open(fileout, 'w') as result_file:
        with ProcessGuard(dump_cmd, sp.PIPE, result_file, None) as dump_proc:
          while True:
            data = btrfs_proc.stdout.read()
            if not data and btrfs_proc.poll() != None: 
              data = btrfs_proc.stdout.read()
              if not data: break
            if data:
              dump_proc.stdin.write(data)
              hasher.update(data)

          dump_proc.stdin.close()
          dump_proc.wait()
          assert btrfs_proc.returncode == 0 and dump_proc.returncode == 0

    hashstr = hasher.digest()  
    assert os.path.exists(fileout)
    return hashstr

  @staticmethod
  def receive_subvol_file(btrfs_cmd, fileout, hashstr):
    hasher = hashlib.sha256()
    read_cmd = BackupFileCommands.decrypt_decompress_cmd()

    with open(fileout, 'r') as send_file:
      with ProcessGuard(read_cmd, send_file, sp.PIPE, None) as read_proc:
        with ProcessGuard(btrfs_cmd, sp.PIPE, None, None) as btrfs_proc:
          while True:
            data = read_proc.stdout.read()
            if not data and read_proc.poll() != None: 
              data = read_proc.stdout.read()
              if not data: break
            if data:
              btrfs_proc.stdin.write(data)
              hasher.update(data)

          btrfs_proc.stdin.close()
          btrfs_proc.wait()
          assert btrfs_proc.returncode == 0 and read_proc.returncode == 0
    assert hashstr == hasher.digest()
    return fileout

  @staticmethod
  def write_tx_log():
    hasher = hashlib.sha256()
    logfile = get_txlog().logfile
    back_logfile = '%s/backup_%s_%s' % (get_conf().btrfs.backup_subvol, os.path.basename(logfile), timestamp.str)
    dump_cmd = BackupFileCommands.encrypt_compress_cmd()

    with open(logfile, 'r') as logfile_obj:
      with open(back_logfile, 'w') as result_file:
        with ProcessGuard(dump_cmd, sp.PIPE, result_file, None) as dump_proc:
          while True:
            data = logfile_obj.read(1024*1024)
            if not data: break
            hasher.update(data)
            dump_proc.stdin.write(data)
          dump_proc.stdin.close()
          dump_proc.wait()

    hashstr = hasher.digest()  
    get_txlog().record_backup_tx_log(back_logfile, hashstr)
    logger.info("Wrote tx log at %s", back_logfile)
    return back_logfile    

  @staticmethod
  def fetch_tx_log(txlog_name):
    if txlog_name:
      result_name = '%s/backup_%s.restore' % (get_conf().btrfs.backup_subvol, os.path.basename(txlog_name))
      read_cmd = BackupFileCommands.decrypt_decompress_cmd()

      with open(txlog_name, 'r') as logfile_obj:
        with open(result_name, 'w') as result_file:
          with ProcessGuard(read_cmd, logfile_obj, result_file, None) as read_proc:
            read_proc.wait()

      logger.info("Restored %s from %s", result_name, txlog_name)
      txlog = TransactionLog(result_name)
    else:
      txlog = get_txlog()

    txlog.check_log_for_restore()
    return txlog

### END BackupFileCommands

class ProcessGuard:
  def __init__(self, cmd, stdin, stdout, stderr, interactive=None):
    self.cmd = cmd
    self.inf = stdin
    self.err = stderr
    self.out = stdout
    self.proc = None
    self.interactive = interactive
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

    self.proc = sp.Popen(self.cmd, stdin=self.inf, stdout=self.out, stderr=self.err)
    assert self.proc.pid
    logger.debug('Spawned process %d', self.proc.pid)
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

