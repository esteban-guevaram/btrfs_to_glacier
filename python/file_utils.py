from common import *
logger = logging.getLogger(__name__)

class FileUtils (object):

  @staticmethod
  def decrypt_decompress_cmd():
    if get_conf().app.encrypt:
      passphrase = " --passphrase-file " + get_conf().app.passfile
      return 'gpg --decrypt' + passphrase
    else:
      return 'gzip -dc'

  @staticmethod
  def encrypt_compress_cmd():
    if get_conf().app.encrypt:
      passphrase = " --passphrase-file " + get_conf().app.passfile
      return 'gpg --compress-algo zlib --cipher-algo CAMELLIA256 --symmetric' + passphrase
    else:
      return 'gzip -c'

  @staticmethod
  def encrypt_from_cmd_stdout(btrfs_cmd, fileout):
    hasher = hashlib.sha256()
    dump_cmd = FileUtils.encrypt_compress_cmd()

    with ProcessGuard(btrfs_cmd, None, sp.PIPE, None) as btrfs_proc:
      with open(fileout, 'wb') as result_file:
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
  def decrypt_file_into_cmd_stdin(btrfs_cmd, fileout):
    hasher = hashlib.sha256()
    read_cmd = FileUtils.decrypt_decompress_cmd()

    with open(fileout, 'rb') as send_file:
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

    hashstr = hasher.digest()  
    return hashstr

  @staticmethod
  def compress_crypt_file(source):
    back_logfile = '%s/backup_%s_%s' % (get_conf().app.staging_dir, os.path.basename(source), timestamp.str)
    dump_cmd = FileUtils.encrypt_compress_cmd()

    with open(source, 'rb') as logfile_obj:
      with open(back_logfile, 'wb') as result_file:
        with ProcessGuard(dump_cmd, logfile_obj, result_file, None) as dump_proc:
          dump_proc.wait()

    logger.info("Crypted file %s at %s", source, back_logfile)
    return back_logfile    

  @staticmethod
  def decompress_decrypt_file(source, dest):
    read_cmd = FileUtils.decrypt_decompress_cmd()

    with open(source, 'rb') as logfile_obj:
      with open(dest, 'wb') as result_file:
        with ProcessGuard(read_cmd, logfile_obj, result_file, None) as read_proc:
          read_proc.wait()

    logger.info("Restored %s from %s", dest, source)
    return dest

### END FileUtils

class TreeHasher:
  PART_LEN = 1024**2

  def __init__ (self):
    self.chunk_hashes = []

  def update_chunk (self, byte_array):
    hash_leaves = []
    for range_bytes in range_bytes_it( (0, len(byte_array)), TreeHasher.PART_LEN):
      hasher = hashlib.sha256( byte_array[range_bytes[0] : range_bytes[1]] )
      hash_leaves.append( hasher.digest() )

    hash_bytes = TreeHasher.calculate_treehash_from_leaves(hash_leaves)  
    self.chunk_hashes.append(hash_bytes)
    return hash_bytes

  def add_chunk_hash_as_hexstr (self, hexstr):
    byte_array = convert_hexstr_to_bytes(hexstr)
    assert len(byte_array) == 32
    self.chunk_hashes.append( byte_array )

  def digest_chunk_as_hexstr (self):
    result = self.chunk_hashes[-1]
    return convert_bytes_to_hexstr(result)

  def digest_all_parts_as_hexstr (self):
    assert self.chunk_hashes
    result = TreeHasher.calculate_treehash_from_leaves(self.chunk_hashes)
    return convert_bytes_to_hexstr(result)

  def digest_single_shot_as_hexstr (self, byte_array):
    assert not self.chunk_hashes
    result = self.update_chunk(byte_array)
    return convert_bytes_to_hexstr(result)

  @staticmethod
  def calculate_treehash_from_leaves (hash_leaves):
    if len(hash_leaves) == 1:
      return hash_leaves[0]
    
    next_hash_level = []
    for i in range(0, len(hash_leaves), 2):
      if i + 1 < len(hash_leaves):
        hasher = hashlib.sha256( hash_leaves[i] + hash_leaves[i+1] )
        next_hash_level.append( hasher.digest() )
      else:
        next_hash_level.append( hash_leaves[i] )
    return TreeHasher.calculate_treehash_from_leaves(next_hash_level)    

### END TreeHasher

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

