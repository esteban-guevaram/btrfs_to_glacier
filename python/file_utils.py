from common import *
logger = logging.getLogger(__name__)

class FileUtils (object):

  @staticmethod
  def decrypt_decompress_cmd():
    if get_conf().app.encrypt:
      assert os.path.isfile(get_conf().app.passfile)
      passphrase = " --passphrase-file " + get_conf().app.passfile
      return 'gpg --decrypt' + passphrase
    else:
      return 'gzip -dc'

  @staticmethod
  def encrypt_compress_cmd():
    if get_conf().app.encrypt:
      assert os.path.isfile(get_conf().app.passfile)
      passphrase = " --passphrase-file " + get_conf().app.passfile
      return 'gpg --compress-algo zlib --cipher-algo CAMELLIA256 --symmetric' + passphrase
    else:
      return 'gzip -c'

  @staticmethod
  def encrypt_from_cmd_stdout(btrfs_cmd, fileout):
    hasher = hashlib.sha256()
    dump_cmd = FileUtils.encrypt_compress_cmd()

    with ProcessGuard(btrfs_cmd, None, sp.PIPE, None, as_root=True) as btrfs_proc:
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

    assert btrfs_proc.returncode == 0 and dump_proc.returncode == 0, "%r" % {
      "btrfs_proc.returncode" : btrfs_proc.returncode,
      "dump_proc.returncode" : dump_proc.returncode,
      "dump_cmd" : dump_cmd,
      "btrfs_cmd" : btrfs_cmd,
    }

    hashstr = hasher.digest()  
    assert os.path.exists(fileout)
    return hashstr

  @staticmethod
  def decrypt_file_into_cmd_stdin(btrfs_cmd, fileout):
    hasher = hashlib.sha256()
    read_cmd = FileUtils.decrypt_decompress_cmd()

    with open(fileout, 'rb') as send_file:
      with ProcessGuard(read_cmd, send_file, sp.PIPE, None) as read_proc:
        with ProcessGuard(btrfs_cmd, sp.PIPE, None, None, as_root=True) as btrfs_proc:
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

  @staticmethod
  def clean_staging_dir ():
    interesting_records = (Record.SNAP_TO_FILE, Record.FILESEG_START)
    staging_dir = get_conf().app.staging_dir
    files_to_remove = set()

    for record in get_txlog().iterate_through_records():
      if record.r_type in interesting_records:
        filepath = os.path.join(staging_dir, record.fileout)
        files_to_remove.add(filepath)

    logger.info("Cleaning staging dir %s, to be removed : %r", staging_dir, files_to_remove)
    for filepath in files_to_remove:
      if os.path.isfile(filepath):
        os.remove(filepath)
    return files_to_remove    

### END FileUtils

class Fileseg:
  __slots__ = ['aws_id', 'archive_id', 'fileout', 'range_bytes', 'chunks', 'done']

  @staticmethod
  def build_from_record (fileout, record):
    fileout = os.path.join( get_conf().app.staging_dir, record.fileout )
    aws_id, archive_id = None, None
    if hasattr(record, 'aws_id'):     awd_id = record.awd_is
    if hasattr(record, 'archive_id'): archive_id = record.archive_id
    fileseg = Fileseg(fileout, aws_id, archive_id, record.range_bytes)
    return fileseg

  @staticmethod
  def build_from_fileout (fileout, range_bytes=None):
    fileseg = Fileseg(fileout, None, None, range_bytes)
    return fileseg

  @staticmethod
  def calculate_range_substraction (adjacent_filesegs, containing_fs):
    merged_range = merge_range(adjacent_filesegs)
    range_bytes = range_substraction(containing_fs.range_bytes, merged_range)
    if not range_bytes:
      return None

    result = copy.copy(containing_fs)
    result.range_bytes = range_bytes
    return result

  @staticmethod
  def build_from_txlog_upload (record):
    fileout = os.path.join( get_conf().app.staging_dir, record.fileout )
    fileseg = Fileseg(fileout, None, record.archive_id, None)
    fileseg.done = True
    return fileseg

  def __init__ (self, fileout, aws_id, archive_id, range_bytes):
    self.fileout = fileout
    self.aws_id = aws_id
    self.archive_id = archive_id
    self.done = False
    self.chunks = []

    if not range_bytes and os.path.exists(fileout):
      size = os.stat(fileout).st_size
      self.range_bytes = (0, size)
    else:
      self.range_bytes = range_bytes
    assert self.fileout and self.range_bytes
    assert os.path.dirname(fileout) == get_conf().app.staging_dir, '%r, %r' % (os.path.dirname(fileout), get_conf().app.staging_dir)
  
  def calculate_remaining_range (self):
    if not self.chunks:
      return self.range_bytes
    range_bytes = (self.chunks[-1].range_bytes[1], self.range_bytes[1])
    assert range_bytes[1] - range_bytes[0] > 0
    return range_bytes

  def clean_pending_chunk (self):
    self.done = False
    if self.chunks and not self.chunks[1].done:
      self.chunks.pop()

  def clean_chunks (self):
    self.chunks = []
    self.done = False

  def assert_in_valid_pending_state (self):
    assert not self.done
    if self.chunks and self.chunks[1].done:
      assert self.range_bytes[1] > self.chunks[1].range_bytes[1]

  def set_done (self, archive_id=None):
    if archive_id:
      self.archive_id = archive_id
    assert self.fileout and self.archive_id
    self.done = True

  def add_chunk (self, chunk):
    assert not self.chunks or self.chunks[-1][1] == chunk[0], 'Uncontinous chunk added to fileseg'
    self.chunks.append(chunk)

  def get_aws_arch_description(self):
    return str(self.key())

  def len (self):
    return self.range_bytes[1] - self.range_bytes[0]

  def key (self):
    return (os.path.basename(self.fileout), self.range_bytes)

  def __repr__ (self):
    return "(aws_id=%r, fileout=%r, range=%r, done=%r)" % (self.aws_id, self.fileout, self.range_bytes, self.done)

## END Fileseg

class Chunk:
  __slots__ = ['range_bytes', 'done']
  def __init__ (self, range_bytes):
    assert not range_bytes or range_bytes[0] < range_bytes[1]
    self.range_bytes = range_bytes
    self.done = False

  def __repr__ (self):
    return "(range=%r, done=%r)" % (self.range_bytes, self.done)

## END Chunk


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

