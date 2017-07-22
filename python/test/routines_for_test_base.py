import uuid, shutil
from common import *
from file_utils import *

#################################################################################
# File routines

def add_rand_file_to_all_targets(targets):
  for target in targets:
    add_rand_file_to_dir(target.path)

def give_stage_filepath(path):
  with tempfile.NamedTemporaryFile(mode='w', dir=path, delete=True) as fileobj:
    return fileobj.name

def add_rand_data_to_fileobj_and_rewind(fileobj, size_kb=None):
  assert not size_kb or size_kb < 256 * 1024, "%d bytes is a bit too much" % (size_kb * 1024)
  if not size_kb: # random length
    for i in range(1024):
      fileobj.write("%d\n" % random.randint(0,1024*1024*1024))
  else:
    line = (str(random.randrange(100000000000000, 999999999999999)) + "\n") * 64
    for i in range(size_kb):
      fileobj.write(line.encode('utf8'))
  # rewind so that it can be read from the start
  fileobj.seek(0, os.SEEK_SET)

def get_rand_data(size_kb=None):
  with tempfile.TemporaryFile(mode='r+b') as fileobj:
    add_rand_data_to_fileobj_and_rewind(fileobj, size_kb)
    return fileobj.read()

def add_rand_file_to_dir(path, size_kb=None):
  with tempfile.NamedTemporaryFile(mode='wb', dir=path, delete=False) as fileobj:
    logger.debug("Writing %s", fileobj.name)
    os.chmod(fileobj.name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
    if size_kb != None and size_kb > 0:
      add_rand_data_to_fileobj_and_rewind(fileobj, size_kb) 
  return fileobj.name

def add_rand_file_to_staging(size_kb=None):
  filepath = add_rand_file_to_dir( get_conf().app.staging_dir, size_kb )
  fileseg = Fileseg.build_from_fileout(filepath)
  fileseg.range_bytes = (0, size_kb*1024)
  return fileseg

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

def modify_random_byte_in_file (filein, min_offset=0, max_offset=0):
  size = os.path.getsize(filein)
  if not max_offset:
    max_offset = size-1
  assert min_offset < max_offset
  offset = random.randint(min_offset, max_offset)

  fileout = get_conf().app.staging_dir + '/' + uuid.uuid4().hex
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

