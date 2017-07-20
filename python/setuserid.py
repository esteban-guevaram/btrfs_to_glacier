# this should be the first thing sourced so that we do not run anything as root if needed
import os, sys, contextlib, pwd

def set_user_environ(uid):
  record = pwd.getpwuid(uid)
  os.environ['HOME'] = record.pw_dir
  os.environ['USER'] = record.pw_name
  os.environ['USERNAME'] = record.pw_name
  os.environ['LOGNAME'] = record.pw_name

def get_saved_user():
  user = os.getresuid()[2]
  group = os.getresgid()[2]
  #print ('get_saved_user', user, group)
  return (user, group)

def get_effective_user():
  user = os.getuid()
  group = os.getgid()
  #print ('get_effective_user', user, group)
  return (user, group)

def get_unpriviledged_user():
  guid = list(get_effective_user())
  if 'SUDO_UID' in os.environ:
    guid[0] = int(os.environ['SUDO_UID'])
  if 'SUDO_GID' in os.environ:
    guid[1] = int(os.environ['SUDO_GID'])
  #print ('get_unpriviledged_user', guid)
  return tuple(guid)

def push_unpriviledged_user():
  eff_guid = get_effective_user()
  unp_guid = get_unpriviledged_user()
  if eff_guid != unp_guid:
    # order matters https://stackoverflow.com/questions/4692720/operation-not-permitted-while-dropping-privileges-using-setuid-function
    os.setresgid(unp_guid[1], unp_guid[1], -1)
    os.setresuid(unp_guid[0], unp_guid[0], -1)
    set_user_environ(unp_guid[0])
  
def pop_unpriviledged_user():
  eff_guid = get_effective_user()
  sav_guid = get_saved_user()
  if eff_guid != sav_guid:
    os.setresuid(sav_guid[0], sav_guid[0], -1)
    os.setresgid(sav_guid[1], sav_guid[1], -1)
    set_user_environ(sav_guid[0])
  #print ('pop_unpriviledged_user', get_effective_user())
    
def set_unpriviledged_user():
  unp_guid = get_unpriviledged_user()
  os.setresgid(unp_guid[1], unp_guid[1], -1)
  os.setresuid(unp_guid[0], unp_guid[0], -1)
  set_user_environ(unp_guid[0])

def assert_unpriviledged_user():
  assert get_effective_user() == get_unpriviledged_user()

@contextlib.contextmanager
def PriviledgeGuard():
  pop_unpriviledged_user()
  try: yield
  finally: push_unpriviledged_user()

set_unpriviledged_user()

