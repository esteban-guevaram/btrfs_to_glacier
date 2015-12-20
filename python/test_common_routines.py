import unittest as ut
from common import *

class TestCommon (ut.TestCase):

  def test_call(self):
    out = call('echo call')
    self.assertEquals('call', out.strip())
    try:
      call('buggy')
      self.fail("Expecting command to fail")
    except: pass  
  
  def test_temp_files(self):
    with tempfile.TemporaryFile() as out_file:
      out_file.write('chocolat');
      out_file.seek(0)
      self.assertEquals('chocolat', out_file.read())

  def test_async_call(self):
    with async_call('echo async_call') as guard:
      guard.proc.wait()
      guard.out_file.seek(0)
      out = guard.out_file.read()
      self.assertEquals('async_call', out.strip())
    
    try:
      with async_call('buggy') as guard:
        pass
      self.fail("Expecting command to fail")
    except: pass  

  def test_sudo_call(self):
    out = sudo_call('echo call', False)
    self.assertEquals('call', out.strip())
    try:
      sudo_call('buggy', False)
      self.fail("Expecting command to fail")
    except: pass  

  def test_sudo_async_call(self):
    with sudo_async_call('echo async_call', False) as guard:
      guard.proc.wait()
      guard.out_file.seek(0)
      out = guard.out_file.read()
      self.assertEquals('async_call', out.strip())
    
    try:
      with sudo_async_call('buggy', False) as guard:
        pass
      self.fail("Expecting command to fail")
    except: pass  

### END TestCommon

if __name__ == "__main__":
  ut.main()

