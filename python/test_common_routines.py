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

### END TestCommon

if __name__ == "__main__":
  conf_for_test()
  ut.main()

