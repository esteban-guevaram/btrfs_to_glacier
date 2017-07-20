import unittest as ut
from common import *
from routines_for_test import *

class TestCommon (ut.TestCase):

  #@ut.skip("For quick validation")
  def test_call_ext_program(self):
    out = call('echo call')
    self.assertEqual('call', out.strip())
    try:
      call('buggy')
      self.fail("Expecting command to fail")
    except: pass  
  
  #@ut.skip("For quick validation")
  def test_temp_files(self):
    with tempfile.TemporaryFile(mode='w+') as out_file:
      out_file.write('chocolat');
      out_file.seek(0, os.SEEK_SET)
      self.assertEqual('chocolat', out_file.read())

### END TestCommon

if __name__ == "__main__":
  conf_for_test()
  ut.main()

