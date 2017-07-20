import unittest as ut
from common import *
from routines_for_test import *

class TestSetUserId (ut.TestCase):

  #@ut.skip("For quick validation")
  def test_priviledge_guard (self):
    with setuserid.PriviledgeGuard():
      user = os.getuid()
      self.assertEqual( user, 0 )
    user = os.getuid()
    self.assertFalse( user == 0 )

    try:
      with setuserid.PriviledgeGuard():
        raise Exception()
    except: pass
    user = os.getuid()
    self.assertFalse( user == 0 )

  #@ut.skip("For quick validation")
  def test_initial_priviledge_state (self):
    user = os.getuid()
    root = os.getresuid()[2]
    self.assertFalse( root == user )
    self.assertEqual( root, 0 )

  #@ut.skip("For quick validation")
  def test_push_pop_twice_unpriviled_user (self):
    setuserid.pop_unpriviledged_user()
    user = os.getuid()
    self.assertEqual( 0, user )

    setuserid.pop_unpriviledged_user()
    user = os.getuid()
    self.assertEqual( 0, user )

    setuserid.push_unpriviledged_user()
    user = os.getuid()
    self.assertFalse( user == 0 )

    setuserid.push_unpriviledged_user()
    user = os.getuid()
    self.assertFalse( user == 0 )

### END TestSetUserId

if __name__ == "__main__":
  conf_for_test()
  ut.main()

