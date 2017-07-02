import unittest as ut
from common import *
from routines_for_test import *

class TestTreeHasher (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  def setUp(self):
    logger.info("*** Running : %r", self.id())

  #@ut.skip("For quick validation")
  def test_odd_number_of_chunks (self):
    one_meg_data = b'mrmonkey' * (1024*128)

    hasher = TreeHasher()
    result = hasher.update_chunk( one_meg_data )
    self.assertEqual(b'43cadff4f1e8cd7e25c13b7dea589588d76c4e4b778c643cad65b305ebe8a801', binascii.hexlify(result))
    self.assertEqual(b'43cadff4f1e8cd7e25c13b7dea589588d76c4e4b778c643cad65b305ebe8a801', hasher.digest_chunk_as_hexstr())

    hasher = TreeHasher()
    result = hasher.digest_single_shot_as_hexstr( one_meg_data )
    self.assertEqual(b'43cadff4f1e8cd7e25c13b7dea589588d76c4e4b778c643cad65b305ebe8a801', result)

    hasher = TreeHasher()
    for i in range(5):
      hasher.update_chunk( one_meg_data )
    result = hasher.digest_all_parts_as_hexstr()
    self.assertEqual(b'5f3f64924317b10b2ddb99299c9efc54d665d3ad94756d08ed6d2f85f5c7c565', result)

    hasher = TreeHasher()
    result = hasher.digest_single_shot_as_hexstr( one_meg_data * 5 )
    self.assertEqual(b'5f3f64924317b10b2ddb99299c9efc54d665d3ad94756d08ed6d2f85f5c7c565', result)

  #@ut.skip("For quick validation")
  def test_even_number_of_chunks (self):
    one_meg_data = b'mrmonkey' * (1024*128)

    hasher = TreeHasher()
    for i in range(4):
      hasher.update_chunk( one_meg_data )
    result = hasher.digest_all_parts_as_hexstr()
    self.assertEqual(b'c2192941a2d88e71d6c43ce12fec25005ed8d3ab187023f340a12d87d3a47171', result)

    hasher = TreeHasher()
    for i in range(2):
      hasher.update_chunk( one_meg_data * 2 )
    result = hasher.digest_all_parts_as_hexstr()
    self.assertEqual(b'c2192941a2d88e71d6c43ce12fec25005ed8d3ab187023f340a12d87d3a47171', result)

    hasher = TreeHasher()
    result = hasher.digest_single_shot_as_hexstr( one_meg_data * 4 )
    self.assertEqual(b'c2192941a2d88e71d6c43ce12fec25005ed8d3ab187023f340a12d87d3a47171', result)

  #@ut.skip("For quick validation")
  def test_not_multiple_of_meg (self):
    one_meg_data = b'mrmonkey' * (1024*128)
    one_and_something_meg_data = b'mrmonkey' * (1024*171)
    less_than_meg_data = b'mrmonkey' * (1024*21)

    hasher = TreeHasher()
    hasher.update_chunk( one_and_something_meg_data )
    self.assertEqual(b'4457113e30c619a969c48a7f5456f3b1d076e5f41dae02a453a87b67473eb93a', hasher.digest_chunk_as_hexstr())

    hasher = TreeHasher()
    for i in range(4):
      hasher.update_chunk( one_meg_data )
    hasher.update_chunk( less_than_meg_data )
    result = hasher.digest_all_parts_as_hexstr()
    self.assertEqual(b'6d85431fdba614967e3bfae5569e301737f5fac1ab1dc1da8e01e6613e53a8ef', result)

    hasher = TreeHasher()
    result = hasher.digest_single_shot_as_hexstr( one_meg_data * 4 + less_than_meg_data )
    self.assertEqual(b'6d85431fdba614967e3bfae5569e301737f5fac1ab1dc1da8e01e6613e53a8ef', result)

### END TestTreeHasher

if __name__ == "__main__":
  conf_for_test()
  ut.main()

