import logging
logger = logging.getLogger(__name__)

class BtrfsNode (object):
  SNAP, SUBVOL = 1, 2

  def __init__(self, **kwargs):
    self.childs = []
    self.parent = None
    assert kwargs.get('uuid')
    for k,v in kwargs.items():
      setattr(self, k, v)

  def accept (self, visitor):
    logger.debug('Visiting : %r', self)
    visitor(self)

  def __repr__ (self):
    parent = None
    if self.parent: parent = self.parent.uuid
    return "(%s, %s, par=%s, childs=%d)" % (self.label, self.uuid, parent, len(self.childs))

class BtrfsRoot (BtrfsNode):
  def __repr__ (self):
    return "(%s, %s, childs=%d, %r)" % (self.label, self.uuid, len(self.childs), self.devices)

