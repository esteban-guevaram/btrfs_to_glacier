import logging
logger = logging.getLogger(__name__)

class BtrfsNode (object):
  ROOT, SNAP, SUBVOL = 0, 1, 2

  def __init__(self, **kwargs):
    self.childs = []
    self.label = self.get_parent = self.otime = None
    assert kwargs.get('uuid')
    for k,v in kwargs.items():
      setattr(self, k, v)

  def accept (self, visitor):
    logger.debug('Visiting : %r', self)
    visitor(self)

  def __repr__ (self):
    label = 'label?'
    if hasattr(self, 'label'): label = self.label
    mount = 'mount?'
    if hasattr(self, 'mount'): mount = self.mount
    parent = '??'
    if self.get_parent: parent = self.get_parent().label
    return "(%s, %s, %s, par=%s, childs=%d)" % (self.uuid, label, mount, parent, len(self.childs))

class BtrfsRoot (BtrfsNode):
  def __init__(self, **kwargs):
    super(BtrfsRoot, self).__init__(**kwargs)
    self.type = BtrfsNode.ROOT

  def __repr__ (self):
    return "(%s, %s, childs=%d, %r)" % (self.label, self.uuid, len(self.childs), self.devices)

