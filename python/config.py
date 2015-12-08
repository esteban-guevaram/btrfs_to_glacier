import logging, config_log, argparse, ConfigParser, os
logger = logging.getLogger(__name__)

class MyStoreTrue(argparse.Action):
  def __init__(self, option_strings, dest, nargs=None, **kwargs):
    super(MyStoreTrue, self).__init__(option_strings, dest, 0, **kwargs)
  def __call__(self, parser, namespace, values, option_string=None):
    if option_string: setattr(namespace, self.dest, values)

ARG_OPTION_MAPPING = {
  ('app', 'verbose')  :   { 'flag' : '-v', 'args' : { 'action' : MyStoreTrue } },
  ('app', 'dryrun')   :   { 'flag' : '-d', 'args' : { 'action' : MyStoreTrue } },
}

class FinalConf (object): 
  def __repr__ (self):
    lines = []
    for key,val in vars(self).items():
      if type(val) == FinalConf:
        lines.append( "### %s ###\n%r" % (key, val) )
      elif not key.endswith('_help'):
        lines.append( "  % -20s = %r" % (key, val) )
    return '\n'.join(lines)  

def sanity_checks (final_conf):
  assert os.path.isdir( final_conf.rsync.source )
  assert os.path.isdir( final_conf.rsync.dest )
  assert os.path.isdir( final_conf.btrfs.backup_subvol )
  assert os.path.exists( final_conf.btrfs.device )

def adjust_config_types (final_conf):
  transform_into_list(final_conf.rsync, 'exclude')
  transform_into_bool(final_conf.app, 'verbose')
  transform_into_bool(final_conf.app, 'dryrun')
  transform_into_bool(final_conf.app, 'interactive')

def transform_into_bool (section, prop):
  if not hasattr(section, prop):
    assert False, "Bad config for key %r - %s" % (section, prop)
  
  value = getattr(section, prop)
  if value in [True, False]: pass
  elif getattr(section, prop).lower() in ['1', 'true', 'yes', 'y']:
    setattr(section, prop, True)
  else:
    setattr(section, prop, False)

def transform_into_list (section, prop):
  if not hasattr(section, prop):
    setattr(section, prop, [])
  else:
    str_list = getattr(section, prop)
    obj_list = [ s.strip() for s in str_list.split(',') ]
    setattr(section, prop, obj_list)

def adjust_logger_config (final_conf):
  if final_conf.app.verbose:
    config_log.set_global_level(logging.DEBUG)
    logger.debug('Set logger to verbose mode')

def build_final_conf (config, namespace):
  final_conf = FinalConf()

  for section in config.sections():
    for prop,value in config.items(section):
      if not hasattr(final_conf, section):
        setattr(final_conf, section, FinalConf())
      value = config.get(section, prop)
      setattr( getattr(final_conf, section), prop, value)

  for key,info in ARG_OPTION_MAPPING.items():
    if not hasattr(final_conf, key[0]):
      setattr(final_conf, key[0], FinalConf())
    if hasattr(namespace, key[0]+key[1]):
      value = getattr(namespace, key[0]+key[1])  
      if value is not None:
        setattr( getattr(final_conf, key[0]), key[1], value)

  return final_conf    

def parse_command_line (config):
  description = config.get('app', 'help')
  parser = argparse.ArgumentParser(description=description)
  
  for key,info in ARG_OPTION_MAPPING.items():
    parser.add_argument(  info['flag'], '--' + key[1],
                          dest=key[0]+key[1],
                          help=config.get(key[0], key[1] + '_help'),
                          **info['args']
    )

  namespace = parser.parse_args()
  logger.debug('Command line args : %r', namespace)
  return namespace

def parse_config_file (filename):
  assert os.path.isfile(filename)
  config = ConfigParser.ConfigParser()
  config.read(filename)
  return config

def parse_all_config ():
  conf_file = 'config.properties'
  assert os.path.isfile(conf_file)

  config = parse_config_file(conf_file)
  namespace = parse_command_line(config)
  final_conf = build_final_conf(config, namespace)
  adjust_config_types(final_conf)
  adjust_logger_config(final_conf)
  sanity_checks(final_conf)
  logger.debug("Runtime config :\n%r", final_conf)

  return final_conf

singleton_conf = None
def get_conf ():
  global singleton_conf
  if not singleton_conf:
    singleton_conf = parse_all_config()
  return singleton_conf  

get_conf()

