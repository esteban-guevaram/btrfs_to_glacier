import logging.config, json, os
import setuserid

class ColorFilter (logging.Filter):
  txtrst = '\x1b[0m' # Black
  txtblk = '\x1b[0;30m' # Black
  txtred = '\x1b[0;31m' # Red
  txtgrn = '\x1b[0;32m' # Green
  txtylw = '\x1b[0;33m' # Yellow
  txtblu = '\x1b[0;34m' # Blue
  txtpur = '\x1b[0;35m' # Purple
  txtcyn = '\x1b[0;36m' # Cyan
  txtwht = '\x1b[0;37m' # White
  bldblk = '\x1b[1;30m' # Bold Black
  bldred = '\x1b[1;31m' # Bold Red
  bldgrn = '\x1b[1;32m' # Bold Green
  bldylw = '\x1b[1;33m' # Bold Yellow
  bldblu = '\x1b[1;34m' # Bold Blue
  bldpur = '\x1b[1;35m' # Bold Purple
  bldcyn = '\x1b[1;36m' # Bold Cyan
  bldwht = '\x1b[1;37m' # Bold White

  assert logging.CRITICAL < 100
  level_col = [txtrst] * (logging.CRITICAL+1)
  level_col[logging.CRITICAL] = bldpur
  level_col[logging.ERROR] = bldred
  level_col[logging.WARN] = txtylw
  level_col[logging.INFO] = txtgrn
  level_col[logging.DEBUG] = txtcyn

  @staticmethod
  def factory ():
    return ColorFilter()

  def filter (self, record):
    record.color = ColorFilter.level_col[record.levelno]
    record.reset = ColorFilter.txtrst
    return True

### ColorFilter

def set_global_level (level):
  logging.getLogger('').setLevel(level)

def find_conf_file ():
  conf_file = 'config_log.json'
  if not os.path.isfile(conf_file):
    conf_file = os.path.dirname(os.path.realpath(__file__)) + '/' + conf_file
  assert os.path.isfile(conf_file)
  return conf_file

def __init_logging__ ():
  setuserid.assert_unpriviledged_user()
  try:
    with open(find_conf_file(), 'r') as conf_file:
      conf_dict = json.load(conf_file)
    logging.config.dictConfig(conf_dict)
    logger = logging.getLogger(__name__)
    logger.debug('Logging config OK')
  except Exception as err:
    logging.basicConfig(level=logging.NOTSET)
    logger = logging.getLogger(__name__)
    logger.exception('Logging configuration failed. Using basicConfig')

__init_logging__()

