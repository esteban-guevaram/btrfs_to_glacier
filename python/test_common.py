from common import *
logger = logging.getLogger(__name__)

def clean_tx_log():
  if os.path.isfile(get_conf().app.transaction_log):
    os.remove(get_conf().app.transaction_log)

def setup_filesystem(extra_options, subvol_paths):
  script_path = os.path.dirname(os.path.realpath(__file__)) + '/' + get_conf().test.btrfs_setup_script
  subvol_names = [ os.path.basename(n) for n in subvol_paths ]

  setup_cmd = [ script_path, '-d', get_conf().test.btrfs_device ]
  setup_cmd.extend(extra_options)
  setup_cmd.extend(subvol_names)

  logger.info("Reset filesystem : %r", setup_cmd)
  sp.check_call( setup_cmd )

