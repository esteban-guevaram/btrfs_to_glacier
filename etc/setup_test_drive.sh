#! /bin/bash
# Creates a dummy btrfs filesystem to test the project

MOUNT_POINT='/media/PybtrfsTestDrive'
RESTORE_POINT="$MOUNT_POINT/restore"
SNAP_POINT="$MOUNT_POINT/snapshots"

DEVICE=""
PARTITION=""
LABEL='PybtrfsTestDrive'

SUBVOL_LIST=( )
CREATE_SNAP_FLAG=0
CREATE_PART_FLAG=0

parse_opts() {
  while getopts 'd:sp' opt; do
    case $opt in
      d) DEVICE="$OPTARG" ;;
      s) CREATE_SNAP_FLAG=1 ;;
      p) CREATE_PART_FLAG=1 ;;
      *)
        echo "[USAGE] $0 -d DEVICE [-s][-p] [SUBVOL_LIST]"
        exit 5
      ;;
    esac
  done
  shift $((OPTIND-1))
  PARTITION="${DEVICE}1"
  SUBVOL_LIST=( "$@" )
}

try_or_die() {
  test "$@"
  if [[ $? != 0 ]]; then
    echo "[ERROR] test $@ => FAILED"
    exit 1
  fi
}

run_cmd() {
  local ignore_if_fail=0
  if [[ $1 == -i ]]; then
    ignore_if_fail=1
    shift
  fi
  echo "Running : $@"
  "$@"
  if [[ $? != 0 ]] && [[ $ignore_if_fail == 0 ]]; then
    echo "[ERROR] $@"
    exit 2
  fi
}

# We will only erase a partition if it does not exist or if it has a test label
avoid_shot_in_the_foot() {
	try_or_die ! -z "$DEVICE" -a -e "$DEVICE"
	part_info="`sudo blkid $PARTITION`"
	if [[ ! -z "$part_info" ]]; then
		if [[ "$part_info" == *"LABEL=\"$LABEL\""* ]]; then
			echo "[INFO] We will erase this : $part_info"
		else
			echo "[ERROR] Abort will NOT touch : $part_info"
			exit 4
		fi	
	fi
}

write_random_file() {
  local size=${2:-64}
  local filepath=`mktemp -u "${1}/file.XXXXXX"`

  run_cmd sudo chown -R `whoami`: "`dirname $filepath`"
  echo "Writing random data to $filepath"
  dd bs=1024 count=$size if=/dev/urandom | strings > "$filepath"

  if [[ $? != 0 ]]; then
    echo "[ERROR] Could not write $filepath"
    exit 3
  fi
}

prepare_partition() {
  [[ $CREATE_PART_FLAG == 1 ]] || return
  [[ -d "$MOUNT_POINT" ]] || run_cmd sudo mkdir -p "$MOUNT_POINT"

  run_cmd -i sudo umount "$MOUNT_POINT"
  run_cmd sudo parted $DEVICE print
  run_cmd sudo parted $DEVICE mktable msdos
  run_cmd sudo parted $DEVICE mkpart primary 0% 100%

  sync
  try_or_die -e $PARTITION
}

prepare_filesystem() {
  run_cmd -i sudo umount "$MOUNT_POINT"
  run_cmd sudo mkfs.btrfs -fL $LABEL "$PARTITION"
  run_cmd sudo mount -o user,noatime,nodiratime -L $LABEL "$MOUNT_POINT"
  run_cmd sudo chown -R `whoami`: "$MOUNT_POINT"

  [[ -d "$RESTORE_POINT" ]] || run_cmd mkdir -p "$RESTORE_POINT"
  [[ -d "$SNAP_POINT" ]] || run_cmd mkdir -p "$SNAP_POINT"
}

create_dummy_subvol_and_snap() {
  pushd "$MOUNT_POINT"
  for subvol_name in "${SUBVOL_LIST[@]}"; do
    try_or_die ! -z "$subvol_name" -a ! -e "$subvol_name"

    run_cmd sudo btrfs subvolume create "$subvol_name" 
    modify_subvol_and_snap $subvol_name
    modify_subvol_and_snap $subvol_name
  done
  popd
}

modify_subvol_and_snap() {
  pushd "$MOUNT_POINT"
  local subvol_name="$1"
  local snap_name=`mktemp -u "${SNAP_POINT}/${subvol_name}.XXXXXX"`
  local restore_name=`mktemp -u "${RESTORE_POINT}/${subvol_name}.XXXXXX"`

  write_random_file "$subvol_name"
  if [[ $CREATE_SNAP_FLAG == 1 ]]; then
    run_cmd sudo btrfs subvolume snapshot -r "$subvol_name" "$snap_name"
    run_cmd sudo btrfs send -f "$restore_name" "$snap_name"
  fi  
  popd
}

############################################### MAIN #########################################

parse_opts "$@"
avoid_shot_in_the_foot
prepare_partition
prepare_filesystem
create_dummy_subvol_and_snap
sync

