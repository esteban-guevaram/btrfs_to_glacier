#! /bin/bash
# Creates dummy btrfs filesystems to test the project

MOUNT_SRC="$TEMP/btrfs_test_part_src"
MOUNT_DST="$TEMP/btrfs_test_part_dst"
RESTORE_POINT="$MOUNT_DST/restore"
SNAP_POINT="$MOUNT_SRC/snapshots"

DISK_UUID=""
DISK_DEVICE=""
BASE_DEVICE=""
PART_PREFIX=""
PART_LBL_SRC=""
PART_LBL_DST=""

SUBVOL_LIST=( )
CREATE_DISK_FLAG=0
CREATE_PART_FLAG=0
CREATE_SNAP_FLAG=0
LEAVE_UMOUNTED=0
DRY_RUN=""

parse_opts() {
  while getopts 'd:l:fpsu' opt; do
    case $opt in
      d) DISK_UUID="$OPTARG" ;;
      l) PART_PREFIX="$OPTARG" ;;
      f) CREATE_DISK_FLAG=1 ;;
      p) CREATE_PART_FLAG=1 ;;
      s) CREATE_SNAP_FLAG=1 ;;
      u) LEAVE_UMOUNTED=1 ;;
      *)
        echo "[USAGE] $0 -d DISK_UUID -l PART_PREFIX [-f][-p][-s][-u] [SUBVOL_LIST]"
        exit 5
      ;;
    esac
  done
  shift $((OPTIND-1))
  DISK_DEVICE="`readlink -f "/dev/disk/by-uuid/$DISK_UUID"`"
  try_or_die -b "$DISK_DEVICE"
	BASE_DEVICE="`basename "$DISK_DEVICE"`"
  PART_LBL_SRC="${PART_PREFIX}_src"
  PART_LBL_DST="${PART_PREFIX}_dst"
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
  [[ -z "$DRY_RUN" ]] || return 0
  "$@"
  if [[ $? != 0 ]] && [[ $ignore_if_fail == 0 ]]; then
    echo "[ERROR] $@"
    exit 2
  fi
}

# We will only erase a partition if it does not exist or if it has a test label
avoid_shot_in_the_foot() {
	local is_removable=`cat "/sys/block/$BASE_DEVICE/removable" | tr -d '\n'`

	if [[ "$is_removable" != 1 ]]; then
		echo "[ERROR] '$DISK_DEVICE' is not removable"
		exit 7
	fi

  for label in "$PART_LBL_SRC" "$PART_LBL_DST"; do
    local part_path="`readlink -qf "/dev/disk/by-partlabel/$label"`"
    local part_dev="`basename "$part_path"`"
    [[ "$part_dev" == "$label" ]] && continue # the partition does not exist
    if [[ "$part_dev" != "$BASE_DEVICE"* ]]; then
      echo "[ERROR] '$label' is a partition not belonging to disk with '$BASE_DEVICE'"
      exit 4
    fi
  done
}

umount_fs() {
  for mntpt in "$MOUNT_SRC" "$MOUNT_DST"; do
    mountpoint -q "$mntpt" && run_cmd sudo umount "$mntpt"
  done
}

mount_fs() {
  for part_mnt in "${PART_LBL_SRC}:${MOUNT_SRC}" "${PART_LBL_DST}:${MOUNT_DST}"; do
    local mntpt="${part_mnt#*:}"
    local part_path="/dev/disk/by-partlabel/${part_mnt%:*}"
    [[ -d "$mntpt" ]] || run_cmd mkdir "$mntpt"

    if ! mountpoint -q "$mntpt"; then
      run_cmd sudo mount -o user,noatime,nodiratime "$part_path" "$mntpt"
      run_cmd sudo chown -R "`whoami`": "$mntpt"

      [[ -d "$RESTORE_POINT" ]] || run_cmd mkdir -p "$RESTORE_POINT"
      [[ -d "$SNAP_POINT" ]] || run_cmd mkdir -p "$SNAP_POINT"
    fi
  done
}

prepare_disk() {
  [[ "$CREATE_DISK_FLAG" == 1 ]] || return
  run_cmd sudo parted "$DISK_DEVICE" mklabel gpt
}

prepare_partition() {
  [[ $CREATE_PART_FLAG == 1 ]] || return
  run_cmd sudo parted "$DISK_DEVICE" print
  run_cmd sudo parted "$DISK_DEVICE" mkpart primary btrfs 0% 50%
  run_cmd sudo parted "$DISK_DEVICE" mkpart primary btrfs 50% 100%
  run_cmd sudo parted "$DISK_DEVICE" name 1 "$PART_LBL_SRC"
  run_cmd sudo parted "$DISK_DEVICE" name 2 "$PART_LBL_DST"

  sync
  run_cmd ls "/dev/disk/by-partlabel"
  try_or_die -e "/dev/disk/by-partlabel/$PART_LBL_SRC"
  try_or_die -e "/dev/disk/by-partlabel/$PART_LBL_DST"
}

prepare_filesystem() {
  for label in "$PART_LBL_SRC" "$PART_LBL_DST"; do
    local part_path="/dev/disk/by-partlabel/${label}"
    run_cmd sudo mkfs.btrfs -fL "$label" "$part_path"

    #run_cmd sudo blkid --garbage-collect
    #run_cmd blkid --label "$label"
  done
}

create_dummy_subvol_and_snap() {
  pushd "$MOUNT_SRC"
  for subvol_name in "${SUBVOL_LIST[@]}"; do
    try_or_die ! -z "$subvol_name" -a ! -e "$subvol_name"

    run_cmd sudo btrfs subvolume create "$subvol_name" 
    run_cmd sudo chown -R `whoami`: "$subvol_name"
    modify_subvol_and_snap "$subvol_name"
    #modify_subvol_and_snap "$subvol_name"
  done
  popd
}

modify_subvol_and_snap() {
  [[ "$CREATE_SNAP_FLAG" == 1 ]] || return
  local subvol_name="$1"
  local subvol_path="$MOUNT_SRC/$1"
  local snap_path=`mktemp -u "${SNAP_POINT}/${subvol_name}.XXXXXX"`
  local restore_path=`mktemp -u "$TEMP/${subvol_name}.send.XXXXXX"`

  write_random_file "$subvol_path"
  run_cmd sudo btrfs subvolume snapshot -r "$subvol_path" "$snap_path"
  run_cmd sudo btrfs send -f "$restore_path" "$snap_path"
  run_cmd sudo btrfs receive -f "$restore_path" "${RESTORE_POINT}"
}

write_random_file() {
  local size=${2:-64}
  local filepath=`mktemp -u "${1}/file.XXXXXX"`

  echo "Writing random data to $filepath"
  dd bs=1024 count=$size if=/dev/urandom | strings > "$filepath"

  if [[ $? != 0 ]]; then
    echo "[ERROR] Could not write $filepath"
    exit 3
  fi
}

############################################### MAIN #########################################

parse_opts "$@"
avoid_shot_in_the_foot
umount_fs
prepare_disk
prepare_partition
prepare_filesystem
mount_fs
create_dummy_subvol_and_snap
sync
[[ "$LEAVE_UMOUNTED" == 1 ]] && umount_fs

