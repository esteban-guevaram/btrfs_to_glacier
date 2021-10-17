#! /bin/bash
# Creates dummy btrfs filesystems to test the project

MOUNT_SRC=""
MOUNT_DST=""
RESTORE_POINT=""
SNAP_POINT=""

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
CREATE_RSTR_SNAP=0
LEAVE_UMOUNTED=0
DRY_RUN=""
LOOP_FILE="/tmp/loop.btrfs.setup_test_drive"
LOOP_DEV="/dev/loop111"

PARTED_CMD=( parted --align=optimal --script )

parse_opts() {
  while getopts 'd:l:fpsur' opt; do
    case $opt in
      d) DISK_UUID="$OPTARG" ;;
      l) PART_PREFIX="$OPTARG" ;;
      f) CREATE_DISK_FLAG=1 ;;
      p) CREATE_PART_FLAG=1 ;;
      s) CREATE_SNAP_FLAG=1 ;;
      r) CREATE_RSTR_SNAP=1 ;;
      u) LEAVE_UMOUNTED=1 ;;
      *)
        echo "[USAGE] $0 -d DISK_UUID -l PART_PREFIX [-f][-p][-s][-u][-r] [SUBVOL_LIST]"
        exit 5
      ;;
    esac
  done
  shift $((OPTIND-1))
  DISK_DEVICE="`readlink -f "/dev/disk/by-uuid/$DISK_UUID"`"
	BASE_DEVICE="`basename "$DISK_DEVICE"`"
  PART_LBL_SRC="${PART_PREFIX}_src"
  PART_LBL_DST="${PART_PREFIX}_dst"
  SUBVOL_LIST=( "$@" )
  MOUNT_SRC="$TEMP/${PART_PREFIX}_src"
  MOUNT_VOL="$TEMP/${PART_PREFIX}_vol"
  MOUNT_DST="$TEMP/${PART_PREFIX}_dst"
  RESTORE_POINT="$MOUNT_DST/restore"
  SNAP_POINT="$MOUNT_SRC/snapshots"
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
  try_or_die -b "$DISK_DEVICE"

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

avoid_shot_in_the_foot_loop() {
  local backfile="/sys/block/`basename $LOOP_DEV`/loop/backing_file"
  sudo losetup "$LOOP_DEV" &> /dev/null || return
  if [[ -f "$backfile" ]]; then
    [[ "`cat "$backfile"`" == "$LOOP_FILE" ]] && return
    echo "[ERROR] '$backfile' points to another file"
    exit 8
  fi
}

umount_fs() {
  for mntpt in "$MOUNT_VOL"/* "$MOUNT_SRC" "$MOUNT_DST"; do
    [[ -e "$mntpt" ]] || continue
    mountpoint -q "$mntpt" && run_cmd sudo umount "$mntpt"
  done

  if sudo losetup "$LOOP_DEV" &> /dev/null; then
    if ! sudo losetup -d "$LOOP_DEV"; then
      echo "[ERROR] failed to detach '$LOOP_DEV'"
      exit 9
    fi
  fi
  [[ -f "$LOOP_FILE" ]] && rm "$LOOP_FILE"
}

mount_fs() {
  for part_mnt in "${PART_LBL_SRC}:${MOUNT_SRC}" "${PART_LBL_DST}:${MOUNT_DST}"; do
    local mntpt="${part_mnt#*:}"
    local part_path="/dev/disk/by-partlabel/${part_mnt%:*}"
    [[ -d "$mntpt" ]] || run_cmd mkdir "$mntpt"

    if ! mountpoint -q "$mntpt"; then
      run_cmd sudo mount -o user_subvol_rm_allowed,user,noexec "$part_path" "$mntpt"
      run_cmd sudo chown -R "`whoami`": "$mntpt"

      [[ -d "$RESTORE_POINT" ]] || run_cmd mkdir -p "$RESTORE_POINT"
      [[ -d "$MOUNT_VOL" ]] || run_cmd mkdir -p "$MOUNT_VOL"
    fi
  done
}

prepare_disk_dev() {
  [[ "$CREATE_DISK_FLAG" == 1 ]] || return
  run_cmd sudo "${PARTED_CMD[@]}" "$DISK_DEVICE" mklabel gpt
}

prepare_loop_dev() {
  local backfile="/sys/block/`basename $LOOP_DEV`/loop/backing_file"
  [[ "$CREATE_DISK_FLAG" == 1 ]] || return

  run_cmd dd if=/dev/zero of="$LOOP_FILE" bs=128M count=1
  run_cmd sudo losetup "$LOOP_DEV" "$LOOP_FILE"
  run_cmd sudo "${PARTED_CMD[@]}" "$LOOP_DEV" mklabel gpt
  try_or_die -b "$LOOP_DEV"
  try_or_die -f "$backfile"
}

prepare_partition() {
  local disk_dev="$1"
  try_or_die ! -z "$disk_dev"
  [[ "$CREATE_PART_FLAG" == 1 ]] || return
  run_cmd sudo "${PARTED_CMD[@]}" "$disk_dev" print
  run_cmd sudo "${PARTED_CMD[@]}" "$disk_dev" mkpart primary btrfs 1% 49%
  run_cmd sudo "${PARTED_CMD[@]}" "$disk_dev" mkpart primary btrfs 50% 99%
  run_cmd sudo "${PARTED_CMD[@]}" "$disk_dev" name 1 "$PART_LBL_SRC"
  run_cmd sudo "${PARTED_CMD[@]}" "$disk_dev" name 2 "$PART_LBL_DST"

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

prepare_all_for_disk_dev() {
  avoid_shot_in_the_foot
  umount_fs
  prepare_disk_dev
  prepare_partition "$DISK_DEVICE"
  prepare_filesystem
}

prepare_all_for_loop_dev() {
  avoid_shot_in_the_foot_loop
  umount_fs
  prepare_loop_dev
  prepare_partition "$LOOP_DEV"
  prepare_filesystem
}

# I tried to see if it was possible to have a user create,delete,list subvolumes
# on a btrfs fs created out of a loop device. Could not manage to list without having root privileges.
prepare_all_for_loop_fiasco() {
  # You need a root directory structure because even if you can mount the fs image,
  # The root of the mount will always be owned by root (unless you chown but you need root for that ...)
  local root_image="/tmp/root_image"
  mkdir -p "$root_image/subvolumes"
  mkdir -p "$root_image/snapshots"

  dd if=/dev/zero of="$LOOP_FILE" bs=128M count=1
  mkfs.btrfs --rootdir=root_image "$LOOP_FILE"

  # Requires the following in /etc/fstab
  # $LOOP_FILE $MOUNT_SRC  auto user_subvol_rm_allowed,noauto,user,noexec 0 2
  mount "$LOOP_FILE"

  # Works OK
  btrfs subvolume create "$MOUNT_SRC/subvolumes/asubvol"
  btrfs subvolume snap -r "$MOUNT_SRC/snapshots/asnap"
  btrfs subvolume delete "$MOUNT_SRC/subvolumes/asubvol"

  # ERROR: can't perform the search: Operation not permitted
  # Tried with different paths and -o flag, always the same ...
  # `chown`ing the whole mount root does not do sh*t
  # Using `unshare --map-root` does not do sh*t
  btrfs subvolume list "$MOUNT_SRC"

  # ERROR: Could not destroy subvolume/snapshot: Read-only file system
  # (but using `sudo` is OK)
  btrfs subvolume delete "$MOUNT_SRC/snapshots/asnap"
}

create_dummy_subvol_and_snap() {
  pushd "$MOUNT_SRC"
  # snapshot will be nested in another subvolume
  run_cmd btrfs subvolume create "`basename "$SNAP_POINT"`"

  for subvol_name in "${SUBVOL_LIST[@]}"; do
    try_or_die ! -z "$subvol_name" -a ! -e "$subvol_name"
    if [[ "`basename "$subvol_name"`" != "$subvol_name" ]]; then
      echo "[ERROR] only leaf subvols allowed, got '$subvol_name'"
      exit 10
    fi

    run_cmd btrfs subvolume create "$subvol_name"
    write_random_file "$MOUNT_SRC/$subvol_name/adir"
    #run_cmd sudo chown -R `whoami`: "$subvol_name"
    modify_subvol_and_snap "$subvol_name" "${subvol_name}.snap.1"
    modify_subvol_and_snap "$subvol_name" "${subvol_name}.snap.2"

    local mntpt="${MOUNT_VOL}/$subvol_name"
    [[ -d "$mntpt" ]] || run_cmd mkdir "$mntpt"
    run_cmd sudo mount -o subvol="$subvol_name" LABEL="$PART_LBL_SRC" "$mntpt"
  done
  popd
}

modify_subvol_and_snap() {
  [[ "$CREATE_SNAP_FLAG" == 1 ]] || return
  local subvol_name="$1"
  local subvol_path="$MOUNT_SRC/$1"
  local snap_path="${SNAP_POINT}/$2"

  write_random_file "$subvol_path"
  run_cmd btrfs subvolume snapshot -r "$subvol_path" "$snap_path"

  [[ "$CREATE_RSTR_SNAP" == 1 ]] || return
  local restore_path=`mktemp -u "$TEMP/${subvol_name}.send.XXXXXX"`
  # send/receive needs to be run as root
  run_cmd sudo btrfs send -f "$restore_path" "$snap_path"
  run_cmd sudo btrfs receive -f "$restore_path" "${RESTORE_POINT}"
}

write_random_file() {
  local size=${2:-64}
  local filepath=`mktemp -u "${1}/file.XXXXXX"`

  [[ -d "$1" ]] || mkdir "$1"
  echo "Writing random data to $filepath"
  dd bs=1024 count=$size if=/dev/urandom | strings > "$filepath"

  if [[ $? != 0 ]]; then
    echo "[ERROR] Could not write $filepath"
    exit 3
  fi
}

############################################### MAIN #########################################

parse_opts "$@"
#prepare_all_for_disk_dev
prepare_all_for_loop_dev
mount_fs
create_dummy_subvol_and_snap
sync
[[ "$LEAVE_UMOUNTED" == 0 ]] || umount_fs
echo "ALL DONE"

