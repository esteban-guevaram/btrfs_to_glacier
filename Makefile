.ONESHELL:
.SECONDARY:
.PHONY: all clean

STAGE_PATH := /tmp/bin_btrfs_to_glacier

CC       := gcc
CPPFLAGS := 
opt: CPPFLAGS := $(CPPFLAGS) -D_GNU_SOURCE -D__LEVEL_LOG__=2 -D__LEVEL_ASSERT__=0
dbg: CPPFLAGS := $(CPPFLAGS) -D_GNU_SOURCE -D__LEVEL_LOG__=4 -D__LEVEL_ASSERT__=1
CFLAGS_BTRFS  := -std=c99
CFLAGS   := $(CFLAGS_BTRFS) -I $(INC_DIR) -Wall -Werror
opt: CFLAGS := $(CFLAGS) -mtune=native -march=native -O3
dbg: CFLAGS := $(CFLAGS) -ggdb -fsanitize=address
LDFLAGS  :=
dbg: LDFLAGS := $(LDFLAGS) -fsanitize=address
LDLIBS   :=

flavors = opt dbg
headers = $(wildcard $(INC_DIR)/*.h)
c_files = $(wildcard $(SRC_DIR)/*.c)
objects = $(addsuffix .o, $(basename $(notdir $(c_files))))

all: bin/btrfs-progs/install_root;

bin:
	[[ -d $(STAGE_PATH) ]] || mkdir $(STAGE_PATH)
	ln -s $(STAGE_PATH) bin

bin/btrfs-progs: bin
	cp -r btrfs-progs $(STAGE_PATH)

bin/btrfs-progs/install_root: bin/btrfs-progs
	pushd $(STAGE_PATH)/btrfs-progs
	[[ -d install_root ]] || mkdir install_root
	bash autogen.sh
	CC="$(CC)" CFLAGS="$(CFLAGS_BTRFS)" \
	  bash configure --prefix=$(STAGE_PATH)/btrfs-progs/install_root
	# otherwise instal will fail since udev dir cannot be written
	sed -i 's!/usr/lib/udev!$${prefix}/lib/udev!' Makefile.inc
	[[ `id -u` == "0" ]] && echo never run this as root && exit 1
	make install

clean:
	rm -rf bin/*

fs_init: config
	echo bash setup_test_drive.sh -d /dev/sdb -o

tags:
	ctags *.h *.c

