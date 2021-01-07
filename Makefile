.ONESHELL:
.SECONDARY:
.PHONY: all clean dbg opt test

STAGE_PATH := /tmp/bin_btrfs_to_glacier
BTRFS_INSTALL := $(STAGE_PATH)/btrfs-progs/install_root
BTRFS_LIB     := $(BTRFS_INSTALL)/lib
BTRFS_INCLUDE := $(BTRFS_INSTALL)/include

CC       := gcc
CPPFLAGS := 
opt: CPPFLAGS := $(CPPFLAGS) -D_GNU_SOURCE -D__LEVEL_LOG__=2 -D__LEVEL_ASSERT__=0
dbg: CPPFLAGS := $(CPPFLAGS) -D_GNU_SOURCE -D__LEVEL_LOG__=4 -D__LEVEL_ASSERT__=1
CFLAGS_BTRFS  := -std=c11
CFLAGS   := $(CFLAGS_BTRFS) -I$(BTRFS_INCLUDE) -Iinclude -Wall -Werror
opt: CFLAGS := $(CFLAGS) -mtune=native -march=native -O3
dbg: CFLAGS := $(CFLAGS) -ggdb -fsanitize=address
LDFLAGS  :=
dbg: LDFLAGS := $(LDFLAGS) -fsanitize=address
LDLIBS   :=

headers = $(wildcard include/*.h)

all: bin/btrfs-progs/install_root dbg;
opt dbg: bin/test_btrfs_prog_integration;

bin/test_btrfs_prog_integration: LDLIBS := $(BTRFS_LIB)/libbtrfsutil.a
bin/test_btrfs_prog_integration: bin/common.o bin/test_btrfs_prog_integration.o

test: all
	bin/test_btrfs_prog_integration /tmp/btrfs_test_part_src/asubvol

bin:
	[[ -d $(STAGE_PATH) ]] || mkdir $(STAGE_PATH)
	ln -s $(STAGE_PATH) bin

bin/btrfs-progs: bin
	cp -r btrfs-progs $(STAGE_PATH)

bin/btrfs-progs/install_root: bin/btrfs-progs
	pushd $(STAGE_PATH)/btrfs-progs
	[[ -d "$(BTRFS_INSTALL)" ]] || mkdir "$(BTRFS_INSTALL)"
	bash autogen.sh
	CC="$(CC)" CFLAGS="$(CFLAGS_BTRFS)" \
	  bash configure --prefix="$(BTRFS_INSTALL)"
	# otherwise instal will fail since udev dir cannot be written
	sed -i 's!/usr/lib/udev!$${prefix}/lib/udev!' Makefile.inc
	[[ `id -u` == "0" ]] && echo never run this as root && exit 1
	make install

clean:
	rm -rf bin/*

fs_init:
	[[ `id -u` == "0" ]] && echo never run this as root && exit 1
	bash etc/setup_test_drive.sh -d "2018-08-18-15-33-20-00" -p btrfs_test_partition -s

tags:
	ctags *.h *.c

bin/%.o : src/%.c $(headers)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

bin/%:
	$(CC) $(LDFLAGS) -o $@ $^ $(LOADLIBES) $(LDLIBS)

