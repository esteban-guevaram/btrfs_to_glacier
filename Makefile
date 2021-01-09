.ONESHELL:
.SECONDARY:
.PHONY: all bin clean dbg opt test

include test_fs.include
STAGE_PATH := /tmp/bin_btrfs_to_glacier
BTRFS_GIT     := $(STAGE_PATH)/btrfs-progs
BTRFS_INSTALL := $(BTRFS_GIT)/install_root
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

all: $(BTRFS_INSTALL) dbg;
opt dbg: bin/test_btrfs_prog_integration;

bin/test_btrfs_prog_integration: LDLIBS := $(BTRFS_LIB)/libbtrfsutil.a
bin/test_btrfs_prog_integration: bin/common.o bin/test_btrfs_prog_integration.o \
                                 | $(BTRFS_INSTALL)

test: all
	bin/test_btrfs_prog_integration "$(SUBVOL_PATH)"

bin:
	[[ -d $(STAGE_PATH) ]] || mkdir $(STAGE_PATH)
	[[ -L bin ]] || ln -s $(STAGE_PATH) bin

$(BTRFS_GIT): bin
	git submodule init
	git submodule update
	cp -r "btrfs-progs" "$(STAGE_PATH)"

$(BTRFS_INSTALL): $(BTRFS_GIT)
	pushd "$(BTRFS_GIT)"
	[[ -d "$(BTRFS_INSTALL)" ]] || mkdir "$(BTRFS_INSTALL)"
	bash autogen.sh
	CC="$(CC)" CFLAGS="$(CFLAGS_BTRFS)" \
	  bash configure --prefix="$(BTRFS_INSTALL)"
	# otherwise instal will fail since udev dir cannot be written
	sed -i 's!/usr/lib/udev!$${prefix}/lib/udev!' Makefile.inc
	[[ `id -u` == "0" ]] && echo never run this as root && exit 1
	$(MAKE) install

clean:
	rm -rf bin/*

fs_init:
	[[ `id -u` == "0" ]] && echo never run this as root && exit 1
	bash etc/setup_test_drive.sh -d "$(DRIVE_UUID)" -l "$(FS_PREFIX)" -s "$(SUBVOL_NAME)"

tags:
	ctags *.h *.c

bin/%.o : src/%.c $(headers)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

bin/%:
	$(CC) $(LDFLAGS) -o $@ $^ $(LOADLIBES) $(LDLIBS)

