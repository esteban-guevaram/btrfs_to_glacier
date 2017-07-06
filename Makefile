.ONESHELL:
.PHONY : clean native test coverage config package fs_init

CC := gcc
COVRUN := coverage3
PYTHON := python3
PYTHON_INCL := /usr/include/python3.4m
#CPPFLAGS := -D_XOPEN_SOURCE=700 -DTRACING=1 -I$(PYTHON_INCL)
CPPFLAGS := -D_XOPEN_SOURCE=700 -I$(PYTHON_INCL)
#CFLAGS := -fPIC -Ibtrfs_lib -std=gnu99 -Wall -O0 -ggdb
CFLAGS := -fPIC -Ibtrfs_lib -std=gnu99 -Wall -O2
COVFLAGS := --append --rcfile=coveragerc
LDFLAGS :=
LDLIBS := 
SOFLAGS := -shared -Wl,--version-script=btrfs_lib/pybtrfs.export 

native: bin/pybtrfs.so bin/btrfs_test btrfs_lib/tags

bin/python_ts: $(wildcard python/*/*.py ) $(wildcard python/*.py )
	cp python/*.py bin
	cp python/*/*.py bin
	touch bin/python_ts

config:	$(wildcard etc/*)
	cp etc/* bin

test: bin/python_ts native config
	cd bin
	rm -f *.log
	$(COVRUN) erase --rcfile=coveragerc

	# these need to be run as root since they use the btrfs syscalls
	#sudo $(COVRUN) run $(COVFLAGS) test_common_routines.py      || exit 1
	#sudo $(COVRUN) run $(COVFLAGS) test_pybtrfs.py              || exit 1
	#sudo $(COVRUN) run $(COVFLAGS) test_btrfs_backup_restore.py || exit 1

	#$(COVRUN) run $(COVFLAGS) test_tree_hasher.py               || exit 1
	#$(COVRUN) run $(COVFLAGS) test_aws_s3_mgr.py                || exit 1
	$(COVRUN) run $(COVFLAGS) test_aws_glacier_mgr.py           || exit 1
	echo -e "\n\n######################### ALL TESTS OK #########################################\n"

coverage: 
	cd bin
	$(COVRUN) html --rcfile=coveragerc
	sed -i "s|`readlink -f .`/||g" htmlcov/*

package: bin/python_ts native config
	rm -f bin/*.o
	rm -f bin/*test*
	tar -zcf btrfs_glacier_`date +%y%m%d%H%M`.tar.gz bin/*

clean:
	rm -rf bin/*
	rm -f btrfs_lib/tags

fs_init: config
	bin/setup_test_drive.sh -d /dev/sde -o

bin/btrfs_test : $(addprefix bin/, btrfs_test.o btrfs_lib.o btrfs_utils.o rbtree.o )
bin/pybtrfs.so : $(addprefix bin/, pybtrfs.o pybtrfs_mod_function.o pybtrfs_mod_type.o btrfs_lib.o btrfs_utils.o rbtree.o ) btrfs_lib/pybtrfs.export
	$(CC) $(SOFLAGS) $(LDFLAGS) $(LDLIBS) -o $@ $(filter %.o %.a, $^)

bin/pybtrfs_mod_function.o :     $(addprefix btrfs_lib/,  pybtrfs_mod_function.c pybtrfs_mod_function.h )
bin/pybtrfs_mod_type.o :     $(addprefix btrfs_lib/,  pybtrfs_mod_type.c pybtrfs_mod_type.h )
bin/pybtrfs.o :     $(addprefix btrfs_lib/,  pybtrfs.c )

bin/btrfs_test.o :  $(addprefix btrfs_lib/,  btrfs_test.c )
bin/btrfs_utils.o : $(addprefix btrfs_lib/,  btrfs_utils.c btrfs_lib.h )
bin/btrfs_lib.o :   $(addprefix btrfs_lib/,  btrfs_lib.c btrfs_lib.h btrfs_list.h ctree.h )
bin/rbtree.o :      $(addprefix btrfs_lib/,  rbtree.c rbtree.h rbtree_augmented.h list.h extend_io.h kerncompat.h)

btrfs_lib/tags: $(wildcard btrfs_lib/*.h )
	cd btrfs_lib
	ctags *.h *.c $(PYTHON_INCL)/*.h

bin/%.o :
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

