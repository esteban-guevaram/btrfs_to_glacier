.PHONY : clean native python test config package

CC := gcc
PYTHON := python3
PYTHON_INCL := /usr/include/python3.4m
#CPPFLAGS := -D_XOPEN_SOURCE=700 -DTRACING=1 -I$(PYTHON_INCL)
CPPFLAGS := -D_XOPEN_SOURCE=700 -I$(PYTHON_INCL)
#CFLAGS := -fPIC -Ibtrfs_lib -std=gnu99 -Wall -O0 -ggdb
CFLAGS := -fPIC -Ibtrfs_lib -std=gnu99 -Wall -O2
LDFLAGS :=
LDLIBS := 
SOFLAGS := -shared -Wl,--version-script=btrfs_lib/pybtrfs.export 

native: bin/pybtrfs.so bin/btrfs_test btrfs_lib/tags

python: $(wildcard python/*.py )
	$(PYTHON) -m compileall -b -l python
	mv python/*.pyc bin

config:	$(wildcard etc/*)
	cp etc/* bin

test: python native config
	sudo rm -f *.log || true
	sudo $(PYTHON) bin/test_common_routines.pyc
	sudo $(PYTHON) bin/test_pybtrfs.pyc
	sudo $(PYTHON) bin/test_btrfs_backup_restore.pyc
	echo -e "\n\n######################### ALL TESTS OK #########################################\n"

package: python native config
	rm -f bin/*.o
	rm -f bin/*test*
	tar -zcf btrfs_glacier_`date +%y%m%d%H%M`.tar.gz bin/*

clean:
	rm -f bin/*
	rm -f btrfs_lib/tags

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
	(cd btrfs_lib && ctags *.h *.c $(PYTHON_INCL)/*.h)

bin/%.o :
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

