.PHONY : clean native python test config package

CC := gcc
CPPFLAGS := -D_XOPEN_SOURCE=700
CFLAGS := -fPIC -Ibtrfs_lib -std=gnu99 -Wall -O0 -ggdb
LDFLAGS :=
LDLIBS := 
SOFLAGS := -shared -Wl,--version-script=btrfs_lib/pybtrfs.export 

native: bin/pybtrfs.so bin/btrfs_test btrfs_lib/tags

python: $(wildcard python/*.py )
	python -m compileall -l python
	mv python/*.pyc bin

config:	config.properties config_log.json
	cp config.properties config_log.json bin

test: python native config
	sudo bin/btrfs_test $(C_TEST_ARG)
	sudo python bin/test_pybtrfs.pyc
	#sudo python bin/test_common_routines.pyc
	echo -e "\n\n######################### ALL TESTS OK #########################################\n"

package: python native config
	rm bin/*.o
	rm bin/*test*
	tar -zcf btrfs_glacier_`date +%y%m%d%H%M`.tar.gz bin/*

clean:
	rm bin/*
	rm btrfs_lib/tags

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
	(cd btrfs_lib && ctags *.h *.c)

bin/%.o :
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

