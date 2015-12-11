.PHONY : clean native

CC := gcc
CPPFLAGS := -D_XOPEN_SOURCE=700
CFLAGS := -Ibtrfs_lib -std=gnu99 -Wall -O0 -ggdb
LDFLAGS :=# -rdynamic
LDLIBS := 

bin/%.o :
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

native: bin/btrfs_test btrfs_lib/tags

bin/btrfs_test : bin/btrfs_test.o bin/btrfs_lib.o bin/btrfs_utils.o bin/rbtree.o 
	$(CC) $(LDFLAGS) $(LDLIBS) -o $@ $^

btrfs_lib/tags: $(wildcard btrfs_lib/*.h )
	(cd btrfs_lib && ctags *.h *.c)

bin/btrfs_test.o :  $(addprefix btrfs_lib/,  btrfs_test.c )
bin/btrfs_utils.o : $(addprefix btrfs_lib/,  btrfs_utils.c btrfs_lib.h )
bin/btrfs_lib.o :   $(addprefix btrfs_lib/,  btrfs_lib.c btrfs_lib.h btrfs_list.h ctree.h )
bin/rbtree.o :      $(addprefix btrfs_lib/,  rbtree.c rbtree.h rbtree_augmented.h list.h extend_io.h kerncompat.h)

clean:
	rm bin/*.o
	rm btrfs_lib/tags

