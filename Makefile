.PHONY : clean

CC := gcc
CPPFLAGS := -D_XOPEN_SOURCE=700
CFLAGS := -Ibtrfs_lib -std=gnu99 -Wall -O0 -ggdb
LDFLAGS :=# -rdynamic
LDLIBS := 

bin/%.o :
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

bin/btrfs_test : bin/btrfs_test.o bin/btrfs_lib.o bin/rbtree.o
	$(CC) $(LDFLAGS) $(LDLIBS) -o $@ $^

bin/btrfs_test.o : $(addprefix btrfs_lib/,  btrfs_test.c )
bin/btrfs_lib.o :  $(addprefix btrfs_lib/,  btrfs_lib.c btrfs_lib.h btrfs_list.h ctree.h )
bin/rbtree.o :     $(addprefix btrfs_lib/,  rbtree.c rbtree.h rbtree_augmented.h list.h extend_io.h kerncompat.h)

clean:
	rm bin/*.o

