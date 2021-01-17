.ONESHELL:
.SECONDARY:
.PHONY: all bin clean fs_init go_code go_unittest test

include etc/Makefile.include
STAGE_PATH := /tmp/bin_btrfs_to_glacier
BTRFS_GIT     := $(STAGE_PATH)/btrfs-progs
BTRFS_INSTALL := $(BTRFS_GIT)/install_root

# If the host has a libbtrfsutil with headers, link againt that instead of submodule
ifneq ($(USE_HOST_BTRFSUTIL), )
	BTRFS_LIB     := /usr/lib
	BTRFS_INCLUDE := /usr/include
	BTRFSUTIL_LBLIB := -lbtrfsutil
else
	BTRFS_LIB     := $(BTRFS_INSTALL)/lib
	BTRFS_INCLUDE := $(BTRFS_INSTALL)/include
	BTRFSUTIL_LBLIB := $(BTRFS_LIB)/libbtrfsutil.a
endif

GOENV    := $(STAGE_PATH)/go_env
MYGOSRC  := src/golang
GO_PROTOC_INSTALL := $(STAGE_PATH)/gobin/protoc-gen-go
# Chose NOT to store generated proto sources in git
# Can be problematic for some languages like C++ (see groups.google.com/g/protobuf/c/Qz5Aj7zK03Y)
GO_PROTO_GEN_SRCS  := $(MYGOSRC)/messages/messages.pb.go
PROTOSRC := src/proto
CC       := gcc
CPPFLAGS := -D_GNU_SOURCE -D__LEVEL_LOG__=4 -D__LEVEL_ASSERT__=1
CFLAGS_BTRFS  := -std=gnu11
# -mtune=native -march=native -O3
CFLAGS   := $(CFLAGS_BTRFS) -I$(BTRFS_INCLUDE) -Iinclude -Wall -Werror -ggdb
LDFLAGS  := 
LDLIBS   :=

headers := $(wildcard include/*.h)
go_files := $(shell find "$(MYGOSRC)" -type f -name '*.go')

all: $(BTRFS_INSTALL) go_code bin/test_btrfs_prog_integration;

clean:
	rm -rf bin/*

test: go_unittest all
	bin/test_btrfs_prog_integration "$(SUBVOL_PATH)" || exit 1
	pushd "$(MYGOSRC)"
	GOENV="$(GOENV)" go run ./integration_test --subvol="$(SUBVOL_PATH)" || exit 1

fs_init:
	[[ `id -u` == "0" ]] && echo never run this as root && exit 1
	bash etc/setup_test_drive.sh -d "$(DRIVE_UUID)" -l "$(FS_PREFIX)" -s "$(SUBVOL_NAME)"

go_code: $(GOENV) $(go_files) $(GO_PROTO_GEN_SRCS) | $(BTRFS_INSTALL)
	pushd "$(MYGOSRC)"
	GOENV="$(GOENV)" go install ./...

go_unittest: go_code
	pushd "$(MYGOSRC)"
	# add --test.v to get verbose tests
	GOENV="$(GOENV)" go test ./...

$(GOENV): | bin
	GOENV="$(GOENV)" go env -w CC="$(CC)" \
	                           CGO_CFLAGS="$(CFLAGS)" \
														 CGO_LDFLAGS="$(BTRFSUTIL_LBLIB)" \
														 GOPATH="$(STAGE_PATH)/gopath" \
														 GOBIN="$(STAGE_PATH)/gobin" \
														 GOCACHE="$(STAGE_PATH)/go-build"
	#GOENV="$(GOENV)" go env

bin:
	[[ -d $(STAGE_PATH) ]] || mkdir $(STAGE_PATH)
	[[ -L bin ]] || ln -s $(STAGE_PATH) bin

$(BTRFS_GIT): | bin
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

$(GO_PROTOC_INSTALL):
	GOENV="$(GOENV)" go get google.golang.org/protobuf/cmd/protoc-gen-go
	GOENV="$(GOENV)" go install google.golang.org/protobuf/cmd/protoc-gen-go

$(MYGOSRC)/messages/%.pb.go: $(PROTOSRC)/%.proto $(GO_PROTOC_INSTALL)
	export PATH="$(PATH):`GOENV="$(GOENV)" go env GOBIN`"
	protoc '-I=$(PROTOSRC)' '--go_out=$(MYGOSRC)' "$<"

bin/test_btrfs_prog_integration: LDLIBS := $(BTRFSUTIL_LBLIB)
bin/test_btrfs_prog_integration: bin/common.o bin/test_btrfs_prog_integration.o \
																 | $(BTRFS_INSTALL)

bin/%.o : src/%.c $(headers)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

bin/%:
	$(CC) $(LDFLAGS) -o $@ $^ $(LOADLIBES) $(LDLIBS)

