.ONESHELL:
.SECONDARY:
.PHONY: all clean fs_init c_code go_code go_unittest test

PROJ_ROOT     := $(realpath $(dir $(lastword $(MAKEFILE_LIST))))
include etc/Makefile.include
STAGE_PATH    := /tmp/bin_btrfs_to_glacier

BTRFS_LIB     := /usr/lib
BTRFS_INCLUDE := /usr/include
BTRFSUTIL_LDLIB := -lbtrfsutil

GOENV    := $(STAGE_PATH)/go_env
MYGOSRC  := src/golang
GO_PROTOC_INSTALL := $(STAGE_PATH)/gobin/protoc-gen-go
# Chose NOT to store generated proto sources in git
# Can be problematic for some languages like C++ (see groups.google.com/g/protobuf/c/Qz5Aj7zK03Y)
GO_PROTO_GEN_SRCS  := $(MYGOSRC)/messages/config.pb.go $(MYGOSRC)/messages/messages.pb.go
PROTOSRC := src/proto
CC       := gcc
CPPFLAGS := -D_GNU_SOURCE -D__LEVEL_LOG__=4 -D__LEVEL_ASSERT__=1
CFLAGS_BTRFS  := -std=gnu11
# -mtune=native -march=native -O3
CFLAGS   := $(CFLAGS_BTRFS) -I$(BTRFS_INCLUDE) "-I$(PROJ_ROOT)/include" -Wall -Werror -ggdb
LDFLAGS  := 
LDLIBS   :=

headers  := $(wildcard include/*.h)
go_files := $(shell find "$(MYGOSRC)" -type f -name '*.go')
c_lib     = bin/$(1).so bin/$(1).a bin/$(1)_test

all: go_code c_code
go_code c_code test: | bin

c_code: $(call c_lib,linux_utils) bin/btrfs_progs_test

clean:
	if [[ -f "$(GOENV)" ]]; then
	  GOPATH="`GOENV="$(GOENV)" go env GOPATH`"
		chmod --recursive 'a+wx' "$$GOPATH"
	fi
	rm -rf bin/*

test: all go_unittest | $(SUBVOL_PATH)
	bin/btrfs_progs_test "$(SUBVOL_PATH)" || exit 1
	pushd "$(MYGOSRC)"
	GOENV="$(GOENV)" go run ./shim/integration \
	  --subvol="$(SUBVOL_PATH)" --rootvol="$(MOUNT_TESTVOL_SRC)" || exit 1

$(SUBVOL_PATH) fs_init:
	[[ `id -u` == "0" ]] && echo never run this as root && exit 1
	bash etc/setup_test_drive.sh -d "$(DRIVE_UUID)" -l "$(FS_PREFIX)" -s "$(SUBVOL_NAME)"

go_code: c_code $(GOENV) $(go_files) $(GO_PROTO_GEN_SRCS)
	pushd "$(MYGOSRC)"
	GOENV="$(GOENV)" go install ./...

go_unittest: go_code
	pushd "$(MYGOSRC)"
	# add --test.v to get verbose tests
	GOENV="$(GOENV)" go test ./...

$(GOENV): | bin
	COMMIT_ID="`git rev-list -1 HEAD`"
	GOENV="$(GOENV)" go env -w CC="$(CC)" \
														 CGO_CPPFLAGS="-DBTRFS_TO_GLACIER_VERSION=\"$$COMMIT_ID\"" \
	                           CGO_CFLAGS="$(CFLAGS)" \
														 CGO_LDFLAGS="$(BTRFSUTIL_LDLIB) $(STAGE_PATH)/linux_utils.a -lcap" \
														 GOPATH="$(STAGE_PATH)/gopath" \
														 GOBIN="$(STAGE_PATH)/gobin" \
														 GOCACHE="$(STAGE_PATH)/go-build"
	#GOENV="$(GOENV)" go env

bin: | $(STAGE_PATH)
	[[ -L bin ]] || ln -s $(STAGE_PATH) bin

$(STAGE_PATH):
	[[ -d $(STAGE_PATH) ]] || mkdir $(STAGE_PATH)

$(GO_PROTOC_INSTALL): $(GOENV)
	GOENV="$(GOENV)" go get google.golang.org/protobuf/cmd/protoc-gen-go

$(MYGOSRC)/messages/%.pb.go: $(PROTOSRC)/%.proto $(GOENV) | $(GO_PROTOC_INSTALL)
	export PATH="$(PATH):`GOENV="$(GOENV)" go env GOBIN`"
	protoc '-I=$(PROTOSRC)' '--go_out=$(MYGOSRC)' "$<"

$(call c_lib,linux_utils): LDLIBS := -lcap
$(call c_lib,linux_utils): bin/linux_utils.o bin/common.o

bin/%.o : src/%.c $(headers)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o "$@" "$<"

bin/%.so:
	$(CC) -shared $(LDFLAGS) -o "$@" $^ $(LOADLIBES) $(LDLIBS)

bin/%.a:
	ar rcs "$@" $^

bin/%_test: bin/%_test.o bin/%.o
	$(CC) $(LDFLAGS) -o "$@" $^ $(LOADLIBES) $(LDLIBS)

bin/btrfs_progs_test: bin/btrfs_progs_test.o bin/common.o
	$(CC) $(LDFLAGS) -o "$@" $^ $(BTRFSUTIL_LDLIB)

