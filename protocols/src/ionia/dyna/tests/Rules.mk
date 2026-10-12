d := $(dir $(lastword $(MAKEFILE_LIST)))

#
# gtest-based tests
#
GTEST_SRCS += $(addprefix $(d), \
		types_test.cc \
		indexed_pq_test.cc \
		orderer_release_test.cc \
		quorum_test.cc \
		orderer_commit_test.cc \
		tsagree_test.cc \
		replicate_test.cc \
		exec_test.cc \
		read_test.cc \
		slowpath_test.cc \
		robust_test.cc)

$(d)dyna-types-test: $(o)types_test.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-types-test

$(d)dyna-indexed-pq-test: $(o)indexed_pq_test.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-indexed-pq-test

$(d)dyna-orderer-release-test: $(o)orderer_release_test.o \
	.obj/dyna/server_orderer.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-orderer-release-test

$(d)dyna-quorum-test: $(o)quorum_test.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-quorum-test

$(d)dyna-orderer-commit-test: $(o)orderer_commit_test.o \
	.obj/dyna/server_orderer.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-orderer-commit-test

$(d)dyna-tsagree-test: $(o)tsagree_test.o \
	.obj/dyna/server_orderer.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-tsagree-test

$(d)dyna-replicate-test: $(o)replicate_test.o \
	.obj/dyna/server_orderer.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-replicate-test

$(d)dyna-exec-test: $(o)exec_test.o \
	.obj/dyna/server_orderer.o \
	.obj/dyna/in_memory_backend.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-exec-test

$(d)dyna-read-test: $(o)read_test.o \
	.obj/dyna/server_orderer.o \
	.obj/dyna/in_memory_backend.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-read-test

$(d)dyna-slowpath-test: $(o)slowpath_test.o \
	.obj/dyna/server_orderer.o \
	.obj/dyna/in_memory_backend.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-slowpath-test

$(d)dyna-robust-test: $(o)robust_test.o \
	.obj/dyna/server_orderer.o \
	.obj/dyna/in_memory_backend.o \
	.obj/dyna/dyna-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-robust-test
