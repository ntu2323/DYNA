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
		slowpath_test.cc)

$(d)dyna-multi-types-test: $(o)types_test.o \
	.obj/dyna_multi/dyna-multi-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-types-test

$(d)dyna-multi-indexed-pq-test: $(o)indexed_pq_test.o \
	.obj/dyna_multi/dyna-multi-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-indexed-pq-test

$(d)dyna-multi-orderer-release-test: $(o)orderer_release_test.o \
	.obj/dyna_multi/server_orderer.o \
	.obj/dyna_multi/dyna-multi-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-orderer-release-test

$(d)dyna-multi-quorum-test: $(o)quorum_test.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-quorum-test

$(d)dyna-multi-orderer-commit-test: $(o)orderer_commit_test.o \
	.obj/dyna_multi/server_orderer.o \
	.obj/dyna_multi/dyna-multi-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-orderer-commit-test

$(d)dyna-multi-tsagree-test: $(o)tsagree_test.o \
	.obj/dyna_multi/server_orderer.o \
	.obj/dyna_multi/dyna-multi-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-tsagree-test

$(d)dyna-multi-replicate-test: $(o)replicate_test.o \
	.obj/dyna_multi/server_orderer.o \
	.obj/dyna_multi/dyna-multi-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-replicate-test

$(d)dyna-multi-exec-test: $(o)exec_test.o \
	.obj/dyna_multi/server_orderer.o \
	.obj/dyna_multi/in_memory_backend.o \
	.obj/dyna_multi/dyna-multi-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-exec-test

$(d)dyna-multi-read-test: $(o)read_test.o \
	.obj/dyna_multi/server_orderer.o \
	.obj/dyna_multi/in_memory_backend.o \
	.obj/dyna_multi/dyna-multi-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-read-test

$(d)dyna-multi-slowpath-test: $(o)slowpath_test.o \
	.obj/dyna_multi/server_orderer.o \
	.obj/dyna_multi/in_memory_backend.o \
	.obj/dyna_multi/dyna-multi-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-multi-slowpath-test
