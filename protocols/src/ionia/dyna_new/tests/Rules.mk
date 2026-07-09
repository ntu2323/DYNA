d := $(dir $(lastword $(MAKEFILE_LIST)))

#
# gtest-based tests
#
GTEST_SRCS += $(addprefix $(d), \
		types_test.cc \
		storage_test.cc \
		orderer_test.cc \
		quorum_test.cc \
		recon_test.cc \
		commit_test.cc \
		dedup_flow_test.cc \
		release_repro_test.cc)

$(d)dyna-new-types-test: $(o)types_test.o \
	.obj/dyna_new/dyna-new-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-new-types-test

$(d)dyna-new-storage-test: $(o)storage_test.o \
	.obj/dyna_new/in_memory_backend.o \
	.obj/dyna_new/dyna-new-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-new-storage-test

$(d)dyna-new-orderer-test: $(o)orderer_test.o \
	.obj/dyna_new/partition_orderer.o \
	.obj/dyna_new/in_memory_backend.o \
	.obj/dyna_new/dyna-new-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-new-orderer-test

$(d)dyna-new-quorum-test: $(o)quorum_test.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-new-quorum-test

$(d)dyna-new-recon-test: $(o)recon_test.o \
	.obj/dyna_new/partition_orderer.o \
	.obj/dyna_new/in_memory_backend.o \
	.obj/dyna_new/dyna-new-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-new-recon-test

$(d)dyna-new-commit-test: $(o)commit_test.o \
	.obj/dyna_new/partition_orderer.o \
	.obj/dyna_new/in_memory_backend.o \
	.obj/dyna_new/dyna-new-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-new-commit-test

$(d)dyna-new-dedup-flow-test: $(o)dedup_flow_test.o \
	.obj/dyna_new/dyna-new-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-new-dedup-flow-test

$(d)dyna-new-release-repro-test: $(o)release_repro_test.o \
	.obj/dyna_new/partition_orderer.o \
	.obj/dyna_new/in_memory_backend.o \
	.obj/dyna_new/dyna-new-proto.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)dyna-new-release-repro-test
