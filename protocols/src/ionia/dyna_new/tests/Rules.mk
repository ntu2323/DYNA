d := $(dir $(lastword $(MAKEFILE_LIST)))

#
# gtest-based tests
#
GTEST_SRCS += $(addprefix $(d), \
		types_test.cc \
		storage_test.cc \
		orderer_test.cc \
		quorum_test.cc \
		recon_test.cc)

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
