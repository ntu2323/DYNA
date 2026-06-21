d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc replica.cc in_memory_backend.cc partition_orderer.cc)

PROTOS += $(addprefix $(d), \
	dyna-new-proto.proto)

OBJS-dyna-new-client := $(o)client.o $(o)dyna-new-proto.o \
                        $(OBJS-client) $(LIB-message) \
                        $(LIB-configuration)

OBJS-dyna-new-replica := $(o)replica.o $(o)dyna-new-proto.o \
                         $(o)in_memory_backend.o $(o)partition_orderer.o \
                         $(LIB-message) $(LIB-configuration) $(LIB-transport)

include $(d)tests/Rules.mk
