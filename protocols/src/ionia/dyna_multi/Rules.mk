d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc replica.cc server_orderer.cc in_memory_backend.cc)

PROTOS += $(addprefix $(d), \
	dyna-multi-proto.proto)

OBJS-dyna-multi-client := $(o)client.o $(o)dyna-multi-proto.o \
                          $(OBJS-client) $(LIB-message) \
                          $(LIB-configuration)

OBJS-dyna-multi-replica := $(o)replica.o $(o)server_orderer.o $(o)in_memory_backend.o \
                           $(o)dyna-multi-proto.o \
                           $(LIB-message) $(LIB-configuration) $(LIB-transport)

include $(d)tests/Rules.mk
