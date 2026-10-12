d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc replica.cc server_orderer.cc in_memory_backend.cc)

PROTOS += $(addprefix $(d), \
	dyna-proto.proto)

OBJS-dyna-client := $(o)client.o $(o)dyna-proto.o \
                          $(OBJS-client) $(LIB-message) \
                          $(LIB-configuration)

OBJS-dyna-replica := $(o)replica.o $(o)server_orderer.o $(o)in_memory_backend.o \
                           $(o)dyna-proto.o \
                           $(LIB-message) $(LIB-configuration) $(LIB-transport)

include $(d)tests/Rules.mk
