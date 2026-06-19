d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc replica.cc)

PROTOS += $(addprefix $(d), \
	dyna-new-proto.proto)

OBJS-dyna-new-client := $(o)client.o $(o)dyna-new-proto.o \
                        $(OBJS-client) $(LIB-message) \
                        $(LIB-configuration)

OBJS-dyna-new-replica := $(o)replica.o $(o)dyna-new-proto.o \
                         $(LIB-message) $(LIB-configuration) $(LIB-transport)
