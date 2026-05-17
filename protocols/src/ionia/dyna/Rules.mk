d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc replica.cc \
	deadline/deadline_config.cc \
	deadline/client_bound_tracker.cc \
	deadline/replica_owd_estimator.cc)

PROTOS += $(addprefix $(d), \
	dyna-proto.proto)

OBJS-dyna-client := $(o)client.o $(o)dyna-proto.o \
                    $(o)deadline/deadline_config.o $(o)deadline/client_bound_tracker.o \
                    $(OBJS-client) $(LIB-message) \
                    $(LIB-configuration)

OBJS-dyna-replica := $(o)replica.o $(o)dyna-proto.o \
                     $(o)deadline/deadline_config.o $(o)deadline/replica_owd_estimator.o \
                     $(LIB-message) $(LIB-configuration) $(LIB-transport)
