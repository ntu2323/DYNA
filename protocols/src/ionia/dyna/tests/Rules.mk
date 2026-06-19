d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	dyna_rw_verify.cc)

$(d)dyna-rw-verify: $(o)dyna_rw_verify.o \
	$(OBJS-dyna-client) \
	$(LIB-udptransport) $(LIB-fasttransport) \
	$(LIB-cpu_affinity) $(LIB-data_structure)

BINS += $(d)dyna-rw-verify
