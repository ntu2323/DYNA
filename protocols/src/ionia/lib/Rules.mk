d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	lookup3.cc message.cc memory.cc \
	latency.cc configuration.cc transport.cc udptransport.cc shared_udp_port.cc \
	simtransport.cc fasttransport.cc \
	cpu_affinity.cc \
	workertasks.cc batchgen.cc data_structure.cc db.cc)

PROTOS += $(addprefix $(d), \
          latency-format.proto)

LIB-hash := $(o)lookup3.o

LIB-message := $(o)message.o $(LIB-hash)

LIB-hashtable := $(LIB-hash) $(LIB-message)

LIB-memory := $(o)memory.o

LIB-latency := $(o)latency.o $(o)latency-format.o $(LIB-message)

LIB-configuration := $(o)configuration.o $(LIB-message)

LIB-transport := $(o)transport.o $(o)workertasks.o $(LIB-message) $(LIB-configuration)

LIB-simtransport := $(o)simtransport.o $(LIB-transport) $(LIB-request) $(LIB-no-proto) $(LIB-mcast-proto)

LIB-udptransport := $(o)udptransport.o $(LIB-transport)

LIB-fasttransport := $(o)fasttransport.o $(o)shared_udp_port.o $(LIB-udptransport)

LIB-cpu_affinity := $(o)cpu_affinity.o

LIB-batchgen := $(o)batchgen.o

LIB-data_structure := $(o)data_structure.o

LIB-db := $(o)db.o

include $(d)tests/Rules.mk
