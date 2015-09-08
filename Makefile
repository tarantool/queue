all: init fifo fifottl utube utubettl ttl

tarantool = tarantool

init:
	$(tarantool) t/000-init.t
fifo:
	$(tarantool) t/010-fifo.t
fifottl:
	$(tarantool) t/020-fifottl.t
utube:
	$(tarantool) t/030-utube.t
utubettl:
	$(tarantool) t/040-utubettl.t
ttl:
	$(tarantool) t/050-ttl.t
help:
	@echo "Only tests are avalable [ init | fifo | fifottl | utube | utubettl | ttl ]"
