all: init fifo fifottl utube utubettl

init:
	tarantool t/000-init.t
fifo:
	tarantool t/010-fifo.t
fifottl:
	tarantool t/020-fifottl.t
utube:
	tarantool t/030-utube.t
utubettl:
	tarantool t/040-utubettl.t
stube:
	tarantool t/050-stube.t
help:
	@echo "Only tests are avalable [ init | fifo | fifottl | utube | utubettl | stube ]"
