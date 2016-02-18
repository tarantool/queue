all:
	@echo "Only tests are avalable: `make test`"

test:
	prove -v ./t/*.t
