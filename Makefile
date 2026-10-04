# SAGUIN_BROKER names the broker the suite drives. Until saguin publishes
# a release binary the only one there is is the one you built: `make build`
# in a saguin checkout writes ./bin/saguin.

help:
	@echo "install  install the library's dependencies"
	@echo "test     the suite, against the broker named by SAGUIN_BROKER"
	@echo "demo     the guided tour, against that same broker"
	@echo "pack     build a tarball into dist/"
	@echo "clean    remove node_modules/ and dist/"

install:
	npm install

test:
	npm test

demo:
	node examples/demo.js

pack:
	npm pack --pack-destination dist

clean:
	rm -rf node_modules dist

.PHONY: help install test demo pack clean
