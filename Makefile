.PHONY: all clean install build
all: build doc

J=4

export OCAMLRUNPARAM=b

TESTS ?= --enable-tests
ifneq "$(MIRAGE_OS)" ""
TESTS := --disable-tests
endif


setup.bin: setup.ml
	@ocamlopt.opt -o $@ $< || ocamlopt -o $@ $< || ocamlc -o $@ $<
	@rm -f setup.cmx setup.cmi setup.o setup.cmo

setup.data: setup.bin
	@./setup.bin -configure $(TESTS)

build: setup.data setup.bin
	@./setup.bin -build -j $(J)

doc: setup.data setup.bin
	@./setup.bin -doc -j $(J)

install: setup.bin
	@./setup.bin -install

uninstall: setup.data
	@./setup.bin -uninstall
	ocamlfind remove xenstored
	ocamlfind remove xenstore

# oasis bug?
#test: setup.bin build
#	@./setup.bin -test
test:
	_build/core_test/core_test.native
	_build/server_test/server_test.native
	_build/server_test/binary.native
	./check-indent.sh

reindent:
	find . -name "*.ml" -exec ocp-indent -i {} \;
	find . -name "*.mli" -exec ocp-indent -i {} \;

reinstall: setup.bin
	@ocamlfind remove xenstore || true
	@ocamlfind remove xenstored || true
	@./setup.bin -reinstall

clean:
	@ocamlbuild -clean
	@rm -f setup.data setup.log setup.bin
