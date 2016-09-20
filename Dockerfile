FROM unikernel/mirage
COPY opam /src/opam
RUN sudo apt-get update
RUN opam install depext -y
RUN opam pin add ocaml-xenstore-server /src -n
RUN opam depext ocaml-xenstore-server -y
RUN opam install ocaml-xenstore-server --deps-only

