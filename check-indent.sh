#!/bin/sh

find . -name "*.ml" -exec cat {} \; > _build/before
find . -name "*.mli" -exec cat {} \; >> _build/before

find . -name "*.ml" -exec ocp-indent {} \; > _build/after
find . -name "*.mli" -exec ocp-indent {} \; >> _build/after

if diff _build/before _build/after; then
  echo OK
else
  echo Indentation looks bad
  exit 1
fi
