#!/bin/bash

output="pres.html"
(
cd presentation && \
  pandoc --standalone --to=revealjs --output "$output" scala-up-north.md && \
  open -a Safari "$output"
)
