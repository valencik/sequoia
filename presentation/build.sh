#!/bin/bash

output="pres.html"
(
cd presentation && \
  pandoc --standalone --to=revealjs \
    -V slideNumber="'c/t'" \
    -V theme=solarized \
    --output "$output" scala-up-north.md && \
  open -a Safari "$output"
)
