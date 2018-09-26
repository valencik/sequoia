#!/bin/bash

styles=(
    "dark	moon	espresso"
    "light	solarized	pygments"
)

(cd presentation && \
  for style in "${styles[@]}"
  do
    name=$(cut -f1 <<<"$style")
    theme=$(cut -f2 <<<"$style")
    highlight=$(cut -f3 <<<"$style")
    pandoc --standalone --to=revealjs \
      --variable slideNumber="'c/t'" \
      --variable theme="$theme" \
      --highlight-style="$highlight" \
      --output "pres-$name.html" scala-up-north.md
  done && \
  open -a Safari "pres-light.html"
)
