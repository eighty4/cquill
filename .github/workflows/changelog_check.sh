#!/bin/bash
set -e

FILE=$1

found=$(awk -v p="## Unreleased" -F":" '$0 ~ p{f=1;next} /## /{f=0} f' "$FILE" | grep -c .)

if ((found == 0)); then
  exit 1
fi
