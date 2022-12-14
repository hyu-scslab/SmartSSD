#! /bin/sh
cat $1 | \
sed 's/cost=10\{7\}[\.0-9]\+ /cost={inf}..{inf} /;s/cost=[\.0-9]\+ /cost=xxx..xxx /;s/width=[0-9]\+\([^0-9]\)/width=xxx\1/;s/^ *QUERY PLAN *$/  QUERY PLAN/;s/^--*$/----------------/' |\
egrep -v "^ *((Planning time|JIT|Functions|Options):|\([0-9]* rows\))"
