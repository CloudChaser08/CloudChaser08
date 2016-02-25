#!/bin/sh

awk '{ one=substr($0,1,5); two=substr($0,7,7); three=substr($0,15,1); four=substr($0,17,60); five=substr($0,78,400); printf ("%s\t%s\t%s\t%s\t%s\n", one, two, three, four, five)}' $1 > $1.tsv
