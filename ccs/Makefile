
include ../config.mk

ifndef CCSDXFILE
  CCSDXFILE=../ccs_dx.csv
endif

ifndef CCSPXFILE
  CCSPXFILE=../ccs_px.csv
endif

clean:
  psql < drop_table.sql

default:
  psql < create_table.sql

install:
  ./update.sh $CCSDXFILE $CCSPXFILE 
