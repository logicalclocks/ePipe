#!/bin/bash

if [ $# -lt 1 ]; then
   echo "./deploy.sh [debian|rhel|test] [build] [cmake opts]"
   exit 1
fi

PLATFORM=$1

if [ "$2" != "" ]; then
   rm -rf build
   mkdir build
   cd build
   cmake .. $3
   make
   cd ..
fi

if [ ! -f ./build/ePipe ]; then 
  echo "./build/ePipe not found, try to build first or run ./deploy.sh with build"
  exit 1
fi

P=`./build/ePipe --version | awk '{print tolower($1)"-"$2}'`
PK=${P}.tar.gz

rm -r $P $PK

mkdir $P
mkdir $P/bin
cp build/ePipe $P/bin/epipe

tar -zcvf $PK $P

rsync $PK   glassfish@snurran.sics.se:/var/www/hops/epipe/$PLATFORM
