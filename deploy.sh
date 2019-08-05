#!/bin/bash

if [ $# -lt 1 ]; then
   echo "./deploy.sh [debian|rhel] [build|build_test] [cmake opts]"
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

EPIPE_BUILD_PATH="epipe"
if [ "$2" == "build_test" ]; then
    EPIPE_BUILD_PATH="epipe/test"
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
mkdir $P/conf
cp build/ePipe $P/bin/epipe
cp config.ini.template $P/conf/

tar -zcvf $PK $P

rsync $PK   glassfish@snurran.sics.se:/var/www/hops/$EPIPE_BUILD_PATH/$PLATFORM
