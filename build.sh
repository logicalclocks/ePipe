#!/bin/bash -i
set -e
rm -rf build
mkdir build
cd build
cmake ..
make -j$(nproc)
cd ..

if [ ! -f ./build/ePipe ]; then
  echo "./build/ePipe not found, try to build first"
  exit 1
fi

P=`./build/ePipe --version | awk '{print tolower($1)"-"$2}'`
PK=${P}.tar.gz

rm -rf $P $PK

mkdir $P
mkdir $P/bin
mkdir $P/conf
cp build/ePipe $P/bin/epipe
cp config.ini.template $P/conf/
tar -zcvf $PK $P
echo "Done building ${P}"
