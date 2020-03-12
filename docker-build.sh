#!/bin/bash

set -e

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    echo "Usage."
    echo "./docker_build.sh [image_prefix]"
    echo "  image_prefix - the prefix to be used with the docker image name."
    exit 0
fi

PREFIX=$1
USERID=`id -u`
GROUPID=`id -g`

command -v "docker"
if [[ "${?}" -ne 0 ]]; then
  echo "Make sure that you have docker installed to be able to build ePipe."
  exit 1
fi

V_MAJOR=`grep EPIPE_VERSION_MAJOR CMakeLists.txt | awk '{gsub(/\)/,""); print $2}'`
V_MINOR=`grep EPIPE_VERSION_MINOR CMakeLists.txt | awk '{gsub(/\)/,""); print $2}'`
V_BUILD=`grep EPIPE_VERSION_BUILD CMakeLists.txt | awk '{gsub(/\)/,""); print $2}'`
EPIPE_VERSION="$V_MAJOR.$V_MINOR.$V_BUILD"

rm -rf builds
mkdir builds
echo "$EPIPE_VERSION" > builds/version

for platform in debian rhel
do
  DOCKER_FILE_DIR="centos"
  if [ "$platform" == "debian" ]; then
    DOCKER_FILE_DIR="ubuntu"
  fi

  DOCKER_IMAGE="epipe_build_${DOCKER_FILE_DIR}:${EPIPE_VERSION}"
  if [ "$PREFIX" != "" ]; then
    DOCKER_IMAGE="${PREFIX}_epipe_build_${DOCKER_FILE_DIR}:${EPIPE_VERSION}"
  fi

  echo "Creating docker image ${DOCKER_IMAGE}"
  docker build --build-arg userid=${USERID} --build-arg groupid=${GROUPID} ./docker/${DOCKER_FILE_DIR} -t $DOCKER_IMAGE

  echo "Building $platform using $DOCKER_IMAGE"
  docker run --rm -v "$PWD":/usr/epipe:z -w /usr/epipe "$DOCKER_IMAGE" ./build.sh

  EPIPE_BUILD_PATH="builds/${platform}"
  mkdir -p $EPIPE_BUILD_PATH
  mv epipe-${EPIPE_VERSION}.tar.gz $EPIPE_BUILD_PATH/
done
