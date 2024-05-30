#!/bin/bash

set -e

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    echo "Usage."
    echo "./docker_build.sh [image_prefix] [--with-rondb]"
    echo "  image_prefix - the prefix to be used with the docker image name."
    exit 0
fi

PREFIX=$1
WITH_RONDB=$2

USERID=`id -u`
GROUPID=`id -g`

RONDB_VERSION="22.10.3"
GLIBC_VERSION="2.28"
UBUNTU_IMAGE="22.04"

# for testing on mac m1 
ARCH="x86_64"
if [ `uname -m` = "arm64" ]; then
    echo "Running on ARM"
    ARCH="arm64_v8"
    RONDB_VERSION="22.10.3"
    GLIBC_VERSION="2.31"
    UBUNTU_IMAGE="focal"
fi

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

PLATFORMS=(debian rhel)
# run with rondb only on ubuntu
if [ "$PREFIX" != "" ] && [ "$WITH_RONDB" == "--with-rondb" ] ; then
  PLATFORMS=(debian)
fi

for platform in "${PLATFORMS[@]}";
do
  SUFFIX_OS_VERSION="8"
  DOCKER_FILE_DIR="centos"
  if [ "$platform" == "debian" ]; then
    DOCKER_FILE_DIR="ubuntu"
    SUFFIX_OS_VERSION="${UBUNTU_IMAGE}"
  fi

  # run with rondb
  if [ "$PREFIX" != "" ] && [ "$WITH_RONDB" == "--with-rondb" ] ; then
    DOCKER_FILE_DIR="ubuntu/with-rondb"
  fi

  DOCKER_IMAGE="epipe_build_${DOCKER_FILE_DIR}_${SUFFIX_OS_VERSION}:${EPIPE_VERSION}"
  if [ "$PREFIX" != "" ]; then
    DOCKER_IMAGE="${PREFIX}_epipe_build_${DOCKER_FILE_DIR}_${SUFFIX_OS_VERSION}:${EPIPE_VERSION}"
  fi

  DOCKER_BUILD_ARGS="--build-arg userid=${USERID} --build-arg groupid=${GROUPID} --build-arg arch=${ARCH} --build-arg rondb_version=${RONDB_VERSION} --build-arg glibc_version=${GLIBC_VERSION} --build-arg ubuntu_image=${UBUNTU_IMAGE}"
  
  echo "Creating docker image ${DOCKER_IMAGE} with args ${DOCKER_BUILD_ARGS}"
  docker build ${DOCKER_BUILD_ARGS} ./docker/${DOCKER_FILE_DIR} -t $DOCKER_IMAGE

  echo "Building $platform using $DOCKER_IMAGE"
  docker run --rm -v "$PWD":/usr/epipe:z -w /usr/epipe "$DOCKER_IMAGE" ./build.sh

  EPIPE_BUILD_PATH="builds/${platform}"
  mkdir -p $EPIPE_BUILD_PATH
  mv epipe-${EPIPE_VERSION}.tar.gz $EPIPE_BUILD_PATH/
done
