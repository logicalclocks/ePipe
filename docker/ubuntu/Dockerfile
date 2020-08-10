FROM ubuntu:bionic

ARG cmake_version=3.16.4
ARG boost_version=1.70.0
ARG ndb_version_family=8.0
ARG ndb_version=8.0.21
ARG userid=1000
ARG groupid=1000

RUN echo Installing GCC 8; \
apt-get update -y; \
add-apt-repository ppa:ubuntu-toolchain-r/test -y; \
apt-get update -y; \
apt-get install gcc-8 g++-8 -y; \
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 60 --slave /usr/bin/g++ g++ /usr/bin/g++-8; \
update-alternatives --config gcc; \
apt-get install -y wget openssl libssl-dev git make

RUN echo Installing cmake; \
wget --progress=bar:force https://github.com/Kitware/CMake/releases/download/v${cmake_version}/cmake-${cmake_version}.tar.gz; \
tar xzf cmake-${cmake_version}.tar.gz; \
cd cmake-${cmake_version}; \
./bootstrap --prefix=/usr/local; \
make -j$(nproc); \
make install; \
cd ..; \
rm -r cmake-${cmake_version}*

RUN echo Installing Boost; \
BOOST_V=`echo ${boost_version} | sed s/'\.'/'_'/g`; \
wget --progress=bar:force https://dl.bintray.com/boostorg/release/${boost_version}/source/boost_${BOOST_V}.tar.gz; \
tar xzf boost_${BOOST_V}.tar.gz; \
cd boost_${BOOST_V}; \
./bootstrap.sh; \
./b2 link=static toolset=gcc cxxstd=14 install; \
cd ..; \
rm -r boost_${BOOST_V}*

# Use master to avoid compilation issues (https://github.com/Tencent/rapidjson/issues/1205)
# will update to new version when this is fixed to a version
RUN echo Installing RapidJson; \
git clone https://github.com/Tencent/rapidjson.git; \
cd rapidjson; \
git checkout master; \
mkdir build; \
cd build; \
cmake ..; \
make install

RUN echo Installing MySQL Cluster; \
cd /usr/local; \
wget --progress=bar:force https://cdn.mysql.com//Downloads/MySQL-Cluster-${ndb_version_family}/mysql-cluster-${ndb_version}-linux-glibc2.12-x86_64.tar.gz; \
tar xzf mysql-cluster-${ndb_version}-linux-glibc2.12-x86_64.tar.gz; \
mkdir -p mysql/lib; \
cp -r mysql-cluster-${ndb_version}-linux-glibc2.12-x86_64/include mysql/; \
cp -r mysql-cluster-${ndb_version}-linux-glibc2.12-x86_64/lib/libndbclient* mysql/lib/; \
rm -r mysql-cluster-${ndb_version}-linux-glibc2.12-x86_64*

RUN groupadd epipe --gid ${groupid}; \
useradd -ms /bin/bash epipe --uid ${userid} --gid ${groupid}

USER epipe
