ARG ubuntu_image=bionic

FROM ubuntu:${ubuntu_image}

ARG cmake_version=3.16.4
ARG boost_version=1.70.0
ARG rondb_version=21.04.7
ARG glibc_version=2.17
ARG arch=x86_64
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
wget --progress=bar:force https://boostorg.jfrog.io/artifactory/main/release/${boost_version}/source/boost_${BOOST_V}.tar.gz; \
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
wget --progress=bar:force https://repo.hops.works/master/rondb-${rondb_version}-linux-glibc${glibc_version}-${arch}.tar.gz; \
tar xzf rondb-${rondb_version}-linux-glibc${glibc_version}-${arch}.tar.gz; \
ln -s rondb-${rondb_version}-linux-glibc${glibc_version}-${arch} mysql 

RUN mkdir /etc/rondb && chmod -R 777 /etc/rondb; \
  mkdir /etc/rondb/data &&  mkdir /etc/rondb/log  && mkdir /etc/rondb/mgmd  && mkdir /etc/rondb/mysqld

COPY rondb.cnf /etc/rondb/rondb.cnf
COPY my.cnf /etc/rondb/my.cnf
COPY start-rondb.sh /etc/rondb/start-rondb.sh 
COPY mysql-client.sh /etc/rondb/mysql-client.sh
COPY mgm-client.sh /etc/rondb/mgm-client.sh

RUN chmod +x /etc/rondb/*.sh

RUN apt-get -y update && apt-get install -y vim libnuma-dev

RUN groupadd epipe --gid ${groupid}; \
useradd -ms /bin/bash epipe --uid ${userid} --gid ${groupid}
