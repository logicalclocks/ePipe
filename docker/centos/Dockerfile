FROM quay.io/centos/centos:stream8

ARG boost_version=1.73.0
ARG rondb_version=21.04.9
ARG glibc_version=2.17
ARG arch=x86_64
ARG lib_ssl_version=3.0.8
ARG userid=1000
ARG groupid=1000

RUN dnf -y update; \
echo Setup environment; \
dnf -y group install "Development Tools"; \
dnf -y install wget git cmake perl-core zlib-devel

RUN cd; \
wget --progress=bar:force https://www.openssl.org/source/openssl-${lib_ssl_version}.tar.gz; \
tar xf openssl-${lib_ssl_version}.tar.gz; \
cd openssl-${lib_ssl_version}; \
./config --prefix=/usr/local/ssl --openssldir=/usr/local/ssl shared zlib; \
make -j$(nproc); \
make install; \
echo "/usr/local/ssl/lib" >> /etc/ld.so.conf.d/openssl-${lib_ssl_version}.conf; \
ldconfig -v; \
cd ..; \
rm -rf openssl-${lib_ssl_version} 


RUN echo Installing Boost; \
BOOST_V=`echo ${boost_version} | sed s/'\.'/'_'/g`; \
source scl_source enable devtoolset-8; \
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
mkdir -p mysql/lib; \
cp -r rondb-${rondb_version}-linux-glibc${glibc_version}-${arch}/include mysql/; \
cp -r rondb-${rondb_version}-linux-glibc${glibc_version}-${arch}/lib/* mysql/lib/; \
rm -r rondb-${rondb_version}-linux-glibc${glibc_version}-${arch}*

RUN groupadd epipe --gid ${groupid}; \
useradd -ms /bin/bash epipe --uid ${userid} --gid ${groupid}

USER epipe
