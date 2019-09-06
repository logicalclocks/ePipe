# ePipe

ePipe is a metadata system for HopsFS that provides replicated-metadata-as-a-service. The key component of ePipe is a databus that both creates a consistent, correctly-ordered change stream from HopsFS and eventually delivers the stream with low latency (sub-second) to external stores and downstream clients. 


How To build
============
**Software Required**
For compiling ePipe you will need the following software.

* CMake 3.5.0 or higher (3.15.0 is recommended)
* GCC 7.0 or higher
* Boost 1.70 or higher 
* [RapidJson](http://rapidjson.org/) 1.1.0
* [MySQL Cluster NDB](https://dev.mysql.com/downloads/cluster/)

Untar the MySQL Cluster binarries in /usr/local/
```
cd /usr/local
wget https://dev.mysql.com/get/Downloads/MySQL-Cluster-7.6/mysql-cluster-gpl-7.6.10-linux-glibc2.12-x86_64.tar.gz
tar xzvf mysql-cluster-gpl-7.6.10-linux-glibc2.12-x86_64.tar.gz
ln -s mysql-cluster-gpl-7.6.10-linux-glibc2.12-x86_64 mysql
```

Clone the ePipe repo, and then build it.
```
git clone https://github.com/logicalclocks/ePipe.git
cd ePipe
mkdir build
cd build
cmake ..
make
```

To run ePipe, you need to create a config.ini file, you can copy the config.ini.template and then update the configuration accordingly, then run ePipe.

```
cp config.ini.template config.ini
./ePipe -c config.ini
```

A description of the allowed configuration parameters can be found in the config.ini.template file as well as when running the description (-d | --desc) switch on ePipe.
```
./ePipe -d
```

ePipe is installed as part of the Hopsworks platform as specified by the [epipe-chef](https://github.com/logicalclocks/epipe-chef) cookbook
