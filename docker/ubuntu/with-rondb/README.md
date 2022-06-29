# ePipe with integrated RonDB

In this setup, we run a local RonDB cluster within the docker container used for building ePipe. This setup is useful for local tests. 

## How to build 

Clone the ePipe repo, and then build it using docker.
```
git clone https://github.com/logicalclocks/ePipe.git
cd ePipe
./docker-build.sh rondb --with-rondb
```

## How to Run 
First run the docker container by executing the following command on the ePipe directory:

```
docker run -t --name epipe_rondb -d  -v "$PWD":/usr/epipe:z -w /usr/epipe rondb_epipe_build_ubuntu/with-rondb:0.18.0 
```

Start RonDB 

```
docker exec -it epipe_rondb bash /etc/rondb/start-rondb.sh
```

Check the cluster status using mgm-client 

```
docker exec -it epipe_rondb bash /etc/rondb/mgm-client.sh
```

Use mysql

```
docker exec -it epipe_rondb bash /etc/rondb/mysql-client.sh
```

Using the mysql client, create a database named hops and add the required tables for your test then run the following to login to the container

```
docker exec -it epipe_rondb bash
```

Then run ePipe 

```
./build/ePipe -c config.ini.template
```