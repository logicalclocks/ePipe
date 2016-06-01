/*
 * Copyright (C) 2016 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

/* 
 * File:   main.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */
#include "Notifier.h"
#include <boost/program_options.hpp>

namespace po = boost::program_options;

int main(int argc, char** argv) { 
    po::options_description desc("Allowed options");
    desc.add_options()
            ("help", "produce help message")
            ("connection", po::value<string>(), "connection string/ ndb host")
            ("database", po::value<string>()->default_value("hops"), "database name.")
            ("meta_database", po::value<string>()->default_value("hopsworks"), "database name for metadata")
            ("poll_maxTimeToWait", po::value<int>()->default_value(1000), "max time to wait in miliseconds while waiting for events in pollEvents")
            ("wait_time", po::value<int>()->default_value(10000), "time to wait in miliseconds before issuing the ndb request or the batch size reached")
            ("ndb_batch", po::value<int>()->default_value(5), "batch size for reading from ndb")
            ("num_ndb_readers", po::value<int>()->default_value(5), "num of ndb reader threads")
            ("elastic_addr", po::value<string>()->default_value("localhost:9200"), "ip and port of the elasticsearch server")
            ("hopsworks", po::value<bool>()->default_value(true), "enable or disable hopsworks, which will use grandparents to index inodes and metadata")
            ("index", po::value<string>()->default_value("projects"), "Elastic index to add the data to.")
            ("project_type", po::value<string>()->default_value("proj"), "Elastic type for projects, only used when hopsworks is enabled.")
            ("dataset_type", po::value<string>()->default_value("ds"), "Elastic type for datasets, only used when hopsworks is enabled.")
            ("inode_type", po::value<string>()->default_value("inode"), "Elastic type for inodes.")
            ("lru_cap", po::value<int>()->default_value(DEFAULT_MAX_CAPACITY), "LRU Cache max capacity")
            ("log_level", po::value<int>()->default_value(2), "log level trace=0, debug=1, info=2, warn=3, error=4, fatal=5")
            ;

    
    string connection_string;
    string database_name;
    string meta_database_name;
    int wait_time;
    int ndb_batch;
    int poll_maxTimeToWait;
    int num_ndb_readers;
    string elastic_addr;
    int log_level;
    
    bool hopsworks;
    string elastic_index;
    string elastic_project_type;
    string elastic_dataset_type;
    string elastic_inode_type;
    int lru_cap;
    
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << "\n";
        return EXIT_SUCCESS;
    }

    if (vm.count("connection")) {
        connection_string = vm["connection"].as<string>();
    }

    if (vm.count("database")) {
        database_name = vm["database"].as<string>();
    }

    if (vm.count("meta_database")) {
        meta_database_name = vm["meta_database"].as<string>();
    }
    
    if (vm.count("log_level")) {
        log_level = vm["log_level"].as<int>();
    }

    if (vm.count("wait_time")) {
        wait_time = vm["wait_time"].as<int>();
    }

    if (vm.count("ndb_batch")) {
        ndb_batch = vm["ndb_batch"].as<int>();
    }

    if (vm.count("poll_maxTimeToWait")) {
        poll_maxTimeToWait = vm["poll_maxTimeToWait"].as<int>();
    }

    if (vm.count("num_ndb_readers")) {
        num_ndb_readers = vm["num_ndb_readers"].as<int>();
    }
    
    if(vm.count("elastic_addr")){
        elastic_addr = vm["elastic_addr"].as<string>();
    }
    
    if(vm.count("hopsworks")){
        hopsworks = vm["hopsworks"].as<bool>();
    }
    
     if(vm.count("index")){
        elastic_index = vm["index"].as<string>();
    }
    
    if(vm.count("project_type")){
        elastic_project_type = vm["project_type"].as<string>();
    }
    
    if(vm.count("dataset_type")){
        elastic_dataset_type = vm["dataset_type"].as<string>();
    }
    
    if(vm.count("inode_type")){
        elastic_inode_type = vm["inode_type"].as<string>();
    }
    
    if (vm.count("lru_cap")) {
        lru_cap = vm["lru_cap"].as<int>();
    }
    
    Logger::setLoggerLevel(log_level);
            
    if(connection_string.empty() || database_name.empty() || meta_database_name.empty()){
        LOG_ERROR("you should provide at least connection, database, meta_database");
        return EXIT_FAILURE;
    }
        
    Notifier *notifer = new Notifier(connection_string.c_str(), database_name.c_str(), meta_database_name.c_str(),
            wait_time, ndb_batch, poll_maxTimeToWait, num_ndb_readers, elastic_addr, hopsworks, elastic_index,
            elastic_project_type, elastic_dataset_type, elastic_inode_type, lru_cap);
    notifer->start();

    return EXIT_SUCCESS;
}
