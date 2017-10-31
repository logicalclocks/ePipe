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
#include "Version.h"

namespace po = boost::program_options;

int main(int argc, char** argv) { 
    
    string connection_string;
    string database_name = "hops";
    string meta_database_name = "hopsworks";
    int poll_maxTimeToWait = 2000;
    string elastic_addr = "localhost:9200";
    int log_level = 2;
    
    TableUnitConf mutations_tu = TableUnitConf(1000, 5, 5);
    TableUnitConf metadata_tu = TableUnitConf(1000, 5, 5);
    TableUnitConf schemaless_tu = TableUnitConf(1000, 5, 5);

    bool hopsworks = true;
    string elastic_index = "projects";
    string elastic_project_type = "proj";
    string elastic_dataset_type = "ds";
    string elastic_inode_type = "inode";
    int elastic_batch_size = 5000;
    int elastic_issue_time = 5000;
    
    int lru_cap = DEFAULT_MAX_CAPACITY;
    bool recovery = true;
    bool stats = false;
    MetadataType metadata_type = Both;
    
    Barrier barrier = EPOCH;
    
    po::options_description desc("Allowed options");
    desc.add_options()
            ("help", "produce help message")
            ("connection", po::value<string>(&connection_string), "connection string/ ndb host")
            ("database", po::value<string>(&database_name)->default_value(database_name), "database name.")
            ("meta_database", po::value<string>(&meta_database_name)->default_value(meta_database_name), "database name for metadata")
            ("poll_maxTimeToWait", po::value<int>(&poll_maxTimeToWait)->default_value(poll_maxTimeToWait), "max time to wait in miliseconds while waiting for events in pollEvents")
            ("fs_mutations_tu", po::value< vector<int> >()->default_value(mutations_tu.getVector(), mutations_tu.getString())->multitoken(), "WAIT_TIME BATCH_SIZE NUM_READERS")
            ("metadata_tu", po::value< vector<int> >()->default_value(metadata_tu.getVector(), metadata_tu.getString())->multitoken(), "WAIT_TIME BATCH_SIZE NUM_READERS")
            ("schemaless_tu", po::value< vector<int> >()->default_value(schemaless_tu.getVector(), schemaless_tu.getString())->multitoken(), "WAIT_TIME BATCH_SIZE NUM_READERS \nWAIT_TIME is the time to wait in miliseconds before issuing the ndb request if the batch size wasn't reached.\nBATCH_SIZE is the batch size for reading from ndb\nNUM_READERS is the num of threads used for reading from ndb and processing the data")
            ("elastic_addr", po::value<string>(&elastic_addr)->default_value(elastic_addr), "ip and port of the elasticsearch server")
            ("hopsworks", po::value<bool>(&hopsworks)->default_value(hopsworks), "enable or disable hopsworks, which will use grandparents to index inodes and metadata")
            ("index", po::value<string>(&elastic_index)->default_value(elastic_index), "Elastic index to add the data to.")
            ("project_type", po::value<string>(&elastic_project_type)->default_value(elastic_project_type), "Elastic type for projects, only used when hopsworks is enabled.")
            ("dataset_type", po::value<string>(&elastic_dataset_type)->default_value(elastic_dataset_type), "Elastic type for datasets, only used when hopsworks is enabled.")
            ("inode_type", po::value<string>(&elastic_inode_type)->default_value(elastic_inode_type), "Elastic type for inodes.")
            ("elastic_batch", po::value<int>(&elastic_batch_size)->default_value(elastic_batch_size), "Elastic batch size in bytes for bulk requests")
            ("ewait_time", po::value<int>(&elastic_issue_time)->default_value(elastic_issue_time), "time to wait in miliseconds before issuing a bulk request to Elasticsearch if the batch size wasn't reached")
            ("lru_cap", po::value<int>(&lru_cap)->default_value(lru_cap), "LRU Cache max capacity")
            ("log_level", po::value<int>(&log_level)->default_value(log_level), "log level trace=0, debug=1, info=2, warn=3, error=4, fatal=5")
            ("recovery", po::value<bool>(&recovery)->default_value(recovery), "enable or disable startup recovery")
            ("stats", po::value<bool>(&stats)->default_value(stats), "enable or disable print of accumulators stats")
            ("metadata_type", po::value<int>()->default_value(metadata_type), "Extended metadata type; SchemaBased=0, Schemaless=1, Both=2")
            ("barrier", po::value<int>()->default_value(barrier), "Table tailer barrier type. EPOCH=0, GCI=1")
            ("version", "ePipe version")
            ;

   
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << endl;
        return EXIT_SUCCESS;
    }

    if (vm.count("version")) {
        cout << "ePipe " << EPIPE_VERSION_MAJOR << "."  << EPIPE_VERSION_MINOR 
                << "." << EPIPE_VERSION_BUILD << endl;
        return EXIT_SUCCESS;
    }
    
    if(vm.count("fs_mutations_tu")){
        mutations_tu.update(vm["fs_mutations_tu"].as< vector<int> >());
    }

    if (vm.count("metadata_tu")) {
        metadata_tu.update(vm["metadata_tu"].as< vector<int> >());
    }

    if (vm.count("schemaless_tu")) {
        schemaless_tu.update(vm["schemaless_tu"].as< vector<int> >());
    }
    
    if(vm.count("metadata_type")){
        metadata_type = static_cast<MetadataType>(vm["metadata_type"].as<int>());
    }
    
    if(vm.count("barrier")){
        barrier = static_cast<Barrier>(vm["barrier"].as<int>());
    }
    
    Logger::setLoggerLevel(log_level);
            
    if(connection_string.empty() || database_name.empty() || meta_database_name.empty()){
        LOG_ERROR("you should provide at least connection, database, meta_database");
        return EXIT_FAILURE;
    }

    Notifier *notifer = new Notifier(connection_string.c_str(), database_name.c_str(), 
            meta_database_name.c_str(), mutations_tu, metadata_tu, schemaless_tu,
            poll_maxTimeToWait, elastic_addr, hopsworks, elastic_index, elastic_project_type, 
            elastic_dataset_type, elastic_inode_type, elastic_batch_size, elastic_issue_time,
            lru_cap, recovery, stats, metadata_type, barrier);
    notifer->start();

    return EXIT_SUCCESS;
}
