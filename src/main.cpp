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
#include "Reindexer.h"

namespace po = boost::program_options;

int main(int argc, char** argv) {

  try {
    string connection_string;
    string database_name = "hops";
    string meta_database_name = "hopsworks";
    string hive_meta_database_name = "metastore";

    int poll_maxTimeToWait = 2000;
    string elastic_addr = "localhost:9200";
    int log_level = 2;

    TableUnitConf mutations_tu = TableUnitConf();
    TableUnitConf schamebased_tu = TableUnitConf();
    TableUnitConf schemaless_tu = TableUnitConf();
    TableUnitConf provenance_tu = TableUnitConf();

    bool hopsworks = true;
    string elastic_index = "projects";
    int elastic_batch_size = 5000;
    int elastic_issue_time = 5000;

    string elastic_provenance_index = "provenance";

    int lru_cap = DEFAULT_MAX_CAPACITY;
    bool recovery = true;
    bool stats = false;

    Barrier barrier = EPOCH;

    bool reindex = false;

    bool hiveCleaner = true;

    po::options_description generalOptions("General");
    generalOptions.add_options()
        ("help,h", "Help screen")
        ("desc,d", "Description of the allowed configuration parameters")
        ("version,v", "ePipe version")
        ("config,c", po::value<string>(), "Config file");


    po::options_description fileOptions("Allowed options");
    fileOptions.add_options()
        ("connection", po::value<string>(&connection_string),
         "connection string/ ndb host")
        ("database",
         po::value<string>(&database_name)->default_value(database_name),
         "database name.")
        ("meta_database", po::value<string>(&meta_database_name)->default_value(
            meta_database_name), "database name for metadata")
        ("hive_meta_database",
         po::value<string>(&hive_meta_database_name)->default_value(
             hive_meta_database_name), "database name for hive metadata")
        ("poll_maxTimeToWait",
         po::value<int>(&poll_maxTimeToWait)->default_value(poll_maxTimeToWait),
         "max time to wait in miliseconds while waiting for events in pollEvents")
        ("fs_mutations_tu",
         po::value<vector<int> >()->default_value(mutations_tu.getVector(),
                                                  mutations_tu.getString())->multitoken(),
         "WAIT_TIME BATCH_SIZE NUM_READERS")
        ("schamebased_tu",
         po::value<vector<int> >()->default_value(schamebased_tu.getVector(),
                                                  schamebased_tu.getString())->multitoken(),
         "WAIT_TIME BATCH_SIZE NUM_READERS")
        ("provenance_tu",
         po::value<vector<int> >()->default_value(provenance_tu.getVector(),
                                                  provenance_tu.getString())->multitoken(),
         "WAIT_TIME BATCH_SIZE NUM_READERS")
        ("schemaless_tu",
         po::value<vector<int> >()->default_value(schemaless_tu.getVector(),
                                                  schemaless_tu.getString())->multitoken(),
         "WAIT_TIME BATCH_SIZE NUM_READERS \nWAIT_TIME is the time to wait in miliseconds before issuing the ndb request if the batch size wasn't reached.\nBATCH_SIZE is the batch size for reading from ndb\nNUM_READERS is the num of threads used for reading from ndb and processing the data. The watch unit is disabled if set to 0 0 0.")
        ("elastic_addr",
         po::value<string>(&elastic_addr)->default_value(elastic_addr),
         "ip and port of the elasticsearch server")
        ("hopsworks", po::value<bool>(&hopsworks)->default_value(hopsworks),
         "enable or disable hopsworks")
        ("hivecleaner",
         po::value<bool>(&hiveCleaner)->default_value(hiveCleaner),
         "enable or disable hiveCleaner")
        ("index",
         po::value<string>(&elastic_index)->default_value(elastic_index),
         "Elastic index to add the data to.")
        ("provenance_index",
         po::value<string>(&elastic_provenance_index)->default_value(
             elastic_provenance_index),
         "Elastic index to add the provenance data to.")
        ("elastic_batch",
         po::value<int>(&elastic_batch_size)->default_value(elastic_batch_size),
         "Elastic batch size in bytes for bulk requests")
        ("ewait_time",
         po::value<int>(&elastic_issue_time)->default_value(elastic_issue_time),
         "time to wait in miliseconds before issuing a bulk request to Elasticsearch if the batch size wasn't reached")
        ("lru_cap", po::value<int>(&lru_cap)->default_value(lru_cap),
         "LRU Cache max capacity")
        ("log_level", po::value<int>(&log_level)->default_value(log_level),
         "log level trace=0, debug=1, info=2, warn=3, error=4, fatal=5")
        ("recovery", po::value<bool>(&recovery)->default_value(recovery),
         "enable or disable startup recovery")
        ("stats", po::value<bool>(&stats)->default_value(stats),
         "enable or disable print of accumulators stats")
        ("barrier", po::value<int>()->default_value(barrier),
         "Table tailer barrier type. EPOCH=0, GCI=1")
        ("reindex", po::value<bool>(&reindex)->default_value(reindex),
         "initialize an empty index with all metadata");


    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, generalOptions), vm);
    po::notify(vm);

    if (vm.count("help")) {
      cout << generalOptions << endl;
      return EXIT_SUCCESS;
    }

    if (vm.count("desc")) {
      cout << fileOptions << endl;
      return EXIT_SUCCESS;
    }

    if (vm.count("version")) {
      cout << "ePipe " << EPIPE_VERSION_MAJOR << "." << EPIPE_VERSION_MINOR
           << "." << EPIPE_VERSION_BUILD << endl;
      return EXIT_SUCCESS;
    }

    if (vm.count("config")) {
      std::ifstream ifs(vm["config"].as<std::string>().c_str());
      if (ifs)
        po::store(po::parse_config_file(ifs, fileOptions), vm);
    }
    po::notify(vm);


    if (vm.count("fs_mutations_tu")) {
      mutations_tu.update(vm["fs_mutations_tu"].as<vector<int> >());
    }

    if (vm.count("schamebased_tu")) {
      schamebased_tu.update(vm["schamebased_tu"].as<vector<int> >());
    }

    if (vm.count("schemaless_tu")) {
      schemaless_tu.update(vm["schemaless_tu"].as<vector<int> >());
    }

    if (vm.count("provenance_tu")) {
      provenance_tu.update(vm["provenance_tu"].as<vector<int> >());
    }

    if (vm.count("barrier")) {
      barrier = static_cast<Barrier> (vm["barrier"].as<int>());
    }

    Logger::setLoggerLevel(log_level);

    LOG_INFO("GOT --- " << vm["database"].as<std::string>() << " DDD " <<
                        database_name);

    if (connection_string.empty() || database_name.empty() ||
        meta_database_name.empty()) {
      LOG_ERROR(
          "you should provide at least connection, database, meta_database");
      return EXIT_FAILURE;
    }

    if (reindex) {
      Reindexer *reindexer = new Reindexer(connection_string.c_str(),
                                           database_name.c_str(),
                                           meta_database_name.c_str(),
                                           hive_meta_database_name.c_str(),
                                           elastic_addr,
                                           elastic_index, elastic_batch_size,
                                           elastic_issue_time, lru_cap);

      reindexer->run();
    } else {
      Notifier *notifer = new Notifier(connection_string.c_str(),
                                       database_name.c_str(),
                                       meta_database_name.c_str(),
                                       hive_meta_database_name.c_str(),
                                       mutations_tu, schamebased_tu,
                                       schemaless_tu, provenance_tu,
                                       poll_maxTimeToWait, elastic_addr,
                                       hopsworks, elastic_index,
                                       elastic_provenance_index,
                                       elastic_batch_size, elastic_issue_time,
                                       lru_cap, recovery, stats, barrier,
                                       hiveCleaner);
      notifer->start();
    }
    return EXIT_SUCCESS;
  }catch(const po::error &ex){
    std::cerr << ex.what() << '\n';
  }
}
