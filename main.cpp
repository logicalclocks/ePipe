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

void init_logging(int level) {
    switch (level) {
        case 0:
            boost::log::core::get()->set_filter
                    (
                    boost::log::trivial::severity >= boost::log::trivial::debug
                    );
            break;
        case 1:
            boost::log::core::get()->set_filter
                    (
                    boost::log::trivial::severity >= boost::log::trivial::info
                    );
            break;
        case 2:
            boost::log::core::get()->set_filter
                    (
                    boost::log::trivial::severity >= boost::log::trivial::error
                    );
            break;
    }
}

int main(int argc, char** argv) {
    po::options_description desc("Allowed options");
    desc.add_options()
            ("help", "produce help message")
            ("connection", po::value<string>(), "connection string/ ndb host")
            ("database", po::value<string>(), "database name")
            ("poll_maxTimeToWait", po::value<int>(), "max time to wait in miliseconds while waiting for events in pollEvents")
            ("wait_time", po::value<int>(), "time to wait in miliseconds before issuing the ndb request or the batch size reached")
            ("ndb_batch", po::value<int>(), "batch size for reading from ndb")
            ("log", po::value<int>(), "log level debug=0, info=1, error=2")

            ;

    string connection_string;
    string database_name;
    int wait_time = 1000;
    int ndb_batch = 5;
    int poll_maxTimeToWait = 1000;
    int log_level = 0;

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

    if (vm.count("log")) {
        log_level = vm["log"].as<int>();
    }

    if (vm.count("wait_time")) {
        log_level = vm["wait_time"].as<int>();
    }

    if (vm.count("ndb_batch")) {
        log_level = vm["ndb_batch"].as<int>();
    }

    if (vm.count("poll_maxTimeToWait")) {
        log_level = vm["poll_maxTimeToWait"].as<int>();
    }

    init_logging(log_level);

    Notifier *notifer = new Notifier(connection_string.c_str(), database_name.c_str(), 
            wait_time, ndb_batch, poll_maxTimeToWait);
    notifer->start();

    return EXIT_SUCCESS;
}
