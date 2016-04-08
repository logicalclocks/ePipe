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
    if (argc < 2) {
        std::cout << "Arguments are <connect_string cluster> <database_name>.\n";
        exit(-1);
    }
    const char *connection_string = argv[1];
    const char *database_name = argv[2];
    
    init_logging(0);
    
    Notifier *notifer = new Notifier(connection_string, database_name, 1000, 5);
    notifer->start();

    return EXIT_SUCCESS;
}
