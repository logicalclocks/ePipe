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
#include"common.h"
#include "MutationsTableTailer.h"

Ndb_cluster_connection* connect_to_cluster(const char *connection_string);
void disconnect_from_cluster(Ndb_cluster_connection *c);

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << "Arguments are <connect_string cluster> <database_name>.\n";
        exit(-1);
    }
    const char *connection_string = argv[1];
    const char *database_name = argv[2];
    
    Ndb_cluster_connection *ndb_connection = connect_to_cluster(connection_string);

    printf("Connection Established.\n\n");

    
    Ndb* ndb = new Ndb(ndb_connection, database_name);
    if(ndb->init() == -1){
        APIERROR(ndb->getNdbError());
    }
    
    
    MutationsTableTailer* mTable = new MutationsTableTailer(ndb);
    mTable->start();
    
    disconnect_from_cluster(ndb_connection);

    return EXIT_SUCCESS;
}

Ndb_cluster_connection* connect_to_cluster(const char *connection_string) {
    Ndb_cluster_connection* c;

    if (ndb_init())
        exit(EXIT_FAILURE);

    c = new Ndb_cluster_connection(connection_string);

    if (c->connect(RETRIES, DELAY_BETWEEN_RETRIES, VERBOSE)) {
        fprintf(stderr, "Unable to connect to cluster.\n\n");
        exit(EXIT_FAILURE);
    }

    if (c->wait_until_ready(WAIT_UNTIL_READY, WAIT_UNTIL_READY) < 0) {
        fprintf(stderr, "Cluster was not ready.\n\n");
        exit(EXIT_FAILURE);
    }

    return c;
}

void disconnect_from_cluster(Ndb_cluster_connection *c) {
    delete c;

    ndb_end(2);
}
