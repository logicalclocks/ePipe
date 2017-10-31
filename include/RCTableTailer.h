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
 * File:   RCTableTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef RCTABLETAILER_H
#define RCTABLETAILER_H

#include "TableTailer.h"

const int SINGLE_QUEUE = -1;

template<typename TableRow>
class RCTableTailer : public TableTailer{
public:
    RCTableTailer(Ndb* ndb, const WatchTable table, const int poll_maxTimeToWait, const Barrier barrier) 
        : TableTailer(ndb, table, poll_maxTimeToWait, barrier){ 
    }
        
    virtual TableRow consumeMultiQueue(int queue_id) {
        //default behaviour is for single queue only
        return consume();
    }
    
    virtual TableRow consume() = 0;
};

#endif /* RCTABLETAILER_H */

