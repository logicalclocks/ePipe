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
 * File:   FsMutationsBatcher.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 * Created on April 19, 2016, 2:01 PM
 */

#ifndef FSMUTATIONSBATCHER_H
#define FSMUTATIONSBATCHER_H

#include "Batcher.h"
#include "FsMutationsTableTailer.h"
#include "NdbDataReader.h"

class FsMutationsBatcher : public Batcher{
public:
    FsMutationsBatcher(FsMutationsTableTailer* table_tailer, NdbDataReader* data_reader, const int time_before_issuing_ndb_reqs, const int batch_size);
    virtual ~FsMutationsBatcher();

private:
    
    FsMutationsTableTailer* mTableTailer;
    NdbDataReader* mNdbDataReader;
    
    Cus* mAddOperations;    
    Cus* mDeleteOperations;    
    boost::mutex mLock;
        
    virtual void run();
    virtual void processBatch();
};

#endif /* FSMUTATIONSBATCHER_H */

