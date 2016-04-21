/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   FsMutationsDataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 * Created on April 21, 2016, 3:28 PM
 */

#ifndef FSMUTATIONSDATAREADER_H
#define FSMUTATIONSDATAREADER_H

#include "NdbDataReader.h"

class FsMutationsDataReader : public NdbDataReader<Cus_Cus>{
public:
    FsMutationsDataReader(Ndb** connections, const int num_readers);
    virtual ~FsMutationsDataReader();
private:
    virtual void readData(Ndb* connection, Cus_Cus data_batch);
};

#endif /* FSMUTATIONSDATAREADER_H */

