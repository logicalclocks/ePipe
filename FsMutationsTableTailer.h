/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   MutationsTableTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef MUTATIONSTABLETAILER_H
#define MUTATIONSTABLETAILER_H

#include "TableTailer.h"

  
class FsMutationsTableTailer : public TableTailer{
public:
    FsMutationsTableTailer(Ndb* ndb);
    virtual ~FsMutationsTableTailer();
    
private:
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);

};

#endif /* MUTATIONSTABLETAILER_H */

