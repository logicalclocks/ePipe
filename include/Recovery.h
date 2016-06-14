/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Recovery.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 * Created on June 14, 2016, 1:29 PM
 */

#ifndef RECOVERY_H
#define RECOVERY_H
#include "Utils.h"

struct RecoveryIndeces{
    int mProjectIndex;
    int mDatasetIndex;
    int mMetadataIndex;
};

class Recovery {
public:
    static RecoveryIndeces getRecoveryIndeces(Ndb* connection);
    
private:

};

#endif /* RECOVERY_H */

