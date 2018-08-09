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
 * File:   Utils.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef UTILS_H
#define UTILS_H

#include "common.h"
#include<cstdlib>
#include<cstring>

typedef boost::posix_time::ptime ptime;

typedef Ndb* SConn;

struct MConn {
  SConn inodeConnection;
  SConn metadataConnection;
};

namespace Utils {
  
  inline static ptime getCurrentTime() {
    //return boost::posix_time::microsec_clock::universal_time()
    return boost::posix_time::microsec_clock::local_time();
  }

  inline static float getTimeDiffInMilliseconds(ptime start, ptime end) {
    boost::posix_time::time_duration diff = end - start;
    return diff.total_microseconds() / 1000.0;
  }

  inline static string concat(const char* a, const string b) {
    string buf(a);
    buf.append(b);
    return buf;
  }

  inline static const char* OpsLogOnToStr(OpsLogOn op) {
    switch (op) {
      case Dataset:
        return "Dataset";
      case Project:
        return "Project";
      case Schema:
        return "Schema";
      default:
        return "Unkown";
    }
  }

  inline static const char* OperationTypeToStr(OperationType optype) {
    switch (optype) {
      case Add:
        return "Add";
      case Update:
        return "Update";
      case Delete:
        return "Delete";
      default:
        return "Unkown";
    }
  }

  inline static const char* MetadataTypeToStr(MetadataType metaType) {
    switch (metaType) {
      case Schemabased:
        return "SchemaBased";
      case Schemaless:
        return "SchemaLess";
      default:
        return "Unkown";
    }
  }

  inline static string to_string(UISet& set) {
    stringstream out;
    out << "[";
    unsigned int i = 0;
    for (UISet::iterator it = set.begin(); it != set.end(); ++it, i++) {
      out << *it << (i < set.size() ? "," : "");
    }
    out << "]";
    return out.str();
  }
}

#endif /* UTILS_H */

