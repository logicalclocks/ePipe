#ifndef EPIPE_FILEPROVENANCECONSTANTSRAW_H
#define EPIPE_FILEPROVENANCECONSTANTSRAW_H

#include <boost/assign.hpp>

namespace FileProvenanceConstantsRaw {
  const Int8 XATTRS_USER_NAMESPACE = 5;
  const std::string XATTR_PROV_CORE = "core";

  const std::string H_OP_CREATE = "CREATE";
  const std::string H_OP_DELETE = "DELETE";
  const std::string H_OP_ACCESS_DATA = "ACCESS_DATA";
  const std::string H_OP_MODIFY_DATA = "MODIFY_DATA";
  const std::string H_OP_METADATA = "METADATA";
  const std::string H_OP_XATTR_ADD = "XATTR_ADD";
  const std::string H_OP_XATTR_UPDATE = "XATTR_UPDATE";
  const std::string H_OP_XATTR_DELETE = "XATTR_DELETE";
  const std::string H_OP_OTHER = "OTHER";

  enum Operation {
    OP_CREATE,
    OP_DELETE,
    OP_ACCESS_DATA,
    OP_MODIFY_DATA,
    OP_XATTR_ADD,
    OP_XATTR_UPDATE,
    OP_XATTR_DELETE,
    OP_METADATA,
    OP_OTHER
  };

  const boost::unordered_map<std::string, Operation> ops = boost::assign::map_list_of
          (H_OP_CREATE, OP_CREATE)
          (H_OP_DELETE, OP_DELETE)
          (H_OP_ACCESS_DATA, OP_ACCESS_DATA)
          (H_OP_MODIFY_DATA, OP_MODIFY_DATA)
          (H_OP_XATTR_ADD, OP_XATTR_ADD)
          (H_OP_XATTR_UPDATE, OP_XATTR_UPDATE)
          (H_OP_XATTR_DELETE, OP_XATTR_DELETE)
          (H_OP_METADATA, OP_METADATA)
          (H_OP_OTHER, OP_OTHER);

  inline Operation findOp(std::string operation) {
    if(ops.find(operation) == ops.end()) {
      std::stringstream cause;
      cause << "no such operation:" << operation;
      LOG_WARN(cause.str());
      throw std::logic_error(cause.str());
    }
    return ops.at(operation);
  }
}
#endif //EPIPE_FILEPROVENANCECONSTANTSRAW_H
