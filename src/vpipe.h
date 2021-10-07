#ifndef __VPIPE_H__
#define __VPIPE_H__

#include <cstdio>
#include <set>
#include <map>
#include <vector>
#include <string>
#include <iostream>
#include <leveldb/db.h>
#include "vpipe.pb.h"

namespace vectordb {

class VPipe {
  public:
    VPipe(const std::string& path);
    ~VPipe();
    VPipe(const VPipe&) = delete;
    VPipe& operator=(const VPipe&) = delete;

    std::string meta_path() const {
        return path_ + "/meta";
    }

    std::string data_path() const {
        return path_ + "/data";
    }

    bool Load();
    bool Create();
    bool Produce(const std::string &msg);
    bool Consume(const std::string &name, std::string &msg);

  private:
    std::string path_;
    leveldb::DB* db_meta_;
    leveldb::DB* db_data_;

    uint64_t next_index_;
};

} // namespace vectordb

#endif
