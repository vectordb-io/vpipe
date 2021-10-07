#include <cstdio>
#include "util.h"
#include "vpipe.h"

namespace vectordb {

VPipe::VPipe(const std::string& path)
    :path_(path),
     next_index_(0) {
}

VPipe::~VPipe() {
    delete db_meta_;
    delete db_data_;
}

bool
VPipe::Load() {

    return true;
}

bool
VPipe::Create() {
    auto b = util::RecurMakeDir2(path_);
    assert(b);

    leveldb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;

    auto s = leveldb::DB::Open(options, meta_path(), &db_meta_);
    assert(s.ok());

    s = leveldb::DB::Open(options, data_path(), &db_data_);
    assert(s.ok());

    leveldb::Iterator* it = db_data_->NewIterator(leveldb::ReadOptions());
    it->SeekToLast();
    if (it->Valid()) {
        std::string value = it->value().ToString();
        auto b = util::KeyString2Timestamp(value, next_index_);
        assert(b);
    } else {
        next_index_ = 1;
    }
    assert(it->status().ok());  // Check for any errors found during the scan
    delete it;

    return true;
}

bool
VPipe::Produce(const std::string &msg) {
    std::string key;
    auto b = util::Timestamp2KeyString(next_index_, key);
    assert(b);
    next_index_++;

    auto s = db_data_->Put(leveldb::WriteOptions(), key, msg);
    assert(s.ok());

    return true;
}

bool
VPipe::Consume(const std::string &name, std::string &msg) {
    uint64_t next_index;
    std::string value;
    auto s = db_meta_->Get(leveldb::ReadOptions(), name, &value);
    if (s.IsNotFound()) {
        next_index = 1;

    } else if (s.ok()) {
        vpipe::UInt64 pb;
        pb.ParseFromString(value);
        next_index = pb.data();
    } else {
        assert(0);
    }

    std::string key;
    auto b = util::Timestamp2KeyString(next_index, key);
    assert(b);

    s = db_data_->Get(leveldb::ReadOptions(), key, &msg);
    assert(s.ok());

    next_index++;
    vpipe::UInt64 pb;
    pb.set_data(next_index);
    std::string str;
    pb.SerializeToString(&str);
    s = db_meta_->Put(leveldb::WriteOptions(), name, str);
    assert(b);

    return true;
}

} // namespace vectordb
