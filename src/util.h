#ifndef __UTIL_H__
#define __UTIL_H__

#include <ctime>
#include <vector>
#include <string>

namespace vectordb {

namespace util {

#define KEY_STRING_LEN 20

bool Timestamp2KeyString(uint64_t timestamp, std::string &key);
bool KeyString2Timestamp(std::string key, uint64_t &timestamp);

void TimeStamp2String(uint64_t timestamp, std::string &str);
bool String2TimeStamp(const std::string &str, uint64_t &timestamp);

bool RemoveDir(const std::string &path);
bool ChildrenOfDir(const std::string &path, std::vector<std::string> &children_paths, std::vector<std::string> &children_names);
std::string LocalTimeString(time_t t);
unsigned int RSHash(const char *str);
void Split(const std::string &s, char separator, std::vector<std::string> &sv, const std::string ignore = "");

void ToLower(std::string &str);
bool DirOK(const std::string &path);
bool MakeDir(const std::string &path);
bool RecurMakeDir(const std::string &path);
bool RecurMakeDir2(const std::string &path);

} // namespace util

} // namespace vectordb

#endif
