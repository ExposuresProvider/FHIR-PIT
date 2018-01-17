#ifndef UTILS_HPP
#define UTILS_HPP
#include <time.h>
#include <map>

long strtdays(const std::string&start_date);

long strtyear(const std::string&start_date);

template<typename K, typename V>
std::map<K,V>& operator+=(std::map<K,V> &a, const std::map<K,V> &b) {
  for(const auto& bkv : b) {
    const auto &bk = std::get<0>(bkv);
    const auto &bv = std::get<1>(bkv);

    a[bk] += bv;
    
  }
  return a;
}

enum Bool {
  True = 1,
  False = 0
};

Bool &operator+=(Bool &a, const Bool &b);

#endif
