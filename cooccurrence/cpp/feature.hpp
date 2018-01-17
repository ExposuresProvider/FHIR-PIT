#ifndef FEATURE_HPP
#define FEATURE_HPP
#include "utils.hpp"
#include <iostream>
#include <tuple>
#include <vector>
#include <set>
#include <algorithm>
#include <cctype>
#include <time.h>
#include <cstdlib>

struct Code {
  std::string name;
  std::string start_date;
  Code(const std::string &name_, const std::string &start_date_);
  bool operator==(const Code& a) const;
  bool operator<(const Code& a) const;
};
  
struct Features {
  std::string patient_num;
  std::string race_cd;
  std::string sex_cd;
  std::string birth_date;
  std::string inout_cd;
  std::string start_date;
  std::set<Code> codes;
  std::set<std::string> genes;
};
  
Features extract_keys(const std::map<std::string, std::string> & input_map, const std::vector<std::tuple<std::string, std::string, std::string>> &criteria);




#endif
