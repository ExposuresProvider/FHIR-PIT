#include "utils.hpp"
#include "feature.hpp"
#include <iostream>
#include <tuple>
#include <vector>
#include <set>
#include <algorithm>
#include <cctype>
#include <time.h>
#include <cstdlib>

Code::Code(const std::string &name_, const std::string &start_date_):name(name_), start_date(start_date_) {
  
}

bool Code::operator==(const Code& a) const {
  return name == a.name && start_date == a.start_date;
}

bool Code::operator<(const Code& a) const {
  return name < a.name || (name == a.name && start_date < a.start_date);
}

template <typename K, typename V, typename KEY>
V get(const std::map<K,V>& m, KEY const& key)
{
  const auto &it = m.find( key );
  if (it == m.end()) {
    return V();
  }
  return it->second;
}

Features extract_keys(const std::map<std::string, std::string> & input_map, const std::vector<std::tuple<std::string, std::string, std::string>> &criteria) {
  Features retval;

  for(const auto&c : criteria) {
    const auto &key = std::get<0>(c);
    const auto &op = std::get<1>(c);
    const auto &value = std::get<2>(c);
    
    if(key == "age") {
      const auto days  = strtdays(input_map.at("birth_date"));
      const int age = days/365;
      const int age2 = atoi(value.c_str());
      if (op == ">") {
	if( age <= age2) {
	  return retval;
	}
      } else if(op == "<=") {
	if( age > age2) {
	  return retval;
	}
      } else if(op == "==") {
	if( age != age2) {
	  return retval;
	}
      } else if (op == "<") {
	if( age >= age2) {
	  return retval;
	}
      } else if(op == ">=") {
	if( age < age2) {
	  return retval;
	}
      } else if(op == "!=") {
	if( age == age2) {
	  return retval;
	}
      }
    } else if (key == "rxnorm" && op == "==") {
      for (auto const& element : input_map) {

	const auto& key = element.first;
	const auto pos = key.find("start_date");
	if (key != "start_date" && pos != std::string::npos && element.second != "") {
	  
	  const auto key2 = key.substr(pos+11); // truncate left
	  const auto pos2 = key2.find_last_of('_');
	  
	  const auto key3 = (pos2 == std::string::npos || !(std::all_of(std::begin(key2)+pos2+1, std::end(key2), [](char x){return std::isdigit(x);})))?key2:key2.substr(0, pos2);
	  
	  auto key4 =
	    (key.substr(0,5) == "mdctn")?
	    key3.substr(0, key3.find('_')):
	    (key.substr(0,3) == "icd")?
	    key3.substr(0,key3.find('.')):
	    key3;
	  
	  key4 = (std::all_of(std::begin(key4), std::end(key4), [](char x){return std::isdigit(x);})?"rxnorm:":"") + key4;
	  std::replace(std::begin(key4), std::end(key4), ' ', '_');

	  if (key4 != "rxnorm:" + value) {
	    if(key.substr(0,5) == "mdctn") {
	      std::cout << "found " << key4 << " not matched" << std::endl;
	    }
	    return retval;
	  } else {
	    std::cout << "found " << value << std::endl;
	  }
	}
      }
      
    }
  }

  retval.start_date = get(input_map, "start_date");
  retval.race_cd = get(input_map, "race_cd");
  retval.sex_cd = get(input_map, "sex_cd");
  retval.birth_date = get(input_map, "birth_date");
  retval.inout_cd = get(input_map, "inout_cd");
  
  for (auto const& element : input_map) {

    const auto& key = element.first;
    const auto pos = key.find("start_date");
    if (key != "start_date" && pos != std::string::npos && element.second != "") {

      const auto key2 = key.substr(pos+11); // truncate left
      const auto pos2 = key2.find_last_of('_');
      
      const auto key3 = (pos2 == std::string::npos || !(std::all_of(std::begin(key2)+pos2+1, std::end(key2), [](char x){return std::isdigit(x);})))?key2:key2.substr(0, pos2);
      
      auto key4 =
	(key.substr(0,5) == "mdctn")?
	key3.substr(0, key3.find('_')):
	(key.substr(0,3) == "icd")?
	key3.substr(0,key3.find('.')):
	key3;

      key4 = (std::all_of(std::begin(key4), std::end(key4), [](char x){return std::isdigit(x);})?"rxnorm:":"") + key4;
      std::replace(std::begin(key4), std::end(key4), ' ', '_');
      
      retval.codes.insert(Code(key4, element.second));
    } else if (key.substr(0,4) == "gene") {
      retval.genes.insert(key.substr(5));
    }
  }
  retval.patient_num = input_map.at("patient_num");
  return retval;
}



