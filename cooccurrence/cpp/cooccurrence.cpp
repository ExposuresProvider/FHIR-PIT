#include "import_df.hpp"
#include <iostream>
#include <tuple>
#include <vector>
#include <set>
#include <algorithm>
#include <cctype>
#include <time.h>
#include <cstdlib>
#include <fstream>

std::tuple<std::string, std::set<std::tuple<std::string, std::string>>> extract_keys(const std::map<std::string, std::string> & input_map) {
  std::set<std::tuple<std::string, std::string>> retval;
  
  for (auto const& element : input_map) {

    const auto& key = element.first;
    const auto pos = key.find("start_date");
    if (key != "start_date" && pos != std::string::npos && element.second != "") {

      const auto key2 = key.substr(pos+11); // truncate left
      const auto pos2 = key2.find_last_of('_');
      
      const auto key3 = (pos2 == std::string::npos || !(std::all_of(std::begin(key2)+pos2+1, std::end(key2), [](char x){return std::isdigit(x);})))?key2:key2.substr(0, pos2);
      
      auto key4 =
	(key.substr(0,5) == "mdctn")?
	(std::all_of(std::begin(key3), std::end(key3), [](char x){return std::isdigit(x);})?"rxnorm:":"") + key3.substr(0, key3.find('_')):
	(key.substr(0,3) == "icd")?
	key3.substr(0,key3.find('.')):
	key3;

      std::replace(std::begin(key4), std::end(key4), ' ', '_');
      
      retval.insert(std::make_tuple(key4, element.second));
    }
  }
  const std::string &patient_num = input_map.at("patient_num");
  return std::make_tuple(patient_num, retval);
}

int bin(int w, const std::string&start_date) {
  struct tm tm;
  strptime(start_date.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  const long seconds = mktime(&tm);
  const long days = seconds / (60*60*24);
  const long r = rand() % w; 
  return (days + r)/w;
}

template<typename K, typename V>
std::map<K,V>& operator+=(std::map<K,V> &a, const std::map<K,V> &b) {
  for(const auto& bkv : b) {
    const auto &bk = std::get<0>(bkv);
    const auto &bv = std::get<1>(bkv);

    a[bk] += bv;
    
  }
  return a;
}

void calculate_cooccurrences(const std::string &filename, const std::string &filemeta, const std::vector<std::tuple<std::string, std::string>> &tablemetas, int w, const std::string &filenamebase) {

  const std::string singletonfilename(filenamebase + "_singleton" + std::to_string(w) + "perbin");
  const std::string cooccurrencefilename(filenamebase + "_cooccurrence" + std::to_string(w) + "perbin");
  const std::string singletonperpatfilename(filenamebase + "_singleton" + std::to_string(w) + "perpat");
  const std::string cooccurrenceperpatfilename(filenamebase + "_cooccurrence" + std::to_string(w) + "perpat");
  int i = 0;

  std::cout << "w = " << w << std::endl;
  std::map<std::string, std::map<int,std::set<std::string>>> binmap;
  
  load_df(
	  filename,
	  [&i, &binmap, &w](const callback_row &row_) {
	    const auto patient_num_keys_start_date = extract_keys(row_);
	    const auto &patient_num = std::get<0>(patient_num_keys_start_date);
	    const auto &keys_start_date = std::get<1>(patient_num_keys_start_date);

	    i++;
	    //	    std::cout << i << std::endl;
	    
	    for(const auto &key_start_date : keys_start_date) {
	      const auto& key = std::get<0>(key_start_date);
	      const auto& start_date = std::get<1>(key_start_date);
	      const auto b = bin(w, start_date);
	      
	      auto& patientmap = binmap[patient_num];
	      patientmap[b].insert(key);
	    }
	    
	    return false;
	  },
	  filemeta,
	  tablemetas
	  );

  std::map<std::tuple<std::string, std::string>, int> cooc;
  std::map<std::string, int> singletonc;
  std::map<std::tuple<std::string, std::string>, int> coocperpat;
  std::map<std::string, int> singletoncperpat;
  
  for(const auto &patient_num_key_bin : binmap) {
    const auto& patient_num = std::get<0>(patient_num_key_bin);
    const auto& key_bin = std::get<1>(patient_num_key_bin);
    std::map<std::tuple<std::string, std::string>, int> coocpat;
    std::map<std::string, int> singletoncpat;
    
    for(const auto &key_bin : key_bin) {
      const auto& keys0 = std::get<1>(key_bin);
      const auto& keys = std::vector<std::string>(std::begin(keys0), std::end(keys0));
      for(int i = 0; i < keys.size(); i++) {
	const auto &key = keys[i];
	singletonc[key] ++;

	singletoncpat[key] = 1;
	for(int j = i + 1; j < keys.size(); j++){
	  const auto &key2 = keys[j];
	  
	  const auto keypb = std::make_tuple(key, key2);
	  
	  cooc[keypb] ++;
	    
	  coocpat[keypb] = 1;
	  
	}
      }
    }
    singletoncperpat += singletoncpat;
    coocperpat += coocpat;
  }

  std::ofstream sf;
  sf.open(singletonfilename);
  for(const auto &keysc : singletonc) {
    const auto& key = std::get<0>(keysc);
    sf << key << " " << std::to_string(std::get<1>(keysc)) << std::endl;
  }
  sf.close();
  
  std::ofstream cf;
  cf.open(cooccurrencefilename);
  for(const auto &keyoc : cooc) {
    const auto& key = std::get<0>(keyoc);
    cf << std::get<0>(key) << " " << std::get<1>(key) << " " << std::to_string(std::get<1>(keyoc)) << std::endl;
  }
  cf.close();

  std::ofstream sfpp;
  sfpp.open(singletonperpatfilename);
  for(const auto &keysc : singletoncperpat) {
    const auto& key = std::get<0>(keysc);
    sfpp << key << " " << std::to_string(std::get<1>(keysc)) << std::endl;
  }
  sfpp.close();
  
  std::ofstream cfpp;
  cfpp.open(cooccurrenceperpatfilename);
  for(const auto &keyoc : coocperpat) {
    const auto& key = std::get<0>(keyoc);
    cfpp << std::get<0>(key) << " " << std::get<1>(key) << " " << std::to_string(std::get<1>(keyoc)) << std::endl;
  }
  cfpp.close();
  
}

int main(int argc, char** argv) {
  const int x = atoi(argv[1]);
  std::vector<int> granularities;
  for(int i = 2; i< 2 + x; i++) {
    granularities.push_back(atoi(argv[i]));
  }
  const int n = x + 2;
  std::cout << argv[n] << std::endl;
  const std::string filenamebase(argv[n]);
  std::string filename(argv[n+1]);
  std::string filemeta(argv[n+2]);
  std::vector<std::tuple<std::string,std::string>> tablemetas;
  for(int i = n+3;i<argc;i+=2) {
    tablemetas.push_back(std::make_tuple(argv[i], argv[i+1]));
  }

  for(const auto i : granularities) {
    calculate_cooccurrences(filename, filemeta, tablemetas, i, filenamebase);
  }
  return 0;
}
