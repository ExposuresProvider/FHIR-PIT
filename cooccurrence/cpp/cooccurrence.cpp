#include "import_df.hpp"
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
#include <fstream>

int bin(int w, const std::string&start_date) {
  const long days = strtdays(start_date);
  const long r = rand() % w; 
  return (days + r)/w;
}

void calculate_cooccurrences(const std::vector<std::tuple<std::string, std::string, std::string>> &criteria, const std::string &filename, const std::string &filemeta, const std::vector<std::tuple<std::string, std::string>> &tablemetas, int w, const std::string &filenamebase) {

  const std::string singletonfilename(filenamebase + "_singleton" + std::to_string(w) + "perbin");
  const std::string cooccurrencefilename(filenamebase + "_cooccurrence" + std::to_string(w) + "perbin");
  const std::string singletonperpatfilename(filenamebase + "_singleton" + std::to_string(w) + "perpat");
  const std::string cooccurrenceperpatfilename(filenamebase + "_cooccurrence" + std::to_string(w) + "perpat");
  int i = 0;

  std::cout << "w = " << w << std::endl;
  std::map<std::string, std::map<int,std::set<std::string>>> binmap;
  
  load_df(
	  filename,
	  [&i, &binmap, &w, &criteria](const callback_row &row_) {
	    const auto features = extract_keys(row_, criteria);
	    const auto &patient_num = features.patient_num;
	    if(patient_num == "") {
	      return false;
	    }
	    const auto &codes = features.codes;

	    i++;
	    //	    std::cout << i << std::endl;
	    
	    for(const auto &code : codes) {
	      const auto& key = code.name;
	      const auto& start_date = code.start_date;
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
  
  for(const auto &patient_num_key_bins : binmap) {
    const auto& patient_num = std::get<0>(patient_num_key_bins);
    const auto& key_bins = std::get<1>(patient_num_key_bins);
    std::map<std::tuple<std::string, std::string>, int> coocpat;
    std::map<std::string, int> singletoncpat;
    
    for(const auto &key_bin : key_bins) {
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
  const int x0 = atoi(argv[1]); // number of criteria
  std::vector<std::tuple<std::string, std::string, std::string>> criteria;
  for(int i =2; i< 2 + x0 * 3; i+=3) {
    criteria.push_back(std::make_tuple(argv[i], argv[i+1], argv[i+2]));
  }
  const int n0 = 2 + x0 * 3;
  
  const int x = atoi(argv[n0]); // number of granularities

  std::vector<int> granularities;
  
  for(int i = n0+1; i< n0 + 1 + x; i++) {
    granularities.push_back(atoi(argv[i])); // bin size
  }
  const int n = n0 + 1 + x;
  std::cout << argv[n] << std::endl;
  const std::string filenamebase(argv[n]); // file name base
  std::string filename(argv[n+1]); // input file name
  std::string filemeta(argv[n+2]); // file meta name
  std::vector<std::tuple<std::string,std::string>> tablemetas;
  for(int i = n+3;i<argc;i+=2) {
    tablemetas.push_back(std::make_tuple(argv[i], argv[i+1])); // col meta name
  }

  for(const auto i : granularities) {
    calculate_cooccurrences(criteria, filename, filemeta, tablemetas, i, filenamebase);
  }
  return 0;
}
