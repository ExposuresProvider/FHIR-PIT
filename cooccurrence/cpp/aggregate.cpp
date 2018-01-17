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

int bin(const std::string&start_date) {
  return strtyear(start_date);
}


void calculate_aggregation(const std::vector<std::tuple<std::string, std::string, std::string>> &criteria, const std::string &filename, const std::string &filemeta, const std::vector<std::tuple<std::string, std::string>> &tablemetas, const std::set<std::string> &inout_cd_keys, const std::map<std::string, std::set<std::string>> &rxnorm_gene_map, const std::vector<int> &years, const std::string &filenamebase) {

  const std::string singletonfilename(filenamebase);
  int i = 0;

  std::map<std::string, std::map<int,std::map<std::string, int>>> binmap;
  
  load_df(
	  filename,
	  [&i, &binmap, &criteria, &inout_cd_keys, &rxnorm_gene_map, &years](const callback_row &row_) {
	    const auto features = extract_keys(row_, criteria);
	    const auto &patient_num = features.patient_num;
	    if(patient_num == "") {
	      return false;
	    }

	    i++;
	    std::cout << i << std::endl;
	    std::set<std::string> keys;
	    
	    const auto &codes = features.codes;

	    const auto &race_cd = features.race_cd;
	    const auto &sex_cd = features.sex_cd;
	    const auto &birth_date = features.birth_date;

	    for(const auto &code : codes) {
	      auto key = code.name;
	      const auto& start_date = code.start_date;
	      const auto b = bin(start_date);
	      keys.insert(key);

	      if(key.substr(0,7) == "rxnorm:") {
		std::string rxnorm = key.substr(7);
		key = "medname_" + rxnorm;
		const auto& genes = rxnorm_gene_map.at(rxnorm);
		for(const auto &gene : genes) {
		  binmap[patient_num][b]["genename_" + gene] = 1;
		}
	      }
	      binmap[patient_num][b][key] = 1;
	      if(race_cd != "") {
		binmap[patient_num][b]["race_" + race_cd] = 1;
	      }
	      if(sex_cd != "") {
		binmap[patient_num][b]["sex_" + sex_cd] = 1;
	      }
	      
	      if(birth_date != "") {
		binmap[patient_num][b]["age"] = b - strtyear(birth_date);
	      }
	    }

	    const auto &start_date = features.start_date;
	    if(start_date != "") {
	      const auto b = bin(start_date);
	      const auto &inout_cd = features.inout_cd;
	      if(inout_cd != "") {
		std::set<std::string> intersection;
		std::set_intersection(std::begin(inout_cd_keys), std::end(inout_cd_keys), std::begin(keys), std::end(keys), std::inserter(intersection, std::begin(intersection)));
		if(intersection.size() > 0) {
		  binmap[patient_num][b][inout_cd] ++;
		}
	      }
	    }


	  

	    return false;
	  },
	  filemeta,
	  tablemetas
	  );

  std::set<std::string> allkeys;
  std::map<std::string, int> singletonc;
  
  for(const auto &patient_num_keyvals_bins : binmap) {
    const auto& keyvals_bins = std::get<1>(patient_num_keyvals_bins);
    
    for(const auto &keyvals_bin : keyvals_bins) {
      const auto& keyvals = std::get<1>(keyvals_bin);

      for(const auto &keyval : keyvals) {
	const auto &key = std::get<0>(keyval);
	allkeys.insert(key);
      }
    }
  }

  std::vector<std::string> keys(std::begin(allkeys), std::end(allkeys));
  
  std::ofstream sf;
  sf.open(singletonfilename);
  sf << "patient_num!year";
  for(const auto &key : keys) {
    sf << "!" << key;
  }
  sf << std::endl;

  for(const auto &patient_num_keyvals_bins : binmap) {
    const auto& patient_num = std::get<0>(patient_num_keyvals_bins);
    const auto& keyvals_bins = std::get<1>(patient_num_keyvals_bins);
    
    for(const auto &keyvals_bin : keyvals_bins) {
      const auto& bin = std::get<0>(keyvals_bin);
      const auto& keyvals = std::get<1>(keyvals_bin);
      if(std::find(std::begin(years), std::end(years), bin) != std::end(years)) {

	sf << patient_num << "!" << bin;

	for(const auto &key : keys) {
	  const auto &valitr = keyvals.find(key);
	  sf << "!" << (valitr == keyvals.end() ? "" : std::to_string(valitr->second));
	}
	
	sf << std::endl;
      }
    }
  }
    
  sf.close();
  
}

int main(int argc, char** argv) {
  const int x0 = atoi(argv[1]); // number of criteria
  std::vector<std::tuple<std::string, std::string, std::string>> criteria;
  for(int i =2; i< 2 + x0 * 3; i+=3) {
    criteria.push_back(std::make_tuple(argv[i], argv[i+1], argv[i+2]));
  }
  int n = 2 + x0 * 3;

  const std::string inout_cd_keys_filename = argv[n++]; // inout_cd filter keys
  std::set<std::string> inout_cd_keys;
  std::ifstream iocdkeysinfile(inout_cd_keys_filename);
  std::string key;
  if(iocdkeysinfile.fail()) {
    std::cerr << "error reading " + inout_cd_keys_filename << std::endl;
  }
  while(iocdkeysinfile >> key) {
    inout_cd_keys.insert(key);
  }
  iocdkeysinfile.close();

  const std::string rxnorm_gene_map_filename = argv[n++]; // rxnorm to gene map
  std::map<std::string, std::set<std::string>> rxnorm_gene_map;
  std::ifstream infile(rxnorm_gene_map_filename);
  std::string rxnorm;
  std::string gene;
  if(infile.fail()) {
    std::cerr << "error reading " + rxnorm_gene_map_filename << std::endl;
  }
  while(infile >> rxnorm >> gene) {
    rxnorm_gene_map[rxnorm].insert(gene);
  }
  infile.close();

  const int nyears = atoi(argv[n++]); // years
  std::vector<int> years;
  for(int i=0;i<nyears;i++) {
    years.push_back(atoi(argv[n++]));
  }

  std::cout << argv[n] << std::endl;
  const std::string filenamebase(argv[n]); // file name base
  std::string filename(argv[n+1]); // input file name
  std::string filemeta(argv[n+2]); // file meta name
  std::vector<std::tuple<std::string,std::string>> tablemetas;
  for(int i = n+3;i<argc;i+=2) {
    tablemetas.push_back(std::make_tuple(argv[i], argv[i+1])); // col meta name
  }

  calculate_aggregation(criteria, filename, filemeta, tablemetas, inout_cd_keys, rxnorm_gene_map, years, filenamebase);

  return 0;
}
