#include "import_df.hpp"
#include <iostream>
#include <tuple>
#include <vector>

int main(int argc, char** argv) {
  int i = 0;
  std::vector<std::tuple<std::string,std::string>> tablemetas;
  for(int i = 3;i<argc;i+=2) {
    tablemetas.push_back(std::make_tuple(argv[i], argv[i+1]));
  }
  
  load_df(
	  argv[1],
	  [&i](const callback_row &) {
	    std::cout << i++ << std::endl;
	    return false;
	  },
	  argv[2],
	  tablemetas
	  );
  return 0;
}
