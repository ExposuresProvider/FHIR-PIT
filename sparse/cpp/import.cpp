#include "import_df.hpp"
#include <iostream>
#include <tuple>

int main(int argc, char** argv) {
  int i = 0;
  load_df(
	  argv[1],
	  [&i](const callback_row &) {
	    std::cout << i++ << std::endl;
	    return false;
	  },
	  "endotype_meta.csv",
	  {std::make_tuple("mdctn","mdctn_meta.csv"),std::make_tuple("loinc","loinc_meta.csv"),std::make_tuple("icd","icd_meta.csv")}
	  );
  return 0;
}
