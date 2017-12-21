
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <tuple>
#include <algorithm>
#include <stdexcept>
#include <cctype>
#include <functional>

std::vector<std::string> import_array(const std::string &filepath);

typedef std::map<std::string, std::string> callback_row;

void load_df(
	     const std::string &filepath,
	     const std::function<bool(const callback_row&)> callback,
	     const std::string &filemeta, //  = "endotype_meta.csv"
	     const std::vector<std::tuple<std::string, std::string>> &colmeta // [("icd", "icd_meta.csv"), ("mdctn", "mdctn_meta.csv"), ("loinc", "loinc_meta.csv")]
	     );

class Input {
public:
  Input( const std::string &buf, const int pos);

  const char & curr();
  
  void skip(const std::string & s);

  void next();

  bool eof();

  int getPos();

private:
  const std::string &buf;
  int pos;
};
  
std::vector<std::string> parse_array(const std::string &line);

callback_row parse_row(const std::string &line, const std::vector<std::tuple<bool, std::vector<std::string>>> &colnames);

void parse_entry(Input &inp, callback_row &row, const std::tuple<bool, std::vector<std::string>> &names);

std::string parse_unquoted_string(Input &inp);
        
std::tuple<std::vector<int>, std::vector<std::string>> parse_sparse_array(Input &inp);

std::vector<int> parse_indices(Input &inp);
    
std::vector<std::string> parse_elements(Input &inp);

std::string parse_while(Input &inp, std::function<bool(char)> cb);

int parse_int(Input &inp);

std::string parse_string2(Input &inp);

std::string parse_string4(Input &inp);
            
std::string parse_unquoted_string(Input &inp);

std::string parse_quoted_string(Input &inp);
        
void import_sparse(const std::vector<std::tuple<bool, std::vector<std::string>>> &colnames, const std::string &filepath, const std::function<bool(const callback_row&)> callback);

        
  
         


