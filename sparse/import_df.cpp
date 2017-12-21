
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <tuple>
#include <algorithm>
#include <stdexcept>
#include <cctype>
#include "import_df.hpp"
#include <functional>
#include <sstream>

std::vector<std::string> import_array(const std::string &filepath) {
  std::string line;

  std::ifstream f{filepath};
  std::getline(f, line);
  return parse_array(line);
}


typedef std::map<std::string, std::string> callback_row;

void load_df(
	     const std::string &filepath,
	     const std::function<bool(const callback_row&)> callback,
	     const std::string &filemeta, //  = "endotype_meta.csv"
	     const std::vector<std::tuple<std::string, std::string>> &colmeta // [("icd", "icd_meta.csv"), ("mdctn", "mdctn_meta.csv"), ("loinc", "loinc_meta.csv")]
	     ) {
  
  std::vector<std::tuple<std::string, std::vector<std::string>>> colnames_dict;
  
  std::transform(std::begin(colmeta), std::end(colmeta), std::back_inserter(colnames_dict), [](const std::tuple<std::string, std::string>& cm) {
      return std::make_tuple(std::get<0>(cm), import_array(std::get<1>(cm)));
    });
  
  auto cols = import_array(filemeta);

  std::vector<std::tuple<bool, std::vector<std::string>>> cols2;

  std::transform(std::begin(cols), std::end(cols), std::back_inserter(cols2), [&colnames_dict](std::string &x) {
      auto i = std::find_if(std::begin(colnames_dict), std::end(colnames_dict), [&x](const std::tuple<std::string, std::vector<std::string>> &cd) {
	  auto &key = std::get<0>(cd);
	  return x.substr(0, key.size()) == key;
	});
      
      if (i == std::end(colnames_dict)) {
	return std::make_tuple(false, std::vector<std::string>{x});
      } else {
	auto & colnames = std::get<1>(*i);
	std::vector<std::string> colnames2;
	
	std::transform(std::begin(colnames), std::end(colnames), std::back_inserter(colnames2), [&x](std::string &y) {
	    return x + "_" + y;
	  });
	return std::make_tuple(true, colnames2);
      }

    });
  
  return import_sparse(cols2, filepath, callback);
}


Input::Input( const std::string &buf, const int pos): buf(buf), pos(pos) {}

  const char & Input::curr() {
    return buf[pos];
  }

  void Input::skip(const std::string & s){
    for (int j = 0; j < s.size(); j++) {
      if (curr() != s[j]) {
	throw std::runtime_error(std::string("error: expected ") + s[j] + " found " + curr() + " at " + std::to_string(pos));
      }
      next();
    }
  }
  void Input::next() {
    pos ++;
  }
  bool Input::eof() {
    return pos == buf.size();
  }
  int Input::getPos() {
    return pos;
  }

  
std::vector<std::string> parse_array(const std::string &line) {
  std::vector<std::string> row;
  Input inp(line, 0);
  if (inp.curr() == '{') {
    inp.next();
  }
  
  while (true) {
    auto s = parse_unquoted_string(inp);
    row.push_back(s);
    if (inp.eof() || inp.curr() == '}') {
      break;
    }
    
    if(inp.curr() == '\\') {
      inp.skip("\\,");
    } 
    else {
      inp.skip(",");
    }
  }
  if (! inp.eof() && inp.curr() == '}') {
    inp.next();
  }
  
  if (! inp.eof() && inp.curr() == '\n') {
    inp.next();
  }
    
  if (! inp.eof()) {
    throw std::runtime_error(std::string("error: expected oef found ") + inp.curr() + " at " + std::to_string(inp.getPos()));
  }
        
  return row;
}

callback_row parse_row(const std::string &line, const std::vector<std::tuple<bool, std::vector<std::string>>> &colnames) {
  callback_row row;
  int col = 0;
  Input inp(line, 0);
  while(true) {
    parse_entry(inp, row, colnames[col]);
    if (inp.eof()) {
      break;
    }
    inp.skip(",");
    col++;
  }
  return row;
}

void parse_entry(Input &inp, callback_row &row, const std::tuple<bool, std::vector<std::string>> &colnames) {
  auto& names = std::get<1>(colnames);
  if (std::get<0>(colnames)) {

    if (inp.curr() == '\"') {
      inp.skip("\"");
      auto entry = parse_sparse_array(inp);
      auto &indices = std::get<0>(entry);
      auto &elements = std::get<1>(entry);
      for (int i = 0; i< indices.size(); i++) {
	row[names[indices[i]]] = elements[i];
      }
      inp.skip("\"");
    }
  }
  else {
    auto string = parse_unquoted_string(inp);
    row[names[0]] = string;
  }
}
        

std::tuple<std::vector<int>, std::vector<std::string>> parse_sparse_array(Input &inp) {
  inp.skip("(");
  auto indices = parse_indices(inp);
  inp.skip(",");
  auto  elements = parse_elements(inp);
  inp.skip(")");
  return std::make_tuple(indices, elements);
}

std::vector<int> parse_indices(Input &inp){
  std::vector<int> indices;
  if (inp.curr() == '\"') {
    inp.skip("\"\"{");
    while (inp.curr() != '}') {
      auto n = parse_int(inp);
      indices.push_back(n);
      if(inp.curr() == ',') {
	inp.next();
      }
    }
    inp.skip("}\"\"");
  }
  else {
    inp.skip("{");
    while (inp.curr() != '}') {
      auto n = parse_int(inp);
      indices.push_back(n);
      if (inp.curr() == ',') {
	inp.next();
      }
    }
    inp.skip("}");
  }

  return indices;
}
    
std::vector<std::string> parse_elements(Input &inp) {
  std::vector<std::string> elements;
  if (inp.curr() == '\"') {
    inp.skip("\"\"{");
    while (inp.curr() != '}') {
      auto n = parse_string4(inp);
      elements.push_back(n);
      if (inp.curr() == ',') {
	inp.next();
      }
    }
    inp.skip("}\"\"");
  }
  else {
    inp.skip("{");
    while (inp.curr() != '}') {
      auto n = parse_string2(inp);
      elements.push_back(n);
      if (inp.curr() == ',') {
	inp.next();
      }
    }
    inp.skip("}");
  }
  return elements;
}

std::string parse_while(Input &inp, std::function<bool(char)> cb) {
  std::stringstream s;
  while (! inp.eof() && cb(inp.curr())) {
    s << inp.curr();
    inp.next();
  }
  return s.str();
}

int parse_int(Input &inp) {
  return atoi(parse_while(inp, isdigit).c_str());
}

std::string parse_string2(Input &inp) {
  std::string s;
  if (inp.curr() == '\"') {
    inp.skip("\"\"");
    s = parse_quoted_string(inp);
    inp.skip("\"\"");
  } else {
    s = parse_unquoted_string(inp);
  }
  return s;
}

std::string parse_string4(Input &inp) {
  std::string s;
  if (inp.curr() == '\"') {
    inp.skip("\"\"\"\"");
    s = parse_quoted_string(inp);
    inp.skip("\"\"\"\"");
  } else {
    s = parse_unquoted_string(inp);
  }
  return s;
}
            
std::string parse_unquoted_string(Input &inp) {
  return parse_while(inp, [](char ch) {
      return std::string("(){}\\,\"").find(ch) == std::string::npos;
    });
}


std::string parse_quoted_string(Input &inp) {
  return parse_while(inp, [](char ch) {
      return ch != '\"';
    });
}
 
        
void import_sparse(const std::vector<std::tuple<bool, std::vector<std::string>>> &colnames, const std::string &filepath, const std::function<bool(const callback_row&)> callback) {
  std::ifstream f(filepath);
  std::string line;
  while(!f.eof()) {
    std::getline(f, line);
    if(callback(parse_row(line, colnames)))
      return;
  }
}
        
  
         


