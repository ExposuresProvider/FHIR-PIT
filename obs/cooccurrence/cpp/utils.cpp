#include "import_df.hpp"
#include "utils.hpp"

long strtdays(const std::string&start_date) {
  struct tm tm;
  strptime(start_date.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  const long seconds = mktime(&tm);
  const long days = seconds / (60*60*24);
  return days;
}

long strtyear(const std::string&start_date) {
  struct tm tm;
  strptime(start_date.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  return tm.tm_year + 1900;
}

Bool &operator+=(Bool &a, const Bool &b) {
  if(!a) {
    a=b;
  }
  return a;
}

