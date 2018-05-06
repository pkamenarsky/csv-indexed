#include "fast-cpp-csv-parser/csv.h"

#include <forward_list>
#include <experimental/optional>
#include <unordered_map>
#include <iostream>

using namespace std;

extern "C" {
  struct API {
    unsigned int indexes_count;
    unsigned int sorted_indexes_count;

    std::unordered_map<std::string, std::forward_list<int>> *indexes;
    std::unordered_map<std::string, std::pair<int, int>*> *sorted_indexes;
    std::unordered_map<unsigned int, std::experimental::optional<unsigned int>> column_to_index;
    std::unordered_map<unsigned int, std::experimental::optional<unsigned int>> column_to_sorted_index;
    std::string *cols;
  };

  API *makeIndexes(
      const char *filename,
      int column_count,
      unsigned int *indexes,
      unsigned int indexes_count,
      unsigned int *sorted_indexes,
      unsigned int sorted_indexes_count) {
    unsigned int line = 0, offset = 0, length = 0;
    io::CSVReaderDyn<io::trim_chars<' '>, io::double_quote_escape<',', '\"'>> in(column_count, filename);

    API *api = new API();

    api->indexes_count = indexes_count;
    api->sorted_indexes_count = sorted_indexes_count;
    api->indexes = new std::unordered_map<std::string, std::forward_list<int>>[indexes_count];
    api->sorted_indexes = new std::unordered_map<std::string, std::pair<int, int>*>[sorted_indexes_count];
    api->cols = new std::string[column_count];

    for (unsigned int i = 0; i < indexes_count; i++) {
      api->column_to_index[indexes[i]] = std::experimental::optional<unsigned int>(i);
    }

    for (unsigned int i = 0; i < sorted_indexes_count; i++) {
      api->column_to_sorted_index[sorted_indexes[i]] = std::experimental::optional<unsigned int>(i);
    }

    while (in.read_row(length, api->cols)){
      for (int i = 0; i < indexes_count; i++) {
        api->indexes[i][api->cols[indexes[i]]].push_front(length);
        api->indexes[i][api->cols[indexes[i]]].push_front(offset);
      }

      for (int i = 0; i < sorted_indexes_count; i++) {
        std::pair<int, int>* current = api->sorted_indexes[i][api->cols[sorted_indexes[i]]];

        if (current == nullptr) {
          current = new std::pair<int, int>(offset, length + 1);
          api->sorted_indexes[i][api->cols[sorted_indexes[i]]] = current;
        }
        else {
          current->second += length + 1;
        }
      }

      offset += length + 1;
      line++;
    }

    return api;
  }

  void freeIndexes(API *api) {
    delete [] api->indexes;
    delete [] api->cols;

    for (int i = 0; i < api->sorted_indexes_count; i++) {
      for (auto pair : api->sorted_indexes[i]) {
        delete pair.second;
      }
    }

    delete [] api->sorted_indexes;
    delete api;
  }

  int *getLinesForIndex(API *api, unsigned int index, const char *value) {
    std::experimental::optional<unsigned int> column = api->column_to_index[index];

    std::cout << index << " " << value << "\n";

    if (column) {
      std::forward_list<int> list = api->indexes[*column][value];

      int length = std::distance(std::begin(list), std::end(list));
      int *result = new int[length + 1];
      int i = 1;

      result[0] = length + 1;

      for (auto e: list) {
        result[i++] = e;
      }

      return result;
    }
    else {
      column = api->column_to_sorted_index[index];

      if (column) {
        std::pair<int, int>* id = api->sorted_indexes[*column][value];

        if (id != nullptr) {
          return new int[3]{3, id->first, id->second};
        }
        else {
          return new int[1]{1};
        }
      }
      else {
        return new int[1]{1};
      }
    }
  }

  void freeResult(int *result) {
    delete [] result;
  }
}
