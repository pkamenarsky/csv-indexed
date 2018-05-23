#ifndef CSV_DYN_H
#define CSV_DYN_H

#define private public
#include "fast-cpp-csv-parser/csv.h"
#undef private

namespace io{
  template<class trim_policy = trim_chars<' ', '\t'>,
    class quote_policy = no_quote_escape<','>,
    class overflow_policy = throw_on_overflow,
    class comment_policy = no_comment
      >
      class CSVReaderDyn{
        private:
          LineReader in;
          unsigned int column_count;

          char** row;
          std::string *column_names;

          std::vector<int>col_order;

          template<class ...ColNames>
            void set_column_names(std::string s, ColNames...cols){
              column_names[column_count-sizeof...(ColNames)-1] = std::move(s);
              set_column_names(std::forward<ColNames>(cols)...);
            }

          void set_column_names(){}


        public:
          CSVReaderDyn() = delete;
          CSVReaderDyn(const CSVReaderDyn&) = delete;
          CSVReaderDyn&operator=(const CSVReaderDyn&);

          template<class ...Args>
            explicit CSVReaderDyn(unsigned int column_count, Args&&...args):in(std::forward<Args>(args)...), column_count(column_count) {
              row = new char*[column_count];
              column_names = new std::string[column_count];
              std::fill(row, row+column_count, nullptr);
              col_order.resize(column_count);
              for(unsigned i=0; i<column_count; ++i)
                col_order[i] = i;
              for(unsigned i=1; i<=column_count; ++i)
                column_names[i-1] = "col"+std::to_string(i);
            }

        private:
          void parse_helper(std::size_t){}

          void parse_helper(std::string *cols){
            for (int r = 0; r < column_count; r++) {
              try{
                try{
                  ::io::detail::parse<overflow_policy>(row[r], cols[r]);
                }catch(error::with_column_content&err){
                  err.set_column_content(row[r]);
                  throw;
                }
              }catch(error::with_column_name&err){
                err.set_column_name(column_names[r].c_str());
                throw;
              }
            }
          }


        public:
          bool read_row(uint64_t &length, std::string *cols){
            try{
              try{

                char*line;
                do{
                  line = in.next_line();
                  length = in.data_begin - (line - in.buffer.get()) - 1;
                  if(!line)
                    return false;
                }while(comment_policy::is_comment(line));

                detail::parse_line<trim_policy, quote_policy>
                  (line, row, col_order);

                parse_helper(cols);
              }catch(error::with_file_name&err){
                err.set_file_name(in.get_truncated_file_name());
                throw;
              }
            }catch(error::with_file_line&err){
              err.set_file_line(in.get_file_line());
              throw;
            }

            return true;
          }
      };
}
#endif

