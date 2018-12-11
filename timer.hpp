/*
  timer.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef timer_hpp
#define timer_hpp

#include <map>
#include <iostream>
#include <string>
#include <chrono>

#include "log.hpp"

typedef std::chrono::time_point<std::chrono::system_clock> time_point;
typedef struct TimePoint_s{
  time_point start, end;
}TimePoint_t;

namespace GraphSN {
  
  class Timer{
    
  public:
    
    std::map <const char* ,TimePoint_t> TimerMap;
    
    ~Timer()
    {
      TimerMap.clear();
    }
    
    void start(const char * keyDescription)
    {
      TimerMap[keyDescription].start = std::chrono::system_clock::now();
    }
    
    void end(const char * keyDescription)
    {
      auto index = TimerMap.find(keyDescription);
      CHECK(index != TimerMap.end()) //checking if we have a start for this timing
      index->second.end = std::chrono::system_clock::now();
    }
    
    void print_timing_report()
    {
      println("\n============  TIMING REPORT ============ \n");
      for (auto map_element : TimerMap){
        std::chrono::duration<float> duration_in_seconds;
        duration_in_seconds = map_element.second.end - map_element.second.start;
        print("%-30s %-12.5f seconds\n",map_element.first, duration_in_seconds.count());
      }
      println("\n======================================== \n");
    }
    
  };
}

#endif /* timer_hpp */
