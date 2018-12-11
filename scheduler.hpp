/*
  scheduler.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef scheduler_hpp
#define scheduler_hpp

#include <vector>
#include "types.hpp"

namespace GraphSN {
  
  class Scheduler{
    
    std::vector<bool> current_bitscheduler;
    std::vector<bool> next_bitscheduler;
    
  public:
    
    bool has_tasks;
    
    Scheduler(size_t size)
    {
      current_bitscheduler.resize(size,true);
      next_bitscheduler.resize(size,false);
      has_tasks = true;
    }
    
    
    ~Scheduler()
    {
      current_bitscheduler.clear();
      current_bitscheduler.shrink_to_fit();
      next_bitscheduler.clear();
      next_bitscheduler.shrink_to_fit();
    }
    
    void add_task(vertex_t id, bool current_iteration = false)
    {
      next_bitscheduler[id] = true;
      if (current_iteration){
        current_bitscheduler[id] = true;
      }
      has_tasks = true;
    }
    
    void clear(std::vector<bool>& scheduler, uint32_t fromID, uint32_t toID)
    {
      CHECK(fromID <= scheduler.size());
      CHECK(fromID <= toID);
      CHECK(toID   <= scheduler.size());
      std::fill(scheduler.begin() + fromID, scheduler.begin() + toID + 1, false);
    }
    
    void remove_task(vertex_t ID)
    {
      CHECK(ID < next_bitscheduler.size());
      next_bitscheduler[ID] = false;
    }
    
    /**
     * remove_tasks_in_range
     *
     * Removes tasks for vertices in [fromID,toID]
     *
     * @return  void
     */
    void remove_tasks_in_range(vertex_t fromID, vertex_t toID)
    {
      clear(next_bitscheduler, fromID, toID);
    }
    
    uint32_t get_tasks_num()
    {
      uint32_t tasks_num = 0;
      for (vertex_t i = 0; i <= current_bitscheduler.size(); i++){
        tasks_num += current_bitscheduler[i];
      }
      return tasks_num;
    }
    
    bool is_scheduled(vertex_t index)
    {
      return current_bitscheduler[index];
    }
    
    void swap()
    {
      current_bitscheduler.swap(next_bitscheduler);
      clear(next_bitscheduler, 0, (uint32_t) next_bitscheduler.size() - 1);
    }
    
    /* for debug purposes */
    void print_vectors()
    {
      for (uint32_t i = 0; i < current_bitscheduler.size(); i++){
        std::cout << current_bitscheduler[i] << " ";
      }
      std::cout << std::endl;
      for (uint32_t i = 0; i < next_bitscheduler.size(); i++){
        std::cout << next_bitscheduler[i] << " ";
      }
      std::cout << std::endl;
    }
    
  };
  
  
}

#endif /* scheduler_hpp */
