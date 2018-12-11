/*
  graph_program.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#include "graph_objects.hpp"

namespace GraphSN {
  
  class GraphSNProgram {
    
  public:
    
    virtual ~GraphSNProgram() {}
    
    /**
     * Called before an iteration starts.
     */
    virtual void before_iteration(GraphBox& graphbox) = 0;
    /**
     * Called after an iteration has finished.
     */
    virtual void after_iteration(GraphBox& graphbox) = 0;
    
    
    /**
     * Called before an execution interval is started.
     */
    virtual void before_exec_interval(GraphBox& graphbox) = 0;
    
    /**
     * Called after an execution interval has finished.
     */
    virtual void after_exec_interval(GraphBox& graphbox) = 0;
    
    /**
     * Update function.
     */
    virtual void update(GraphVertex& v, GraphBox& graphbox) = 0;
  };
}
