/*
  connectedcomponents.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#include <iostream>
#include <string>

#include "timer.hpp"
#include "preprocessing.hpp"
#include "graph_engine.hpp"
#include "scheduler.hpp"
#include "metrics.hpp"

using namespace GraphSN;

value_t data_function(value_t data, value_t edge_data)
{
  SILENCE edge_data;
  return data;
}

class ExampleProgram : public GraphSNProgram{
  
  bool converged;

  void update(GraphVertex& vertex, GraphBox& graphbox) {
    graphbox.scheduler->remove_task(vertex.getID());
    
    if (graphbox.get_current_iteration() == 0) {
      vertex.setData(vertex.getID());
      graphbox.scheduler->add_task(vertex.getID());
    }
    
    vertex_t curmin = vertex.getData();
    for(uint32_t i = 0; i < vertex.num_edges(); i++) {
      vertex_t nblabel = vertex.edge(i).getVertex()->getData();
      if (graphbox.get_current_iteration() == 0){
        nblabel = vertex.edge(i).getID();
      }
      curmin = std::min(nblabel, curmin);
    }
    
    vertex.setData(curmin);
    vertex_t label = vertex.getData();
    
    if (graphbox.get_current_iteration() > 0) {
      for(uint32_t i = 0; i < vertex.num_edges(); i++) {
        if (label < vertex.edge(i).getVertex()->getData()) {
          vertex.edge(i).getVertex()->setData(label);
          /* Schedule neighbor for update */
          graphbox.scheduler->add_task(vertex.edge(i).getID(), true);
          converged = false;
        }
      }
    }
    else if (graphbox.get_current_iteration() == 0) {
      for(uint32_t i = 0; i < vertex.getOutdegree(); i++) {
        vertex.outedge(i).getVertex()->setData(label);
      }
    }
  }
  /**
   * Called before an iteration starts.
   */
  void before_iteration(GraphBox& graphbox) {
    println("Start of iteration %u/%u",graphbox.get_current_iteration(), graphbox.get_iterations_num() - 1);
    converged = graphbox.get_current_iteration() > 0;
  }
  
  /**
   * Called after an iteration has finished.
   */
  void after_iteration(GraphBox& graphbox) {
    if (converged) {
      println("Last iteration set!");
      graphbox.set_last_iteration(graphbox.get_current_iteration());
    }
    else{
      println("End of iteration %u/%u\n",graphbox.get_current_iteration(), graphbox.get_iterations_num() - 1);
    }
  }
  
  /**
   * Called before an execution interval is started.
   */
  void before_exec_interval(GraphBox& graphbox) {
    SILENCE(graphbox);
  }
  
  /**
   * Called after an execution interval has finished.
   */
  void after_exec_interval(GraphBox& graphbox) {
    SILENCE(graphbox);
  }
  
};

int main(int argc, const char * argv[]) {
  
  uint32_t iterations;
  uint64_t cache_size;
  ExampleProgram program;
  Engine * engine;
  Preprocessing hPreprocessing;
  std::string cache_type = "LRU";
  
  timer.start("Main execution");
  iterations = 1000;
  cache_size = 600L * 1024L * 1024L;

  GraphSNInit(argc, argv);
  hPreprocessing.CheckPreprocessing(inFolder);
  engine = new Engine();
  engine->run(program, iterations, cache_size, cache_type);
  timer.end("Main execution");

  timer.start("metrics");
  AnalyzeConnectedComponents(std::string(infile));
  timer.end("metrics");
  
  timer.print_timing_report();
  
  delete engine;
  return 0;
}
