/*
  shortestdistance.hpp
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
#include <mutex>

using namespace GraphSN;

uint32_t * values;
value_t distance;
vertex_t startID = 1, endID = 364; /* 828323->772159 */
std::mutex mtx;

class ShortestDistance : public GraphSNProgram{
  
  bool end = false;
  
  void update(GraphVertex& vertex, GraphBox& graphbox) {
    graphbox.scheduler->remove_task(vertex.getID());
    
    if (graphbox.get_current_iteration() == 0){
      if (vertex.getID() == startID || vertex.getID() == endID){
        vertex.setData(vertex.getID());
        graphbox.scheduler->add_task(vertex.getID());
      }
      else{
        vertex.setData(-1);
      }
      return;
    }
    
    value_t label = vertex.getData();
    uint32_t value = values[vertex.getID()];
    
    for (uint32_t i = 0; i < vertex.num_edges(); i++){
      value_t data = vertex.edge(i).getVertex()->getData();
      if (data == -1){
        values[vertex.edge(i).getID()] = value + 1;
        vertex.edge(i).getVertex()->setData(label);
        graphbox.scheduler->add_task(vertex.edge(i).getID());
      }
      else if (data != label){
        end = true;
        mtx.lock();
        if (distance == -1){
          distance = value + values[vertex.edge(i).getID()] + 1;
        }
        mtx.unlock();
      }
    }

  }
  /**
   * Called before an iteration starts.
   */
  void before_iteration(GraphBox& graphbox) {
    println("Start of iteration %u/%u",graphbox.get_current_iteration(), graphbox.get_iterations_num() - 1);
    if (graphbox.get_current_iteration() == 0){
      values = (uint32_t *) calloc(vertices_number, sizeof(uint32_t));
      distance = -1;
    }
  }
  
  /**
   * Called after an iteration has finished.
   */
  void after_iteration(GraphBox& graphbox) {
    if (end) {
      println("Last iteration set!");
      graphbox.set_last_iteration(graphbox.get_current_iteration());
      free(values);
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
  
  std::cout << "Shortest distance" << std::endl;
  ShortestDistance program;
  Engine * engine;
  Preprocessing hPreprocessing;
  std::string cache_type = "Indegree";
  
  timer.start("Main execution");
  iterations = 1000;
  cache_size = 200L * 1024L * 1024L;
  
  GraphSNInit(argc, argv);
  hPreprocessing.CheckPreprocessing(inFolder);
  engine = new Engine();
  engine->run(program, iterations, cache_size, cache_type);
  timer.end("Main execution");
  
  if (distance == -1){
    LOG("There is no path between %u and %u\n",startID, endID);
  }
  else{
    LOG("Shortest path between %u and %u is %f\n",startID, endID, distance);
  }
  timer.print_timing_report();
  
  delete engine;
  return 0;
}
