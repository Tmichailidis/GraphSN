/*
  metrics.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef metrics_hpp
#define metrics_hpp

#include <algorithm>

#include "types.hpp"
#include "files.hpp"

namespace GraphSN
{
  
  typedef struct ConnectedComponents_s
  {
    uint32_t label;
    uint32_t size;
    
    ConnectedComponents_s(uint32_t label, uint32_t size): label(label), size(size){}
  }ConnectedComponents_t;
  
  bool compareBySize(const ConnectedComponents_t &a, const ConnectedComponents_t &b)
  {
    return a.size > b.size;
  }
  
  void AnalyzeConnectedComponents(std::string filename)
  {
    int32_t fd;
    uint32_t counter;
    value_t previous_label;
    value_t * vertices_data_arr;
    std::string metrics_filename;
    std::vector <ConnectedComponents_t> connected_components_vec;
    
    metrics_filename = filename + ".metrics";
    fd = open((inFolder + "vertex_data").c_str(), O_RDONLY);
    vertices_data_arr = (value_t *) malloc(vertices_number * sizeof(value_t));
    read_sys(reinterpret_cast<char*> (&vertices_data_arr[0]), vertices_number * sizeof(value_t), fd);
    
    std::sort(vertices_data_arr, vertices_data_arr + vertices_number, std::greater<double>());
    
    counter = 1;
    previous_label = vertices_data_arr[0];
    for (uint32_t i = 1; i < vertices_number; i++){
      if (vertices_data_arr[i] == previous_label){
        counter++;
        continue;
      }
      if (counter == 1){
        previous_label = vertices_data_arr[i];
        continue;
      }
      connected_components_vec.insert(connected_components_vec.begin(), ConnectedComponents_t(previous_label,counter));
      previous_label = vertices_data_arr[i];
      counter = 1;
    }
    if (counter != 1){
      connected_components_vec.insert(connected_components_vec.begin(), ConnectedComponents_t(previous_label,counter));
    }
    
    std::sort(connected_components_vec.begin(), connected_components_vec.end(), compareBySize);
    
    println("\nNumber of different Connected Components: %lu",connected_components_vec.size());
    
    for (int32_t i = 0; i < std::min(10, (int32_t) connected_components_vec.size());i++){
      println("Label [%u]: %u nodes", connected_components_vec[i].label, connected_components_vec[i].size);
    }
    
    free(vertices_data_arr);
    connected_components_vec.shrink_to_fit();
    connected_components_vec.clear();
  }
}
#endif /* metrics_hpp */
