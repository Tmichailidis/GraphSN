/*
  memoryshard.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef memoryshard_hpp
#define memoryshard_hpp

#include "cache.hpp"

namespace GraphSN {
  
  class Memoryshard{
    
    bool            keep_vertices_in_memory;
    int32_t         fd_edges, edges_read;
    int16_t         memshardID;
    value_t *       edge_data_arr;
    vertex_t *      adj_shard_arr;
    DegreeData_t *  inbound_degrees_arr;
    uint32_t        mem_destinations, inbound_edges_read;
    vertex_t        first_vid, last_vid;
    Outbound_t **   outbound_indices_arr;
    Cache *         hCache;
    GraphVertex *   vertices;
    
    /**
     * load_edges
     *
     * Load in-edges and internal out-edges from memoryshard
     *
     * @return  void
     */
    void load_edges()
    {
      int32_t fd;
      uint32_t number_of_vertices;
      std::string inbound_degrees_filename = inFolder + "inbound_degrees";
      std::string outbound_filename = inFolder + "Outbound/outbound_indices_";
      std::string memshard_filename = shard_filename + std::to_string(memshardID);
      std::string memshard_edata_filename = std::string(edge_data_filename + std::to_string(memshardID));
      
      if (!keep_vertices_in_memory){
        /* read inbound degrees */
        fd = open(inbound_degrees_filename.c_str(), O_RDONLY);
        if (fd == -1) handle_error(inbound_degrees_filename.c_str());
        
        inbound_degrees_arr = (DegreeData_t *) realloc(inbound_degrees_arr, mem_destinations * sizeof(DegreeData_t));
        pread_sys(reinterpret_cast<char*>(&inbound_degrees_arr[0]), mem_destinations * sizeof(DegreeData_t),
                  inbound_edges_read * sizeof(DegreeData_t), fd);
        close(fd);
        
        /* allocate memory for inbound edges */
        for (uint32_t i = 0; i < mem_destinations; i++){
          vertices[inbound_degrees_arr[i].vID].allocateInedges(inbound_degrees_arr[i].degree);
        }
        /* load shard (destinations) */
        adj_shard_arr = (vertex_t *) realloc(adj_shard_arr, edges_read * sizeof(vertex_t));
        fd = open(memshard_filename.c_str(), O_RDONLY);
        if (fd == -1) handle_error(("opening "+ memshard_filename).c_str());
        read_sys(reinterpret_cast<char*>(&adj_shard_arr[0]), edges_read * sizeof(vertex_t), fd);
        close(fd);
      }
      else{
        for (uint32_t i = inbound_edges_read; i < inbound_edges_read + mem_destinations; i++){
          vertices[inbound_degrees_arr[i].vID].allocateInedges(inbound_degrees_arr[i].degree);
        }
      }
      inbound_edges_read += mem_destinations;
      /* load edges' data */
      edge_data_arr = (value_t *) realloc(edge_data_arr, edges_read * sizeof(value_t));
      if (hCache->noCacheMode()){
        fd_edges = open(memshard_edata_filename.c_str(), O_RDWR, 0777);
        if (fd_edges == -1) handle_error(("opening "+ memshard_edata_filename).c_str());
        read_sys(reinterpret_cast<char*>(&edge_data_arr[0]), edges_read * sizeof(value_t), fd_edges);
        close(fd_edges);
      }
      else{
        hCache->search_and_retrieve(memshardID, 0, edges_read * sizeof(value_t), edge_data_arr);
      }
      
      number_of_vertices = GetElementsNumber(outbound_filename+std::to_string(memshardID)+".binary", sizeof(Outbound_t));
      
#pragma omp parallel for
      for (uint32_t i = 0; i < number_of_vertices; i++){
        vertex_t source_vid = outbound_indices_arr[memshardID][i].vID;
        index_t dest_number;
        index_t j = 0;
        
        if (i < number_of_vertices - 1){
          dest_number = outbound_indices_arr[memshardID][i+1].index;
        }
        else{
          dest_number = edges_read;
        }
        j = outbound_indices_arr[memshardID][i].index;
        if (source_vid >= first_vid && source_vid <= last_vid){
          while(j < dest_number){
            vertex_t dest_vid = adj_shard_arr[j];
            
            vertices[dest_vid].addInedge(&vertices[source_vid], &edge_data_arr[j]);
            vertices[source_vid].addOutedge(&vertices[dest_vid], &edge_data_arr[j]);
            j++;
          }
        }
        else{
          while(j < dest_number){
            vertices[adj_shard_arr[j]].addInedge(&vertices[source_vid], &edge_data_arr[j]);
            j++;
          }
        }
        
      }
    }
    
  public:
    
    uint16_t nextID()
    {
      if (memshardID == intervals_number - 1){
        memshardID = 0;
      }
      else{
        memshardID++;
      }
      return memshardID;
    }
    
    uint16_t getID()
    {
      return memshardID;
    }
    
    uint32_t getDestNum()
    {
      return mem_destinations;
    }
    
    Memoryshard(Outbound_t ** out, bool keep_vertices_in_memory, Cache * hCache): keep_vertices_in_memory(keep_vertices_in_memory), memshardID(-1),
    edge_data_arr(NULL), adj_shard_arr(NULL), inbound_degrees_arr(NULL),
    inbound_edges_read(0), outbound_indices_arr(out), hCache(hCache){}
    
    ~Memoryshard()
    {
      CHECK(edge_data_arr);
      free(edge_data_arr);
      
      if (!keep_vertices_in_memory){
        CHECK(adj_shard_arr);
        CHECK(inbound_degrees_arr);
        
        free(adj_shard_arr);
        free(inbound_degrees_arr);
      }
    }
    
    void SetShardArrays(vertex_t *& adj_shard_arr, DegreeData_t *&  inbound_degrees_arr)
    {
      this->adj_shard_arr       = adj_shard_arr;
      this->inbound_degrees_arr = inbound_degrees_arr;
    }
    
    /**
     * prepare
     *
     * Prepare the next shard as memoryshard
     *
     * @param   membounds         first and last vertex of memoryshard
     * @param   number_of_edges   number of edges in memoryshard
     * @return  void
     */
    void prepare(Interval_t interval_bounds, uint32_t number_of_edges, GraphVertex * vertices)
    {
      this->vertices                = vertices;
      this->edges_read              = number_of_edges;
      this->first_vid               = interval_bounds.first_vid;
      this->last_vid                = interval_bounds.last_vid;
      this->mem_destinations        = interval_bounds.destinations_num;
      /* load in-edges and internal out-edges */
      load_edges();
    }
  };
  
}

#endif /* memoryshard_hpp */
