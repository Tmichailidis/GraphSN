/*
  slidingshard.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef slidingshard_hpp
#define slidingshard_hpp

#include <unordered_map>

namespace GraphSN {
  
  class Slidingshard{
    
    bool            keep_vertices_in_memory;
    int32_t         fd_edges, first_index, last_index, edges_read;  /* first_index and last_index are the bounds of the sliding window */
    int16_t         shardID;
    value_t *       edge_data_arr;
    vertex_t *      adj_shard_arr;
    vertex_t        memshard_firstID, memshard_lastID;
    uint32_t        edge_number_offset;
    GraphVertex *   vertices;
    Cache *         hCache;
    Outbound_t **   outbound_indices_arr;
    
    /**
     * load_edges
     *
     * Load out-edges from sliding shard
     *
     * @param   number_of_edges   number of edges in memoryshard
     * @return
     */
    void load_edges(uint32_t number_of_edges)
    {
      std::unordered_map <uint32_t, uint32_t> inbound_degrees;
      int32_t     fd, number_of_vertices, index, offset;
      std::string outbound_filename = inFolder + "Outbound/outbound_indices_";
      std::string slidshard_filename = shard_filename + std::to_string(shardID);
      std::string slidshard_edata_filename = std::string(edge_data_filename + std::to_string(shardID));
      
      first_index = last_index = -1;
      edges_read = -1;
      number_of_vertices = GetElementsNumber(outbound_filename+std::to_string(shardID)+".binary", sizeof(Outbound_t));
      index = 0;
      /* first, we need to properly find the number of bytes we need to load from this shard */
      while(index < number_of_vertices){
        if (outbound_indices_arr[shardID][index].vID < memshard_firstID){
          index++;
          continue;
        }
        if (outbound_indices_arr[shardID][index].vID <= memshard_lastID){
          first_index = index;
          index++;
        }
        break;
      }
      /* in case first_index = -1, there is no outbound edges to load */
      if (first_index == -1){
        return;
      }
      while(index < number_of_vertices){
        if (outbound_indices_arr[shardID][index].vID > memshard_lastID){
          edges_read = outbound_indices_arr[shardID][index].index - outbound_indices_arr[shardID][first_index].index;
          break;
        }
        index++;
      }
      /* in case that edges_read = -1, we reached end of file and we have to set the correct value */
      if (edges_read == - 1){
        edges_read = number_of_edges - outbound_indices_arr[shardID][first_index].index;
      }
      last_index = index - 1;
      edge_number_offset = outbound_indices_arr[shardID][first_index].index;
      
      if (!keep_vertices_in_memory){
        /* at this point we know how many edges we will need to load from shard */
        adj_shard_arr = (vertex_t *) realloc(adj_shard_arr, edges_read * sizeof(vertex_t));
        fd = open(slidshard_filename.c_str(), O_RDONLY);
        if (fd == -1) handle_error(("opening " + slidshard_filename).c_str());
        pread_sys(reinterpret_cast<char*>(&adj_shard_arr[0]), edges_read * sizeof(vertex_t), edge_number_offset * sizeof(vertex_t), fd);
        close(fd);
      }
      
      offset = keep_vertices_in_memory ? edge_number_offset : 0;
      
      edge_data_arr = (value_t *) realloc(edge_data_arr, edges_read * sizeof(value_t));
      if (hCache->noCacheMode()){
        fd_edges = open(slidshard_edata_filename.c_str(), O_RDWR, 0777);
        if (fd_edges == -1) handle_error(("opening "+ slidshard_edata_filename).c_str());
        pread_sys(reinterpret_cast<char*>(&edge_data_arr[0]), edges_read * sizeof(value_t), edge_number_offset * sizeof(value_t),fd_edges);
        close(fd_edges);
      }
      else{
        off_t end_offset = edges_read * sizeof(value_t) + edge_number_offset * sizeof(value_t);
        hCache->search_and_retrieve(shardID, edge_number_offset * sizeof(value_t), end_offset, edge_data_arr);
      }
      
#pragma omp parallel for
      for (int32_t i = first_index; i <= last_index; i++){
        index_t dest_number;
        vertex_t source_vid  = outbound_indices_arr[shardID][i].vID;
        
        if (i < number_of_vertices - 1){
          dest_number = outbound_indices_arr[shardID][i+1].index - outbound_indices_arr[shardID][i].index;
        }
        else{
          dest_number = number_of_edges - outbound_indices_arr[shardID][i].index;
        }
        uint32_t j = outbound_indices_arr[shardID][i].index - outbound_indices_arr[shardID][first_index].index;
        while(dest_number--){
          vertex_t dest_vid = adj_shard_arr[j + offset];
          
          vertices[source_vid].addOutedge(&vertices[dest_vid], &edge_data_arr[j]);
          j++;
        }
      }
      return;
    }
    
  public:
    
    Slidingshard(Outbound_t ** out, bool keep_vertices_in_memory, Cache * hCache):  keep_vertices_in_memory(keep_vertices_in_memory), edge_data_arr(NULL),
    adj_shard_arr(NULL), hCache(hCache), outbound_indices_arr(out){}
    
    ~Slidingshard()
    {
      CHECK(edge_data_arr);
      free(edge_data_arr);
      
      if (!keep_vertices_in_memory){
        CHECK(adj_shard_arr);
        free(adj_shard_arr);
      }
    }
    
    void SetShardArray(vertex_t *& adj_shard_arr)
    {
      this->adj_shard_arr = adj_shard_arr;
    }
    
    void setID(int16_t ID)
    {
      this->shardID = ID;
    }
    
    uint16_t getID(){ return shardID; }
    
    void prepare(Interval_t interval_bounds, uint32_t number_of_edges, GraphVertex * vertices)
    {
      this->vertices                 = vertices;
      this->memshard_firstID         = interval_bounds.first_vid;
      this->memshard_lastID          = interval_bounds.last_vid;
      load_edges(number_of_edges);
    }
  };
}

#endif /* slidingshard_hpp */
