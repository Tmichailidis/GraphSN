/*
  sharder.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef sharder_hpp
#define sharder_hpp

#include <iostream>
#include <vector>
#include <ctime>
#include <set>

#include "log.hpp"
#include "types.hpp"

#define NUMBER_OF_THREADS number_of_cores*2
// #define DEFAULT_SHARDER_BYTES (24)
#define DEFAULT_SHARDER_BYTES (64L*1024L*1024L)
//#define DEFAULT_SHARDER_BYTES (128L*1024L*1024L)
//#define DEFAULT_SHARDER_BYTES (800L*1024L*1024L)


namespace GraphSN {
  
  template <typename EdgeType>
  static bool Shards_comparator(const EdgeType &a, const EdgeType &b)
  {
    return a.src < b.src;
  }
  
  /**
   * FindOccurences
   *
   * Calculating inbound degree for every vertex
   *
   * @param   sorted_edges    vector with sorted edges
   * @param   occurences_vec  vector with inbound degree for every vertex
   * @return  void
   */
  template <typename EdgeType>
  static void FindOccurences(std::vector<EdgeType>& sorted_edges, std::vector <DegreeData_t> &inbound_vec)
  {
    vertex_t current_vertex;
    uint32_t counter = 1,i;
    DegreeData_t inbound;
    
    current_vertex = sorted_edges[0].dst;
    for (i = 1 ; i < sorted_edges.size();i++)
    {
      if (current_vertex == sorted_edges[i].dst){
        counter++;
      }
      else{
        inbound.vID = current_vertex;
        inbound.degree = counter;
        inbound_vec.push_back(inbound);
        
        counter = 1;
        current_vertex = sorted_edges[i].dst;
      }
    }
    /* adding degree for the last vertex */
    inbound.vID = current_vertex;
    inbound.degree = counter;
    inbound_vec.push_back(inbound);
    
    /* write inbound degrees in file */
    int32_t fd;
    std::string inbound_degrees_filename = inFolder+"inbound_degrees";
    fd = open(inbound_degrees_filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    if (fd == -1) handle_error(inbound_degrees_filename.c_str());
    write_sys(reinterpret_cast<char*>(inbound_vec.data()),inbound_vec.size() * sizeof(DegreeData_t),fd);
    CHECK(close(fd) == 0);
  }
  
  /**
   * write_shard
   *
   * Writing shard to disk
   *
   * @param   buffer            shard buffer
   * @param   number_of_bytes   number of bytes to be written
   * @param   shard_id          id of the shard
   * @return  void
   */
  void write_shard(char * buffer, size_t number_of_bytes,const uint16_t shard_id)
  {
    int32_t fd;
    std::string shard_filename = inFolder + "Shards/shard_";
    fd = open((shard_filename+std::to_string(shard_id)).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    write_sys(buffer,number_of_bytes,fd);
    CHECK(close(fd) == 0);
  }
  
  /**
   * write_outbound_edges
   *
   * Writing outbound edges of a shard
   *
   * @param   vecOutbound       vector with sorted edges
   * @param   shard_id          ID of shard
   * @return  void
   */
  static inline void write_outbound_indices(Outbound_t * outbound_indices_arr, index_t number_of_sources, const uint32_t shard_id)
  {
    std::string outbound_filename = inFolder+"Outbound/outbound_indices_";
    FILE* pFile;
    
    pFile = fopen((outbound_filename+std::to_string(shard_id)+".binary").c_str(), "wb");
    fwrite(reinterpret_cast<char*>(outbound_indices_arr), sizeof(Outbound_t), number_of_sources, pFile);
    fclose(pFile);
  }
  
  /**
   * write_edge_data
   *
   * Writing edge data to disk
   *
   * @param   buffer            shard buffer
   * @param   number_of_bytes   number of bytes to be written
   * @param   shard_id          id of the shard
   * @return  void
   */
  void write_edge_data(char * buffer, size_t number_of_bytes,const uint16_t shard_id)
  {
    int32_t fd;
    std::string data_filename = inFolder + "EdgeData/edgedata_";
    fd = open((data_filename + std::to_string(shard_id)).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    write_sys(buffer, number_of_bytes, fd);
    CHECK(close(fd) == 0);
  }
  
  void write_vertex_data(char * buffer, size_t number_of_bytes)
  {
    int32_t fd;
    std::string data_filename = inFolder + "vertex_data";
    
    fd = open(data_filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    write_sys(buffer, number_of_bytes, fd);
    CHECK(close(fd) == 0);
  }
  
  /**
   * write_intervals
   *
   * writing intervals to their files
   *
   * @param   intervals  vector of pairs with the first and the last vertex of each interval
   * @return  void
   */
  static inline void write_intervals(std::vector <Interval_t> intervals)
  {
    std::ofstream intervals_filestream;
    std::string intervals_filename = inFolder+"intervals.binary";
    intervals_filestream.open(intervals_filename.c_str(),std::ios::out | std::ios::binary);
    
    intervals_filestream.write(reinterpret_cast<char*>(intervals.data()),intervals.size() * sizeof(Interval_t));
    if ((intervals_filestream.rdstate() & std::ifstream::failbit) != 0){
      std::cerr << "Error: " << strerror(errno) << std::endl;
      exit(-1);
    }
  }
  
  /**
   * convert_adjacency_shard
   *
   * Convert shard to adjacency form
   *
   * @param   shard_edges     edges of shard
   * @param   first_index     index of first edge to be sharded
   * @param   number_of_edges number of edges in this shard
   * @param   adj_shard_vec   adjacency vector
   * @param   outbound_vec    vector with first index of each src vertex appearing in shard
   * @return  void
   */
  template <typename EdgeType>
  static void convert_adjacency_shard(std::vector<EdgeType>& shard_edges, const index_t first_index,
                                      const uint32_t number_of_edges, std::vector <vertex_t>& adj_shard_vec, std::vector <Outbound_t>& outbound_vec)
  {
    vertex_t current_src_id = shard_edges[first_index].src;
    Outbound_t tmpOB;
    
    adj_shard_vec.reserve(number_of_edges);
    tmpOB.vID = current_src_id;
    tmpOB.index = 0;
    outbound_vec.push_back(tmpOB);
    
    /* creating a vector with destination vertices in the shard -> adj_shard_vec */
    /* creating another vector with the indices of the first vertex of each source vertex -> outbound_vec */
    for (vertex_t index = first_index; index < first_index + number_of_edges; index++){
      EdgeType current_edge = shard_edges[index];
      adj_shard_vec.push_back(current_edge.dst);
      
      if (current_src_id != current_edge.src){
        current_src_id = current_edge.src;
        tmpOB.vID = current_src_id;
        tmpOB.index = index - first_index;
        outbound_vec.push_back(tmpOB);
      }
    }
  }
  
  
  /**
   * sort_shard
   *
   * Sorts shards by source
   *
   * @param   sorted_edges      vector with sorted edges
   * @param   first_index       index of first edge to be sharded
   * @param   number_of_edges   number of edges in this shard
   * @param   shard_id          ID of shard
   * @return  void
   */
  template <typename EdgeType>
  static void sort_shard(std::vector<EdgeType>& shard_edges,
                         const uint32_t first_index,const uint32_t number_of_edges,const uint32_t shard_id)
  {
    std::vector <value_t>     edge_data_vec;
    std::vector <vertex_t>    adj_shard_vec;
    std::vector <Outbound_t>  outbound_vec;
    
    /* sort shard by source */
    std::sort(shard_edges.begin() + first_index,shard_edges.begin() + first_index + number_of_edges,Shards_comparator<EdgeType>);
    
    /* convert the shard to adjacency format */
    convert_adjacency_shard<EdgeType>(shard_edges, first_index, number_of_edges, adj_shard_vec, outbound_vec);
    
    /* crete edge data */
    edge_data_vec.resize(number_of_edges);
    if (sizeof(EdgeType) == sizeof(EdgeWithValue_t)){
      size_t offset = 2 * sizeof(vertex_t);
      size_t membytes = sizeof(value_t);
      
      for (index_t index = first_index; index < first_index + number_of_edges; index++){
        value_t current_value;
        
        memcpy(&current_value,(char *)&shard_edges[index] + offset, membytes);
        edge_data_vec[index - first_index] = current_value;
      }
    }
    else{
      DBG_LOG("No edge data found, filling with 0\n");
      std::fill(edge_data_vec.begin(), edge_data_vec.end(), 0);
    }
    
    /* write everything on disk */
    write_outbound_indices(&outbound_vec[0], (index_t)outbound_vec.size(), shard_id);
    write_shard(reinterpret_cast<char*>(adj_shard_vec.data()), number_of_edges * sizeof(vertex_t), shard_id);
    write_edge_data(reinterpret_cast<char*>(edge_data_vec.data()), number_of_edges * sizeof(value_t), shard_id);
    
    edge_data_vec.shrink_to_fit();
  }
  
  /**
   * init_sharding
   *
   * Starts the sharding phase
   *
   * @param   sorted_edges  vector with sorted edges
   * @param   intervals     vector of intervals with vertices
   * @return  void
   */
  template <typename EdgeType>
  static void init_sharding(std::vector<EdgeType>& sorted_edges,std::vector<uint32_t> edges_in_intervals){
    std::thread * sharding_threads = new std::thread[NUMBER_OF_THREADS];
    std::ofstream infoshard_file(inFolder+"shards.info",std::ofstream::binary);
    uint16_t threads_used = 0;
    uint32_t current_interval;
    uint64_t edges_sharded = 0;
    
    DBG_LOG("Number of sharding threads = %u\n",NUMBER_OF_THREADS);
    for (current_interval = 0; current_interval < (uint32_t) edges_in_intervals.size(); current_interval++){
      //if we have assigned a task to every thread, wait for them to end before reassigning.
      if (threads_used == NUMBER_OF_THREADS){
        for (uint16_t i = 0; i < NUMBER_OF_THREADS; i++){
          sharding_threads[i].join();
        }
        threads_used = 0;
      }
      DBG_LOG("Interval %u has %u edges\n",current_interval,edges_in_intervals[current_interval]);
      sharding_threads[threads_used] = std::thread(sort_shard<EdgeType>,
                                                   std::ref(sorted_edges),
                                                   edges_sharded,
                                                   edges_in_intervals[current_interval],
                                                   current_interval);
      edges_sharded += edges_in_intervals[current_interval];
      threads_used++;
    }
    
    /* write to file the number of shards */
    CHECK(infoshard_file.is_open());
    infoshard_file << (uint32_t) current_interval << '\n';
    infoshard_file.close();
    
    /*Joing sharding threads and deleting the allocated memory*/
    for (uint16_t i = 0; i < threads_used; i++){
      sharding_threads[i].join();
    }
    LOG("Sharding Ended\n");
    
    delete [] sharding_threads;
  }
  
  /**
   * calc_number_of_shards
   *
   * calculating number of shards & default value for bytes per shards
   *
   * @return default bytes per shard
   */
  static uint32_t calc_number_of_shards()
  {
    if (intervals_number != 0){
      return DEFAULT_SHARDER_BYTES;
    }
    
    uint64_t bytes = (uint64_t) (edges_num * sizeof(vertex_t));
    uint32_t last_shard_bytes;
    
    intervals_number = (uint32_t) (bytes / DEFAULT_SHARDER_BYTES);
    last_shard_bytes = (uint32_t) (bytes % DEFAULT_SHARDER_BYTES);
    
    if (bytes % DEFAULT_SHARDER_BYTES != 0){
      intervals_number += 1;
    }
    if (last_shard_bytes <= 0.5 * DEFAULT_SHARDER_BYTES){
      return DEFAULT_SHARDER_BYTES;
    }
    return (uint32_t) (bytes / intervals_number);
  }
  
  /**
   * calc_number_of_vertices
   *
   * Calculate number of vertices
   *
   * @param   sorted_edges  vector with sorted edges
   * @param   intervals     vector of intervals with vertices
   * @param   max_dst       max vertex destination id
   * @return  void
   */
  template<typename EdgeType>
  static void calc_number_of_vertices(std::vector <EdgeType>& sorted_edges,
                                      std::vector <uint32_t> edges_in_intervals, vertex_t max_dst)
  {
    vertex_t        max, last_vertex_index;
    std::ofstream   infoshard_file(inFolder + "shards.info", std::ofstream::binary | std::ofstream::app);
    value_t *       arrData;
    
    max = max_dst;
    last_vertex_index = 0;
    /* We have sorted each interval by source, so the first vertex has min id and the last max id */
    for (uint32_t index = 0; index < (uint32_t)edges_in_intervals.size(); index++){
      last_vertex_index += edges_in_intervals[index];

      max = std::max(max, sorted_edges[last_vertex_index - 1].src);
    }
    vertices_number = max + 1;
    /* write vertices with their values to file */
    arrData = (value_t *) calloc(vertices_number, sizeof(value_t));
    write_vertex_data(reinterpret_cast <char * > (&arrData[0]), vertices_number * sizeof(value_t));
    free(arrData);
    
    CHECK(infoshard_file.is_open());
    infoshard_file << (uint64_t) edges_num << '\n';
    infoshard_file << (uint32_t) vertices_number << '\n';
    infoshard_file.close();
    
    LOG("Number of vertices = %u\n",vertices_number);
  }
  
  /**
   * CalculateIntervals
   *
   * Calculates intervals of our graph
   *
   * @param   sorted_edges  vector with sorted edges
   * @return  void
   */
  template <typename EdgeType>
  void CalculateIntervals(std::vector <EdgeType>& sorted_edges){
    
    index_t last_index_added = 0;
    vertex_t max_dst;
    uint32_t i = 0, total_edges_in_intervals = 0, default_bytes_per_shard;
    std::vector <Interval_t> intervals;
    std::vector <uint32_t> edges_in_intervals;
    std::vector <DegreeData_t> vecInboundEdges;
    std::ofstream intervals_edges_file(inFolder + "intervals_edges.binary",std::ofstream::binary);
    
    CHECK(sorted_edges.size() != 0);
    max_dst = sorted_edges.back().dst;
    
    default_bytes_per_shard = calc_number_of_shards();
    LOG("Number of shards/intervals = %d\n",intervals_number);
    LOG("Default bytes per shard = %d\n",default_bytes_per_shard);
    
    timer.start("Calculating inbound edges");
    FindOccurences(sorted_edges, vecInboundEdges);
    timer.end("Calculating inbound edges");
    
    timer.start("Creating Intervals");
    /* here we are calculating intervals*/
    while(i < intervals_number){
      int64_t bytes_remaining_in_shard = default_bytes_per_shard;
      uint32_t edges_counter = 0, vertices_counter = 0;
      Interval_t interval;
      
      if (i == 0){
        interval.first_vid = 0;
      }
      else{
        interval.first_vid = vecInboundEdges[last_index_added - 1].vID + 1;
      }
      DBG_LOG("First = %u\n",interval.first_vid);
      while(last_index_added < vecInboundEdges.size()){
        uint32_t bytes_to_be_added = vecInboundEdges[last_index_added].degree * sizeof(vertex_t);
        
        if ((uint16_t)i != (intervals_number - 1) && bytes_to_be_added > bytes_remaining_in_shard && bytes_remaining_in_shard != default_bytes_per_shard){
          break;
        }
        bytes_remaining_in_shard -= bytes_to_be_added;
        edges_counter += vecInboundEdges[last_index_added].degree;
        vertices_counter++;
        last_index_added++;
      }
      
      interval.last_vid = vecInboundEdges[last_index_added - 1].vID;
      interval.destinations_num = vertices_counter;
      intervals.push_back(interval);
      edges_in_intervals.push_back(edges_counter);
      total_edges_in_intervals += edges_counter;
      i++;
    }
    
    CHECK(edges_num == total_edges_in_intervals);
    
    timer.end("Creating Intervals");
    
    timer.start("Sharding");
    init_sharding(sorted_edges,edges_in_intervals);
    timer.end("Sharding");
    
    timer.start("Calculating vertices number");
    calc_number_of_vertices(sorted_edges, edges_in_intervals, max_dst);
    intervals[intervals_number - 1].last_vid = vertices_number - 1;
    timer.end("Calculating vertices number");
    
    write_intervals(intervals);
    CHECK(intervals_edges_file.is_open());
    intervals_edges_file.write(reinterpret_cast <const char*> (&edges_in_intervals[0]), edges_in_intervals.size() * sizeof(uint32_t));
    intervals_edges_file.close();
  }
}

#endif /* sharder_hpp */
