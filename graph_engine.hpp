/*
  graph_engine.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef graph_engine_hpp
#define graph_engine_hpp

#include <vector>
#include <fstream>
#include <iostream>
#include <algorithm>

#include "graph_objects.hpp"
#include "memoryshard.hpp"
#include "slidingshard.hpp"
#include "cache.hpp"

#define KEEP_VERTICES true

namespace GraphSN {
  
  class Engine{
    
    bool                        keep_vertices_in_memory;
    int32_t                     fd_vertices;
    uint32_t                    current_minID, current_maxID, current_vertices_num;
    value_t *                   vertices_data_arr;
    std::vector < uint32_t >    intervals_edges;
    std::vector < Interval_t >  intervals;
    Outbound_t **               outbound_indices_arr;
    GraphSNProgram *         program;
    Memoryshard *               memshard;
    Slidingshard **             slidshard;
    GraphVertex *               vertices;
    GraphBox *                  hGraphbox;
    std::string                 vertices_filename;
    Cache *                     hCache;
    Locks *                     hLocks;
#ifdef KEEP_VERTICES
    vertex_t **                 adj_shard_arr;
    DegreeData_t *              inbound_degrees_arr;
#endif
    
    void print_edges()
    {
      println("\n");
      for (uint32_t i = current_minID; i <= current_maxID; i++){
        uint32_t eID;
        GraphVertex * vertex = &vertices[i];
        println("Vertice %u with data %f\nInedges: %u", vertex->getID(), vertex->getData(), vertex->getIndegree());
        for (eID = 0; eID < vertex->getIndegree(); eID++){
          println("[%u -> %u, %f]", vertex->edge(eID).getID(), vertex->getID(), vertex->edge(eID).getData());
        }
        println("Outedges: %u", vertex->getOutdegree());
        for (; eID < vertex->getIndegree() + vertex->getOutdegree(); eID++){
          println("[%u -> %u, %f]",vertex->getID(), vertex->edge(eID).getID(), vertex->edge(eID).getData());
        }
        print("\n");
      }
    }
    
    
    void load_shards_info()
    {
      std::fstream infoshard_file;
      
      infoshard_file.open(inFolder + "shards.info",std::fstream::binary | std::fstream::in);
      CHECK(infoshard_file.is_open());
      infoshard_file >> intervals_number >> edges_num >> vertices_number;
      infoshard_file.close();
      
      DBG_LOG("Number of shards = %u\n", intervals_number);
      DBG_LOG("Number of edges = %llu\n", (unsigned long long) edges_num);
      DBG_LOG("Number of vertices = %u\n", vertices_number);
    }
    
    void load_intervals()
    {
      std::ifstream intervals_filestream;
      std::string intervals_filename = inFolder + "intervals.binary";
      
      intervals_filestream.open(intervals_filename.c_str(),std::fstream::binary | std::fstream::in);
      if (!intervals_filestream.is_open()) handle_error(("opening " + intervals_filename).c_str());
      intervals.resize(intervals_number);
      intervals_filestream.read(reinterpret_cast<char*>(&intervals[0]),intervals_number * sizeof(Interval_t));
      CHECK(intervals_filestream);
      intervals_filestream.close();
      for (uint32_t i = 0; i < intervals_number; i++){
        DBG_LOG("Interval %u in [%u,%u]\n", i, intervals[i].first_vid, intervals[i].last_vid);
      }
    }
    
    void load_intervals_edges()
    {
      std::fstream intervals_edges_file;
      
      intervals_edges_file.open(inFolder+"intervals_edges.binary",std::fstream::binary | std::fstream::in);
      CHECK(intervals_edges_file.is_open());
      intervals_edges.resize(intervals_number);
      intervals_edges_file.read(reinterpret_cast <char*>(&intervals_edges[0]), intervals_number * sizeof(uint32_t));
      intervals_edges_file.close();
    }
    
    /**
     * preload_outbound
     *
     * Preload out-edges and their indices
     *
     * @return  void
     */
    void preload_outbound_indices()
    {
      FILE * pFile;
      uint32_t number_of_vertices;
      std::string outbound_filename = inFolder+"Outbound/outbound_indices_";
      
      outbound_indices_arr = (Outbound_t **) malloc(intervals_number * sizeof(Outbound_t *));
      /* load sources */
      for (int16_t interval = 0; interval < intervals_number; interval++){
        pFile = fopen((outbound_filename + std::to_string(interval)+".binary").c_str(), "rb");
        if (pFile == NULL) handle_error((outbound_filename + std::to_string(interval) + ".binary").c_str());
        number_of_vertices = GetElementsNumber(outbound_filename + std::to_string(interval) + ".binary", sizeof(Outbound_t));
        outbound_indices_arr[interval] = (Outbound_t *) malloc(number_of_vertices * sizeof(Outbound_t));
        CHECK(number_of_vertices == fread(reinterpret_cast<char*>(&outbound_indices_arr[interval][0]), sizeof(Outbound_t), number_of_vertices, pFile));
        fclose(pFile);
      }
    }
    
    /**
     * load_vertices_values
     *
     * load vertices' values from disk
     *
     * @return  void
     */
    void load_vertices_values()
    {
      vertices_filename = inFolder + "vertex_data";
      fd_vertices = open(vertices_filename.c_str(), O_RDWR);
      if (fd_vertices == -1) handle_error(vertices_filename.c_str());
      
      vertices_data_arr = (value_t *) malloc(vertices_number * sizeof(value_t));
      read_sys(reinterpret_cast<char*>(&vertices_data_arr[0]), vertices_number * sizeof(value_t), fd_vertices);
    }
    
    /**
     * save_vertices_values
     *
     * save vertices' values to disk
     *
     * @return  void
     */
    void save_vertices_values()
    {
      pwrite_sys(reinterpret_cast<char*>(&vertices_data_arr[0]), vertices_number * sizeof(value_t), 0, fd_vertices);
      free(vertices_data_arr);
      close(fd_vertices);
    }
    
    /**
     * assign_IDs
     *
     * Assign ID for every shard
     *
     * @return  void
     */
    void assign_IDs()
    {
      uint16_t memID = memshard->nextID();
      LOG("Memory shard for current execution interval = %u\n", memID);
      for (uint16_t id = 0; id < intervals_number; id++){
        if (memID == id){
          continue;
        }
        else if (id < memID){
          slidshard[id]->setID(id);
        }
        else{
          slidshard[id - 1]->setID(id);
        }
      }
      /* set needed values */
      current_minID = intervals[memshard->getID()].first_vid;
      current_maxID = intervals[memshard->getID()].last_vid;
      current_vertices_num = current_maxID - current_minID + 1;
    }
    
    /**
     * init_graph_vertices
     *
     * Initialize GraphVertex array
     *
     * @return  void
     */
    void init_graph_vertices()
    {
      vertices = (GraphVertex *) malloc(vertices_number * sizeof(GraphVertex));
      for (uint32_t i = 0; i < vertices_number; i++){
        vertices[i] = GraphVertex(i, &vertices_data_arr[i], hLocks);
      }
    }
    
    void reinit_graph_vertices()
    {
      for (uint32_t i = 0; i < vertices_number; i++){
        vertices[i].reinitialize();
      }
    }
    
    /**
     * reserve_outbound_mem
     *
     * Reserve memory for outbound edges
     *
     * @return  void
     */
    void reserve_outbound_mem()
    {
      uint32_t * capacity;  /* amount of total out-edges */
      std::string outbound_filename = inFolder + "Outbound/outbound_indices_";
      
      capacity = (uint32_t *) calloc(current_vertices_num, sizeof(uint32_t));
      for (int16_t interval = 0; interval < intervals_number; interval++){
        uint32_t number_of_vertices = GetElementsNumber(outbound_filename + std::to_string(interval) + ".binary", sizeof(Outbound_t));
        
        for (uint32_t i = 0; i < number_of_vertices; i++){
          uint32_t vertexID = outbound_indices_arr[interval][i].vID;
          if (vertexID < current_minID){
            continue;
          }
          if (vertexID > current_maxID){
            break;
          }
          if (i < number_of_vertices - 1){
            capacity[vertexID - current_minID] += (outbound_indices_arr[interval][i+1].index - outbound_indices_arr[interval][i].index);
          }
          else{
            capacity[vertexID - current_minID] += (intervals_edges[interval] - outbound_indices_arr[interval][i].index);
          }
        }
      }
      /* at this point we know the exact amount of memory we will need to allocate for outbound edges*/
      for (uint32_t i = current_minID; i <= current_maxID; i++){
        vertices[i].allocateOutedges(capacity[i - current_minID]);
      }
      free(capacity);
    }
    
    void prepare_shards()
    {
      uint16_t memID = memshard->getID();
      for (int16_t interval = 0; interval < intervals_number; interval++){
        if (memID == interval){
          if (keep_vertices_in_memory){
            memshard->SetShardArrays(adj_shard_arr[memID], inbound_degrees_arr);
          }
          memshard->prepare(intervals[memID], intervals_edges[memID], vertices);
        }
        else{
          if (interval < memID){
            if (keep_vertices_in_memory){
              slidshard[interval]->SetShardArray(adj_shard_arr[interval]);
            }
            slidshard[interval]->prepare(intervals[memID], intervals_edges[interval], vertices);
          }
          else{
            if (keep_vertices_in_memory){
              slidshard[interval - 1]->SetShardArray(adj_shard_arr[interval]);
            }
            slidshard[interval - 1]->prepare(intervals[memID], intervals_edges[interval], vertices);
          }
        }
      }
    }
    
    void prepare()
    {
      assign_IDs();
      reserve_outbound_mem();
      prepare_shards();
    }
    
    void initialize_shards(Cache * hCache)
    {
      if (memshard){
        delete memshard;
      }
      memshard = new Memoryshard(outbound_indices_arr, keep_vertices_in_memory, hCache);
      for (int16_t interval = 0; interval < intervals_number - 1; interval++){
        if (slidshard[interval]){
          delete slidshard[interval];
        }
        slidshard[interval] = new Slidingshard(outbound_indices_arr, keep_vertices_in_memory, hCache);
      }
    }
    
    /**
     * load_shards_in_memory
     *
     * Load shards & shards' related data in memory
     *
     * @return  void
    */
    void load_shards_in_memory(uint32_t inbound_degrees_num)
    {
      int32_t     fd_inbounds;
      std::string inbound_degrees_filename = inFolder + "inbound_degrees";
      std::string outbound_filename = inFolder + "Outbound/outbound_indices_";
      
      /* load inbound degrees*/
      fd_inbounds = open(inbound_degrees_filename.c_str(), O_RDONLY);
      if (fd_inbounds == -1) handle_error(inbound_degrees_filename.c_str());
      inbound_degrees_arr = (DegreeData_t *) malloc(inbound_degrees_num * sizeof(DegreeData_t));
      read_sys(reinterpret_cast<char*>(&inbound_degrees_arr[0]), inbound_degrees_num * sizeof(DegreeData_t), fd_inbounds);
      close(fd_inbounds);
      
      /* load shards */
      adj_shard_arr = (vertex_t **) malloc(intervals_number * sizeof(vertex_t *));
      for (uint16_t shardID = 0; shardID < intervals_number; shardID++){
        int32_t fd_shard;
        std::string current_shard_filename = shard_filename + std::to_string(shardID);
        
        fd_shard = open(current_shard_filename.c_str(), O_RDONLY);
        adj_shard_arr[shardID] = (vertex_t *) malloc(intervals_edges[shardID] * sizeof(vertex_t));
        read_sys(reinterpret_cast<char*>(&adj_shard_arr[shardID][0]), intervals_edges[shardID] * sizeof(vertex_t), fd_shard);
        close(fd_shard);
      }
    }
    
    void exec_update()
    {
      Scheduler * scheduler = hGraphbox->scheduler;
      
#pragma omp parallel for
      for (vertex_t i = current_minID; i <= current_maxID; i++){
        if (scheduler->is_scheduled(i)){
          program->update(vertices[i], *hGraphbox);
        }
      }
    }
    
    
  public:
    
    Engine()
    {
      /* reading info from file (number of shards, number of edges, number of vertices)*/
      load_shards_info();
      /* read [first,last] edge in each interval */
      load_intervals();
      /* read number of edges of each interval */
      load_intervals_edges();
      /* read outbound edges of each interval */
      preload_outbound_indices();
      /* read vertices' values */
      vertices_data_arr = NULL;
      load_vertices_values();
      
      this->keep_vertices_in_memory = KEEP_VERTICES;
      if (keep_vertices_in_memory){
        uint32_t inbound_degrees_num = 0;
        
        LOG("Keep vertices in memory mode\n");
        for (uint16_t shard = 0; shard < intervals_number; shard++){
          inbound_degrees_num += intervals[shard].destinations_num;
        }
        load_shards_in_memory(inbound_degrees_num);
      }
      memshard = NULL;
      slidshard = new Slidingshard *[intervals_number - 1];
      for (uint16_t i = 0; i < intervals_number - 1; i++){
        slidshard[i] = NULL;
      }
      hLocks = new Locks(4, vertices_number);
      init_graph_vertices();
      omp_set_nested(1);
      omp_set_num_threads(omp_get_max_threads() * 2);
    }
    
    ~Engine()
    {
      /* clear vectors */
      intervals.shrink_to_fit();
      intervals_edges.shrink_to_fit();
      /* free out edges' array */
      for (int16_t i = 0; i < intervals_number; i++){
        free(outbound_indices_arr[i]);
      }
      free(outbound_indices_arr);
      /* delete memory shard object */
      delete memshard;
      /* delete sliding shard objects */
      for (int16_t interval = 0; interval < intervals_number - 1; interval++){
        delete slidshard[interval];
      }
      delete [] slidshard;
      delete hLocks;
    }
    
    void run(GraphSNProgram& main_program, uint32_t iterations_num, uint64_t cache_size, std::string cachetype)
    {
      this->program   = &main_program;
      this->hGraphbox = new GraphBox(iterations_num, vertices_number);
      if (cachetype == "LRU"){
        if (cache_size != 0){
          LOG("Using LRU cache with size: %llu bytes\n",(long long unsigned int) cache_size);
        }
        this->hCache = new LRUCache(cache_size);
      }
      else if (cachetype == "Indegree"){
        if (cache_size != 0){
          LOG("Using Indegree cache with size: %llu bytes\n",(long long unsigned int) cache_size);
        }
        this->hCache = new IndegreeCache(cache_size);
        
        if (keep_vertices_in_memory){
          this->hCache->setArrays(outbound_indices_arr, inbound_degrees_arr, adj_shard_arr, true);
        }
        else{
          this->hCache->setArrays(outbound_indices_arr, NULL, NULL, false);
        }
      }
      else{
        print("Unrecognized cache type\n");
        exit(1);
      }
      timer.start("run");
      if (intervals_number == 1){
        initialize_shards(hCache);
        prepare();
        for (uint32_t iter = 0; iter < iterations_num; iter++){
          /* start of iteration loop */
          if (iter >= hGraphbox->get_iterations_num()){
            break;
          }
          if (!hGraphbox->scheduler->has_tasks){
            break;
          }
          hGraphbox->scheduler->has_tasks = false;
          
          program->before_iteration(*hGraphbox);
          program->before_exec_interval(*hGraphbox);
          exec_update();
          program->after_exec_interval(*hGraphbox);
          program->after_iteration(*hGraphbox);
          
          hGraphbox->increment_iteration();
          hGraphbox->scheduler->swap();
        } /* end of iteration loop */
      }
      else{
        for (uint32_t iter = 0; iter < iterations_num; iter++){
          /* start of iteration loop */
          if (iter >= hGraphbox->get_iterations_num()){
            break;
          }
          if (!hGraphbox->scheduler->has_tasks){
            break;
          }
          hGraphbox->scheduler->has_tasks = false;
          initialize_shards(hCache);
          
          program->before_iteration(*hGraphbox);
          for (uint16_t exec_inter = 0; exec_inter < intervals_number; exec_inter++){
            /* start of interval loop */
            reinit_graph_vertices();
            program->before_exec_interval(*hGraphbox);
            prepare();
            exec_update();
            program->after_exec_interval(*hGraphbox);
          } /* end of interval loop */
          program->after_iteration(*hGraphbox);
          hGraphbox->increment_iteration();
          hGraphbox->scheduler->swap();
        } /* end of iteration loop */
      }
      timer.end("run");
      
      save_vertices_values();
      delete hGraphbox;
      for (uint32_t i = 0; i < vertices_number; i++){
        vertices[i].~GraphVertex();
      }
      free(vertices);
      if (keep_vertices_in_memory){
        for (uint16_t shardID = 0; shardID < intervals_number; shardID++){
          free(adj_shard_arr[shardID]);
        }
        free(adj_shard_arr);
        free(inbound_degrees_arr);
      }
      delete hCache;
    }
  };
  
}

#endif /* graph_engine.hpp */
