/*
  cache.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef cache_hpp
#define cache_hpp
#define CACHE_BLOCK_SIZE (64L*1024L*1024L)

#include <list>
#include <algorithm>
#include <unordered_map>

uint32_t sum_hit_rate = 0, gsum = 0;

namespace GraphSN{
  
  /* 
      no_cache: no cache used,
      semi_cache: data does not fit entirely in cache, 
      full_cache: data does fit entirely in cache 
  */
  enum cache_mode
  {
    no_cache,   
    semi_cache,
    full_cache
  };
  
  typedef struct Cacheblock_s{
    value_t * data;
    uint32_t block_num;
    long long size;
  }Cacheblock_t;
  
  class Cache{
    
  public:
    virtual ~Cache(){};
    
  protected:
    cache_mode mode;
    uint32_t max_size;
    std::list <Cacheblock_t> hList;
    std::unordered_map <uint32_t, Cacheblock_t *> hHashMap;
    uint32_t * interval_edata_bytes;
    std::vector <std::pair <uint32_t, uint32_t> > cacheblocks_bounds;
    
    uint64_t getTotalSystemMemory()
    {
      uint64_t pages = sysconf(_SC_PHYS_PAGES);
      uint64_t page_size = sysconf(_SC_PAGE_SIZE);
      return pages * page_size;
    }
    
    /**
    * isFull
    *
    * Checks if cache is full.
    *
    * @return  bool
    */
    bool isFull()
    {
      return hList.size() == max_size;
    }
    
    /**
    * determine_cacheblocks
    *
    * Determine the number and bounds of each cache block
    *
    * @return  void
    */
    void determine_cacheblocks()
    {
      uint32_t current_start = 0, current_end;
      
      interval_edata_bytes = new uint32_t [intervals_number];
      for (uint16_t i = 0; i < intervals_number; i++){
        std::string current_edata_filename = std::string(edge_data_filename + std::to_string(i));
        
        interval_edata_bytes[i] = (uint32_t) GetFileSize(current_edata_filename);
        current_end = interval_edata_bytes[i] / CACHE_BLOCK_SIZE + ((interval_edata_bytes[i]  % CACHE_BLOCK_SIZE)? 1:0);
        cacheblocks_bounds.push_back(std::make_pair(current_start, current_start + current_end));
        DBG_LOG("Interval %u has blocks in [%u, %u]\n",i, current_start, current_end + current_start);
        current_start += current_end;
      }
    }
    

    /**
    * find_needed_blocks
    *
    * Find which blocks to use
    * 
    * @param    shardID       shardID
    * @param    start_offset  starting offset
    * @param    end_offset    ending offset
    * @return   void
    */
    std::pair<uint32_t, uint32_t> find_needed_blocks(uint32_t shardID,
                                                     off_t start_offset, off_t end_offset)
    {
      uint32_t first_blockID, last_blockID, base_blockID;
      
      base_blockID = cacheblocks_bounds[shardID].first;
      first_blockID = (uint32_t) (base_blockID + start_offset / CACHE_BLOCK_SIZE);
      last_blockID = (uint32_t) (base_blockID + end_offset / CACHE_BLOCK_SIZE);
      
      return std::make_pair(first_blockID, last_blockID);
    }
    
    virtual void add(value_t * data, long long size, uint32_t block_num) = 0;
    virtual void change_priority(uint32_t blockID) = 0;
    
    /**
    * load_from_cache
    *
    * Load data from cache entirely
    * 
    * @param    intervalID      intervalID
    * @param    block_interval  contains blocks that we are going to load
    * @param    start_offset    starting offset
    * @param    end_offset      ending offset
    * @param    arrEdgedata     array where the data is copied
    * @return   void
    */
    void load_from_cache(uint32_t intervalID, std::pair <uint32_t, uint32_t> block_interval, off_t start_offset,
                         off_t end_offset, value_t * &arrEdgedata)
    {
      int32_t fd_edges;
      uint32_t elements_written;
      off_t start, bytes_to_copy;
      std::string edata_filename = std::string(edge_data_filename + std::to_string(intervalID));
      
      elements_written = 0;
      fd_edges = open(edata_filename.c_str(), O_RDONLY, 0777);
      if (fd_edges == -1) handle_error(("opening "+ edata_filename).c_str());
      
      /* in this loop we are loading data from cache */
      for (uint32_t block = block_interval.first; block <= block_interval.second; block++){
        std::unordered_map <uint32_t, Cacheblock_t *>::const_iterator element = hHashMap.find(block);
        CHECK(element != hHashMap.end());
        if (block == block_interval.first){
          start = start_offset % CACHE_BLOCK_SIZE;
        }
        else{
          start = 0;
        }
        if (block == block_interval.second && end_offset != CACHE_BLOCK_SIZE){
          bytes_to_copy = end_offset % CACHE_BLOCK_SIZE - start;
        }
        else{
          bytes_to_copy = CACHE_BLOCK_SIZE - start;
        }
        memcpy(&arrEdgedata[elements_written], &element->second->data[start / sizeof(value_t)], bytes_to_copy);
        elements_written += bytes_to_copy / sizeof(value_t);
      }
      sum_hit_rate += block_interval.second + 1;
      gsum += block_interval.second + 1;
      LOG("%1$u/%1$u hit(s)\n", block_interval.second + 1);
    }
    
    /**
    * load
    *
    * Load data both from disk and from cache
    * 
    * @param    intervalID      intervalID
    * @param    block_interval  contains blocks that we are going to load
    * @param    start_offset    starting offset
    * @param    end_offset      ending offset
    * @param    arrEdgedata     array where the data is copied
    * @return   void
    */
    void load(uint32_t intervalID, std::pair <uint32_t, uint32_t> block_interval, off_t start_offset,
              off_t end_offset, value_t * &arrEdgedata)
    {
      off_t offset;
      int32_t fd_edges;
      uint32_t hit_counter = 0;
      uint32_t elements_written, elements_written_base;
      off_t start, bytes_to_copy;
      value_t * arrTmp;
      std::string edata_filename = std::string(edge_data_filename + std::to_string(intervalID));
      
      fd_edges = open(edata_filename.c_str(), O_RDONLY, 0777);
      if (fd_edges == -1) handle_error(("opening "+ edata_filename).c_str());
      
      /* in this loop we are taking data from cache */
      elements_written_base = (CACHE_BLOCK_SIZE - start_offset % CACHE_BLOCK_SIZE) / sizeof(value_t);
            
#pragma omp parallel for
      for (uint32_t block = block_interval.first; block <= block_interval.second; block++){
        std::unordered_map <uint32_t, Cacheblock_t *>::const_iterator element = hHashMap.find(block);
        uint32_t elements_written;
        off_t start, bytes_to_copy;
        
        if (element != hHashMap.end()){
          if (block == block_interval.first){
            start = start_offset % CACHE_BLOCK_SIZE;
          }
          else{
            start = 0;
          }
          if (block == block_interval.second && end_offset != CACHE_BLOCK_SIZE){
            bytes_to_copy = end_offset % CACHE_BLOCK_SIZE - start;
          }
          else{
            bytes_to_copy = CACHE_BLOCK_SIZE - start;
          }
          if (block != block_interval.first){
            elements_written = elements_written_base + (block - block_interval.first - 1) * CACHE_BLOCK_SIZE;
          }
          else{
            elements_written = 0;
          }
          __sync_add_and_fetch(&hit_counter, 1);
          memcpy(&arrEdgedata[elements_written], &element->second->data[start / sizeof(value_t)], bytes_to_copy);
        }
      }
      
      elements_written = 0;
      offset = CACHE_BLOCK_SIZE * (block_interval.first - cacheblocks_bounds[intervalID].first);
      arrTmp = (value_t *) malloc(CACHE_BLOCK_SIZE);
      
      long file_size = GetFileSize(edata_filename);
      for (uint32_t block = block_interval.first; block <= block_interval.second; block++){
        std::unordered_map <uint32_t, Cacheblock_t *>::const_iterator element = hHashMap.find(block);
        if (element != hHashMap.end()){ /* it is in cache */
          change_priority(element->first);
        }
        else{
          uint32_t bsize;
          
          if (block == cacheblocks_bounds[intervalID].second - 1 && file_size % CACHE_BLOCK_SIZE){
            bsize = file_size % CACHE_BLOCK_SIZE;
          }
          else{
            bsize = CACHE_BLOCK_SIZE;
          }
          pread_sys(reinterpret_cast<char*>(&arrTmp[0]), bsize, offset, fd_edges);
          add(arrTmp, bsize, block);
          if (block == block_interval.first){
            start = start_offset % CACHE_BLOCK_SIZE;
          }
          else{
            start = 0;
          }
          if (block == block_interval.second && end_offset != CACHE_BLOCK_SIZE){
            bytes_to_copy = end_offset % CACHE_BLOCK_SIZE - start;
          }
          else{
            bytes_to_copy = CACHE_BLOCK_SIZE - start;
          }
          memcpy(&arrEdgedata[elements_written], &arrTmp[start / sizeof(value_t)], bytes_to_copy);
        }
        if (block == block_interval.first && start_offset != CACHE_BLOCK_SIZE){
          elements_written += (CACHE_BLOCK_SIZE - start_offset % CACHE_BLOCK_SIZE) / sizeof(value_t);
        }
        else{
          elements_written += CACHE_BLOCK_SIZE / sizeof(value_t);
        }
        offset += CACHE_BLOCK_SIZE;
      }
      free(arrTmp);
      close(fd_edges);
      sum_hit_rate += hit_counter;
      gsum += block_interval.second - block_interval.first + 1;
      LOG("%u/%u hit(s) in interval %u\n", hit_counter, block_interval.second - block_interval.first + 1, intervalID);
    }
    
    /**
    * load_fully
    *
    * Load all data from disk on cache
    * 
    * @return   void
    */
    void load_fully()
    {
      value_t * arrTmp;
      
      arrTmp = (value_t *) malloc(CACHE_BLOCK_SIZE);
      for (uint16_t interval = 0; interval < intervals_number; interval++){
        int32_t fd;
        off_t offset = 0;
        std::string edata_filename = std::string(edge_data_filename + std::to_string(interval));
        fd = open(edata_filename.c_str(), O_RDONLY, 0777);
        if (fd == -1) handle_error(("opening "+ edata_filename).c_str());
        
        long long file_size = GetFileSize(edata_filename);
        for (uint32_t block = cacheblocks_bounds[interval].first; block < cacheblocks_bounds[interval].second; block++){
          uint32_t bsize;
          
          if (block == cacheblocks_bounds[interval].second - 1 && file_size % CACHE_BLOCK_SIZE){
            bsize = file_size % CACHE_BLOCK_SIZE;
          }
          else{
            bsize = CACHE_BLOCK_SIZE;
          }
          
          pread_sys(reinterpret_cast<char*>(&arrTmp[0]), bsize, offset, fd);
          add(arrTmp, bsize, block);
          offset += CACHE_BLOCK_SIZE;
        }
        close(fd);
      }
      free(arrTmp);
    }
    
  public:
    
    virtual void setArrays(Outbound_t ** outbound_indices_arr, DegreeData_t * inbound_degrees_arr,
                           vertex_t ** adj_shard_arr, bool keep_vertices_in_memory) = 0;
    
    bool noCacheMode()
    {
      return mode == no_cache;
    }
    
    void print_cache()
    {
      print("Cache contains: ");
      if (hList.size() == 0){
        println("nothing");
        return;
      }
      for (std::list<Cacheblock_t>::iterator it = hList.begin(); it != hList.end(); ++it){
        print("%u ", it->block_num);
      }
      print("\n");
    }
    
    void search_and_retrieve(uint32_t intervalID, off_t start_offset, off_t end_offset, value_t * arrEdgedata)
    {
      std::pair <uint32_t, uint32_t> block_interval;
      
      block_interval = find_needed_blocks(intervalID, start_offset, end_offset);
      if (mode == semi_cache){
        load(intervalID, block_interval, start_offset, end_offset, arrEdgedata);
      }
      else{
        load_from_cache(intervalID, block_interval, start_offset, end_offset, arrEdgedata);
      }
    }
  };
  
  class LRUCache: public Cache{
    
    void change_priority(uint32_t blockID)
    {
      for (std::list<Cacheblock_t>::iterator it = hList.begin(); it != hList.end(); ++it){
        if (it->block_num == blockID){
          hList.splice(hList.begin(), hList, it);
          return;
        }
      }
    }
    
    void add(value_t * data, long long size, uint32_t block_num)
    {
      Cacheblock_t cache_block;
      
      CHECK(size > 0);
      
      cache_block.data = (value_t *) malloc(size);
      memcpy(cache_block.data, data, size);
      cache_block.size = size;
      cache_block.block_num = block_num;
      
      if (isFull())
      {
        Cacheblock_t deleted_cache_block = hList.back();
        free(deleted_cache_block.data);
        hHashMap.erase(deleted_cache_block.block_num);
        hList.pop_back();
      }
      hList.push_front(cache_block);
      hHashMap[block_num] = &hList.front();
    }
    
  public:
    
    LRUCache(uint64_t size)
    {
      uint64_t edge_data_total_size = 0;
      uint64_t total_system_memory = getTotalSystemMemory();
      
      if (size > total_system_memory / 2){
        size = total_system_memory;
      }
      
      for (uint16_t i = 0; i < intervals_number; i++){
        edge_data_total_size += GetFileSize(edge_data_filename + std::to_string(i));
      }
      if (size == 0){
        mode = no_cache;
        return;
      }
      /* determine number of cache blocks in every interval */
      determine_cacheblocks();
      max_size = (uint32_t) (size / CACHE_BLOCK_SIZE + ((size % CACHE_BLOCK_SIZE)? 1:0));
      
      if (size >= edge_data_total_size){
        mode = full_cache;
        load_fully();
      }
      else{
        mode = semi_cache;
      }
    }
    
    ~LRUCache()
    {
      /* delete array */
      LOG("Sum hit rate = %u/%u\n", sum_hit_rate, gsum);
      delete [] interval_edata_bytes;
    }
    
    void setArrays(Outbound_t ** ,DegreeData_t *, vertex_t **, bool){}
  };
  
  /* This type of cache prioritises blocks that have high indegree */
  class IndegreeCache: public Cache{
    
    bool            keep_vertices_in_memory;
    value_t         * arr_indegree_values;
    uint32_t        * arr_outbound_number, arr_inbound_number, * arr_shard_edges;
    Outbound_t      ** outbound_indices_arr;
    DegreeData_t    * inbound_degrees_arr;
    vertex_t        ** adj_shard_arr;
    
    void determine_indegree_values()
    {
      uint32_t elements_per_block = CACHE_BLOCK_SIZE / sizeof(value_t);
      uint32_t cache_blocks_num = (uint32_t) cacheblocks_bounds.back().second;
      uint32_t * indegrees;
      vertex_t * arrCurShard;
      LOG("Cache blocks number = %u\n",cache_blocks_num);
      timer.start("Indegree values");
      indegrees = (uint32_t *) malloc(vertices_number * sizeof(value_t));
      arrCurShard = (vertex_t * ) malloc(elements_per_block * sizeof(vertex_t));
      for (uint32_t i = 0; i < arr_inbound_number; i++){
        indegrees[inbound_degrees_arr[i].vID] = inbound_degrees_arr[i].degree;
      }
      
      arr_indegree_values = new value_t[cache_blocks_num];
      
      for (uint16_t interval = 0; interval < intervals_number; interval++){
        index_t  outbound_index = 0;
        for (uint32_t blockID = cacheblocks_bounds[interval].first; blockID < cacheblocks_bounds[interval].second; blockID++){
          uint32_t start = (blockID - cacheblocks_bounds[interval].first) * elements_per_block;
          uint32_t end = ((blockID == cacheblocks_bounds[interval].second - 1) ? arr_shard_edges[interval] : start + elements_per_block) - 1;
          value_t  current_value;
          vertex_t prev;
          uint32_t distinct_sources = 0, starting_outbound_index, size;
          Outbound_t * current_outbound = outbound_indices_arr[interval];
          
          size = end - start + 1;

          memcpy(arrCurShard, &adj_shard_arr[interval][start], size * sizeof(vertex_t));
          std::sort(arrCurShard, arrCurShard + size);
          
          prev = arrCurShard[0];
          current_value = indegrees[prev];
          for (uint32_t i = 1; i < size; i++){
            if (prev == arrCurShard[i]){
              continue;
            }
            prev = arrCurShard[i];
            current_value += indegrees[prev];
          }

          DBG_LOG("Current value = %f\n", current_value);
          starting_outbound_index = outbound_index;
          while(outbound_index < arr_outbound_number[interval]){
            if (end >= current_outbound[outbound_index].index){
              outbound_index++;
              continue;
            }
            break;
          }
          if (outbound_index < arr_outbound_number[interval]){
            DBG_LOG("End is %u and index = %u\n", end, current_outbound[outbound_index].index);
            if (end + 1 < current_outbound[outbound_index].index){
              outbound_index--;
            }
          }
          distinct_sources = outbound_index - starting_outbound_index + 1;
          DBG_LOG("distinct sources = %u\n", distinct_sources);
          arr_indegree_values[blockID] = current_value / distinct_sources;
          DBG_LOG("arr_indegree_values[%u] = %f\n",blockID, arr_indegree_values[blockID]);
        }
      }
      free(arrCurShard);
      free(indegrees);
      if (!keep_vertices_in_memory){
        free(inbound_degrees_arr);
        for (uint16_t i = 0; i < intervals_number; i++){
          free(adj_shard_arr[i]);
        }
        free(adj_shard_arr);
      }
      timer.end("Indegree values");
      for(uint32_t i = 0; i < cache_blocks_num; i++){
        DBG_LOG("Block %u has value = %f\n",i,arr_indegree_values[i]);
      }
    }
    
    void add(value_t * data, long long size, uint32_t block_num)
    {
      value_t value_to_add;
      Cacheblock_t cache_block;
      std::list<Cacheblock_t>::iterator it;
      
      CHECK(size > 0);
      
      value_to_add = arr_indegree_values[block_num];
      
      if (isFull())
      {
        if (value_to_add < arr_indegree_values[hList.back().block_num]){
          return;
        }
        Cacheblock_t deleted_cache_block = hList.back();
        free(deleted_cache_block.data);
        hHashMap.erase(deleted_cache_block.block_num);
        hList.pop_back();
      }
      
      cache_block.data = (value_t *) malloc(size);
      memcpy(cache_block.data, data, size);
      cache_block.size = size;
      cache_block.block_num = block_num;
      
      /* TODO: search from end to start */
      for (it = hList.begin(); it != hList.end(); ++it){
        if (arr_indegree_values[it->block_num] < value_to_add){
          break;
        }
      }
      hHashMap[block_num] = &(*hList.insert(it, cache_block));
    }
    
    void change_priority(uint32_t blockID)
    {
      SILENCE blockID;
      return;
    }
    
  public:
    
    IndegreeCache(uint64_t size)
    {
      uint64_t edge_data_total_size = 0;
      uint64_t total_system_memory = getTotalSystemMemory();
      
      if (size > total_system_memory / 2){
        size = total_system_memory;
      }
      
      for (uint16_t i = 0; i < intervals_number; i++){
        edge_data_total_size += GetFileSize(edge_data_filename + std::to_string(i));
      }
      if (size == 0){
        mode = no_cache;
        return;
      }
      /* determine number of cache blocks in every interval */
      determine_cacheblocks();
      max_size = (uint32_t) (size / CACHE_BLOCK_SIZE + ((size % CACHE_BLOCK_SIZE)? 1:0));
      
      if (size >= edge_data_total_size){
        mode = full_cache;
      }
      else{
        mode = semi_cache;
      }
    }
    
    ~IndegreeCache()
    {
      /* delete arrays */
      LOG("Sum hit rate = %u/%u\n", sum_hit_rate, gsum);
      delete [] arr_indegree_values;
      delete [] interval_edata_bytes;
      delete [] arr_outbound_number;
      delete [] arr_shard_edges;
    }
    
    void setArrays(Outbound_t ** outbound_indices_arr, DegreeData_t * inbound_degrees_arr,
                   vertex_t ** arrShard, bool keep_vertices_in_memory)
    {
      uint64_t total_outbound_number, total_edges_in_shards;
      std::string inbound_degrees_filename = inFolder + "inbound_degrees";
      std::string outbound_filename = inFolder + "Outbound/outbound_indices_";
      
      this->keep_vertices_in_memory = keep_vertices_in_memory;
      
      arr_outbound_number = new uint32_t[intervals_number];
      arr_shard_edges     = new uint32_t[intervals_number];
      
      total_outbound_number = 0;
      total_edges_in_shards = 0;
      
      for (uint16_t interval = 0; interval < intervals_number; interval++){
        arr_outbound_number[interval] = GetElementsNumber(outbound_filename + std::to_string(interval) + ".binary", sizeof(Outbound_t));
        arr_shard_edges[interval] = GetElementsNumber(shard_filename + std::to_string(interval), sizeof(vertex_t));
        total_outbound_number += arr_outbound_number[interval];
        total_edges_in_shards += arr_shard_edges[interval];
      }
      this->outbound_indices_arr = outbound_indices_arr;
      
      arr_inbound_number = GetElementsNumber(inbound_degrees_filename, sizeof(DegreeData_t));
      
      if (keep_vertices_in_memory){
        this->inbound_degrees_arr = inbound_degrees_arr;
        this->adj_shard_arr = arrShard;
      }
      else{
        int32_t fd;
        /* load inbound degrees*/
        this->inbound_degrees_arr = (DegreeData_t *) malloc(arr_inbound_number * sizeof(DegreeData_t));
        fd = open(inbound_degrees_filename.c_str(), O_RDONLY);
        if (fd == -1) handle_error(inbound_degrees_filename.c_str());
        this->inbound_degrees_arr = (DegreeData_t *) malloc(arr_inbound_number * sizeof(DegreeData_t));
        read_sys(reinterpret_cast<char*>(&this->inbound_degrees_arr[0]), arr_inbound_number * sizeof(DegreeData_t), fd);
        close(fd);
        
        /*load shards */
        adj_shard_arr = (vertex_t **) malloc(intervals_number * sizeof(vertex_t *));
        for (uint16_t interval = 0; interval < intervals_number; interval++){
          int32_t fd;
          std::string current_shard_filename = shard_filename + std::to_string(interval);
          
          fd = open(current_shard_filename.c_str(), O_RDONLY);
          adj_shard_arr[interval] = (vertex_t *) malloc(arr_shard_edges[interval] * sizeof(vertex_t));
          read_sys(reinterpret_cast<char*>(&adj_shard_arr[interval][0]), arr_shard_edges[interval] * sizeof(vertex_t), fd);
          close(fd);
        }
      }
      /* determine indegree value of each cache block */
      if (mode != no_cache){
        determine_indegree_values();
      }
      if (mode == full_cache){
        load_fully();
      }
    }
  };
  
}

#endif /* cache_hpp */
