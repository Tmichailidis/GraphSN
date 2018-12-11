/*
  externalsorting.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef externalsorting_hpp
#define externalsorting_hpp

#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <algorithm>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/mman.h>
#include <ctime>

#include "core.hpp"
#include "sharder.hpp"

#define NUMBER_OF_THREADS number_of_cores*2

namespace GraphSN {
  
  typedef struct node
  {
    vertex_t val;
    int group;
    node *left;
    node *right;
  }tnode;
  
  class heap
  {
    tnode * root;
  public:
    heap(){
      root = NULL;
    }
    
    void insert(vertex_t value,int group)
    {
      if (root)
        insert(value,group,root);
      else
      {
        root = (tnode *)malloc(sizeof(tnode));
        root->val = value;
        root->group = group;
        root->left = NULL;
        root->right = NULL;
      }
    }
    
    void insert(vertex_t value,int group,tnode* parent)
    {
      if (value < parent->val)
      {
        if (parent->left)
          insert(value,group,parent->left);
        else
        {
          parent->left = (tnode *)malloc(sizeof(tnode));
          parent->left->val = value;
          parent->left->group = group;
          parent->left->left = NULL;
          parent->left->right = NULL;
        }
      }
      else
      {
        if (parent->right)
          insert(value,group,parent->right);
        else
        {
          parent->right = (tnode *)malloc(sizeof(tnode));
          parent->right->val = value;
          parent->right->group = group;
          parent->right->left = NULL;
          parent->right->right = NULL;
        }
      }
    }
    
    int remove_min() //returns group
    {
      int group = 0;
      tnode *tmp = root,*del;
      if (!root->left)
      {
        group = root->group;
        del = root;
        root = root->right;
        free(del);
        return group;
      }
      do
      {
        if (!tmp->left->left)
        {
          del = tmp->left;
          group = tmp->left->group;
          tmp->left = tmp->left->right;
          free(del);
          break;
        }
        tmp = tmp->left;
      }while(tmp->left);
      return group;
    }
  };
  
  template <typename EdgeType>
  class kway
  {
    
    typedef struct chunks
    {
      std::thread * cthreads;
      EdgeType ** thread_chunk;
      int active_threads;
      
      chunks(int size)
      {
        init_chunks(size);
      }
      
      ~chunks()
      {
        destroy_chunks();
      }
      
      void init_chunks(int size)
      {
        cthreads = new std::thread[NUMBER_OF_THREADS];
        thread_chunk = (EdgeType**) malloc(NUMBER_OF_THREADS*sizeof(EdgeType *));
        for (int i = 0;i < NUMBER_OF_THREADS; i++){
          thread_chunk[i] = (EdgeType*) malloc(size*sizeof(EdgeType));
        }
        active_threads = 0;
      }
      
      void destroy_chunks()
      {
        int i;
        delete[] cthreads;
        for(i = 0; i < NUMBER_OF_THREADS; i++)
          free(thread_chunk[i]);
        free(thread_chunk);
      }
    }chunks;
    
    
    chunks * ch;
    int32_t * fd;
    uint32_t * chunk_size;
    int cur_buffer;
    EdgeType * chunk_array;
    heap hHeap;
    
    static bool Comparator(const EdgeType &a, const EdgeType &b)
    {
      return a.dst < b.dst;
    }
    
    void init_fd()
    {
      std::string chunkFileName = inFolder+"Chunks/chunk_";
      fd = (int32_t*) malloc(NUMBER_OF_THREADS*sizeof(int32_t));
      for (int32_t i = 0; i < NUMBER_OF_THREADS; i++){
        fd[i] = open((chunkFileName + std::to_string(i)).c_str(), O_RDWR | O_CREAT | O_TRUNC , 0777);
        if (fd[i] == -1) handle_error(("opening "+ chunkFileName + std::to_string(i)).c_str());
      }
    }
    
    void destroy_fd()
    {
      for (int i = 0; i < NUMBER_OF_THREADS; i++)
        close(fd[i]);
      free(fd);
    }
    
  public:
    
    uint32_t m;  //m: variable of kway merge sort
    
    kway()
    {
      m = 1024 * block_size * 64 / sizeof(EdgeType);
      chunk_array = (EdgeType*) malloc(m * sizeof(EdgeType));
      chunk_size = (unsigned int*) malloc(NUMBER_OF_THREADS*sizeof(unsigned int));
      for (int i = 0; i < NUMBER_OF_THREADS; i++){
        chunk_size[i] = 0;
      }
      cur_buffer = 0;
      ch = new chunks(m);
      init_fd();
    }
    
    ~kway()
    {
      free(chunk_size);
      free(chunk_array);
      destroy_fd();
      delete ch;
    }
    
    static void read_chunk(char *buffer, size_t number_of_bytes,const uint32_t fd)
    {
      read_sys(buffer,number_of_bytes, fd);
    }
    
    void mergesort()
    {
      std::vector<std::vector<std::vector<EdgeType>>> tapes(2,std::vector<std::vector<EdgeType>>(NUMBER_OF_THREADS,std::vector<EdgeType>(0)));
      uint32_t i,j,inp,outp,* count[2],*entries_left[2],*cur_block[2],token,cur_run = 0;
      uint64_t buf_num = 0, iterations, curin_size;
      
      DBG_LOG("Number of edges = %llu\n",(unsigned long long) edges_num);
      
      inp = 0; //input & output buffers for current phase
      outp = 1;
      curin_size = m;
      if ((buf_num = edges_num / curin_size) >= NUMBER_OF_THREADS){
        buf_num = NUMBER_OF_THREADS;
      }
      else{
        if (edges_num % curin_size)
          buf_num += 1;
      }
      
      count[inp] = (unsigned int *) calloc(buf_num,sizeof(unsigned int));
      count[outp] = (unsigned int *) calloc(buf_num,sizeof(unsigned int));
      
      entries_left[inp] = (unsigned int *) malloc(buf_num*sizeof(unsigned int));
      entries_left[outp] = (unsigned int *) calloc(buf_num,sizeof(unsigned int));
      
      cur_block[inp] = (unsigned int *) calloc(buf_num,sizeof(unsigned int));
      cur_block[outp] = (unsigned int *) calloc(buf_num,sizeof(unsigned int));
      timer.start("read_chunks");
      for (i = 0; i < buf_num;i++)
      {
        //get chunks size
        entries_left[inp][i] = (unsigned int) lseek(fd[i], 0, SEEK_CUR)/sizeof(EdgeType);
        DBG_LOG("Chunk size: %u\n",entries_left[inp][i]);
        
        lseek(fd[i],0,SEEK_SET);
        //read edges from file
        tapes[inp][i].resize(entries_left[inp][i]);
        
        read_chunk(reinterpret_cast<char *>(tapes[inp][i].data()),entries_left[inp][i]*sizeof(EdgeType),fd[i]);
      }
      timer.end("read_chunks");
      //initialize number of iterations
      while(1)
      {
        if (buf_num < NUMBER_OF_THREADS)
          iterations = 1;
        else
        {
          if (edges_num%(buf_num*curin_size))
            iterations = edges_num/(buf_num*curin_size)+1;
          else
            iterations = edges_num/(buf_num*curin_size);
          
        }
        while(iterations--)
        {
          //find current block size
          for (i = 0; i < buf_num;i++)
          {
            if (entries_left[inp][i] == 0)
            {
              tapes[inp][i].shrink_to_fit();
              cur_block[inp][i] = 0;
              count[inp][i] = 0;
              break;
            }
            if (entries_left[inp][i] >= curin_size)
            {
              cur_block[inp][i] += curin_size;
              entries_left[outp][cur_run] += curin_size;
              entries_left[inp][i] -= curin_size;
            }
            else
            {
              cur_block[inp][i] += entries_left[inp][i];
              entries_left[outp][cur_run] += entries_left[inp][i];
              entries_left[inp][i] = 0;
            }
            hHeap.insert(tapes[inp][i][count[inp][i]].dst,i);
          }
          j = (unsigned int) tapes[outp][cur_run].size();
          tapes[outp][cur_run].resize(entries_left[outp][cur_run]);

          while(j < entries_left[outp][cur_run]) //merge
          {
            token = hHeap.remove_min();
            tapes[outp][cur_run][j] = tapes[inp][token][count[inp][token]];
            count[inp][token]++;
            if (count[inp][token] < cur_block[inp][token])
              hHeap.insert(tapes[inp][token][count[inp][token]].dst,token);
            j++;
          }
          if (cur_run == buf_num-1)
            cur_run = 0;
          else
            cur_run++;
        }
        for (uint32_t i = 0; i < (uint32_t)tapes[inp].size(); i++){
          tapes[inp][i].shrink_to_fit();
        }
        for (uint32_t i = 0; i < buf_num; i++){
          cur_block[inp][i] = 0;
          count[inp][i] = 0;
        }
        if (tapes[outp][0].size() == edges_num)
          break;
        cur_run = 0;
        inp = 1 - inp;
        outp = 1 - outp;
        curin_size *= NUMBER_OF_THREADS;
        std::cout<< curin_size << std::endl;
        CHECK(curin_size != 0);
        if ((buf_num = edges_num/curin_size) >= NUMBER_OF_THREADS)
          buf_num = NUMBER_OF_THREADS;
        else
        {
          if (edges_num%curin_size)
            buf_num += 1;
        }
      }
      /* Checking if sort worked */
      LOG("Size of output = [%lu]\n",tapes[outp][0].size());
      vertex_t previous = tapes[outp][0][0].dst;
      for (i = 1 ; i < tapes[outp][0].size();i++)
      {
        CHECK(previous <= tapes[outp][0][i].dst);
        previous = tapes[outp][0][i].dst;
      }
      /* freeing memory */
      for (uint32_t i = 1; i < (uint32_t) tapes[outp].size();i++){
        tapes[outp][i].shrink_to_fit();
      }
      tapes[inp].shrink_to_fit();
      
      free(count[0]);
      free(count[1]);
      free(entries_left[0]);
      free(entries_left[1]);
      free(cur_block[0]);
      free(cur_block[1]);
      
      timer.end("MergeSort");
      CalculateIntervals(tapes[outp][0]);
      tapes[outp][0].shrink_to_fit();
      tapes[outp].shrink_to_fit();
      return;
    }
    
    static void write_chunk(char * buffer, size_t number_of_bytes,const uint32_t fd)
    {
      write_sys(buffer, number_of_bytes, fd);
    }
    
    static void sort_chunk(EdgeType * edges_chunk,uint32_t size,uint32_t fd)
    {
      std::sort(edges_chunk,edges_chunk+size,Comparator);
      write_chunk(reinterpret_cast<char*>(edges_chunk), size*sizeof(EdgeType), fd);
    }
    
    void add_chunk()
    {
      int i,thid;
      if (ch->active_threads == NUMBER_OF_THREADS)
      {
        for (i = 0; i < NUMBER_OF_THREADS;i++){
          ch->cthreads[i].join();
          LOG("Chunk thread %d joined\n",i);
        }
        ch->active_threads = 0;
      }
      thid = ch->active_threads;
      memmove(ch->thread_chunk[thid],chunk_array,chunk_size[thid]*sizeof(EdgeType));
      
      ch->cthreads[thid] = std::thread(sort_chunk,ch->thread_chunk[thid],chunk_size[thid],fd[thid]);
      edges_num += chunk_size[thid];

      chunk_size[thid] = 0;
      if (++ch->active_threads == NUMBER_OF_THREADS)
        cur_buffer = 0;
      else
        cur_buffer++;
    }
    
    void addEdgeWithoutValue(vertex_t src, vertex_t dst)
    {
      EdgeType e;
      e.src = src;
      e.dst = dst;
      chunk_array[chunk_size[cur_buffer]++] = e;
      if (chunk_size[cur_buffer] == m){
        add_chunk();
      }
    }
    
    void addEdgeWithValue(vertex_t src, vertex_t dst, value_t value)
    {
      EdgeType e;
      e.src = src;
      e.dst = dst;
      e.value = value;
      
      chunk_array[chunk_size[cur_buffer]++] = e;
      if (chunk_size[cur_buffer] == m){
        add_chunk();
      }
    }
    
    void endFirstPhase()
    {
      for (int i = 0; i < ch->active_threads;i++){
        ch->cthreads[i].join();
        LOG("Chunk thread %d joined\n",i);
      }
      timer.start("MergeSort");
      mergesort();
    }
  };
}
#endif /* externalsorting */
