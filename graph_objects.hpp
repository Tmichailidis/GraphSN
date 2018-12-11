/*
  graph_objects.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef graph_objects_hpp
#define graph_objects_hpp

#include <mutex>
#include "scheduler.hpp"

namespace GraphSN {
  
  class Locks{
    uint32_t num_of_vertices;
    uint32_t edges_per_lock;
    std::mutex * arrMutex;
    
  public:
    
    Locks(uint32_t lock_edges, uint32_t vertices_num): edges_per_lock(lock_edges)
    {
      uint32_t mutexes_num;
      
      num_of_vertices = vertices_num;
      mutexes_num = num_of_vertices / edges_per_lock + (num_of_vertices % edges_per_lock != 0);
      arrMutex = new std::mutex[mutexes_num];
      DBG_LOG("Vertices: %u and Mutexes: %u\n", num_of_vertices, mutexes_num);
    }
    
    ~Locks()
    {
      delete [] arrMutex;
    }
    
    void lock(vertex_t index)
    {
      arrMutex[index / edges_per_lock].lock();
    }
    
    void unlock(vertex_t index)
    {
      arrMutex[index / edges_per_lock].unlock();
    }
  };
  
  class GraphVertex;
  
  class GraphEdge{
    
    value_t * data;
    GraphVertex * vertex;
    
  public:
    
    GraphEdge(GraphVertex * vertex, value_t * data): data(data), vertex(vertex){}
    
    void setData(value_t data);
    value_t getData();
    vertex_t getID();
    GraphVertex * getVertex();
  };
  
  class GraphVertex{
    
    vertex_t      ID;
    value_t     * data;
    Locks       * hLocks;
    GraphEdge   * inedges, * outedges;
    degree_t    inedges_degree, outedges_degree;
    
  public:
    
    GraphVertex(vertex_t ID, value_t * data, Locks * hLocks): ID(ID), data(data), hLocks(hLocks),
    inedges(NULL), outedges(NULL), inedges_degree(0), outedges_degree(0){}
    
    ~GraphVertex()
    {
      if (getIndegree()){
        for (uint32_t i = 0; i < getIndegree(); i++){
          inedges[i].~GraphEdge();
        }
        free(inedges);
      }
      if (getOutdegree()){
        for (uint32_t i = 0; i < getOutdegree(); i++){
          outedges[i].~GraphEdge();
        }
        free(outedges);
      }
    }
    
    value_t getData()
    {
      value_t data;
      hLocks->lock(this->ID);
      data = *this->data;
      hLocks->unlock(this->ID);
      return data;
    }
    
    vertex_t getID()
    {
      return ID;
    }
    
    degree_t getIndegree()
    {
      return this->inedges_degree;
    }
    
    degree_t getOutdegree()
    {
      return this->outedges_degree;
    }
    
    GraphEdge inedge(index_t index)
    {
      CHECK(index < inedges_degree);
      return this->inedges[index];
    }
    
    GraphEdge outedge(index_t index)
    {
      CHECK(index < outedges_degree);
      return this->outedges[index];
    }
    
    GraphEdge edge(index_t index)
    {
      CHECK(index < inedges_degree + outedges_degree);
      return (index < inedges_degree) ? inedges[index]: outedges[index - inedges_degree];
    }
    
    void setData(value_t data)
    {
      hLocks->lock(this->ID);
      *(this->data) = data;
      hLocks->unlock(this->ID);
    }
    
    degree_t num_edges()
    {
      return inedges_degree + outedges_degree;
    }
    
    void reinitialize()
    {
      inedges_degree = 0;
      outedges_degree = 0;
    }
    
    void allocateInedges(degree_t degree)
    {
      inedges = (GraphEdge *) realloc(inedges, degree * sizeof(GraphEdge));
    }
    
    void allocateOutedges(degree_t degree)
    {
      outedges = (GraphEdge *) realloc(outedges, degree * sizeof(GraphEdge));
    }
    
    inline void addInedge(GraphVertex * source_vertex, value_t * edgeData)
    {
      uint32_t position = __sync_add_and_fetch(&inedges_degree, 1) - 1;
      inedges[position] = GraphEdge(source_vertex, edgeData);
    }
    
    inline void addOutedge(GraphVertex * dest_vertex, value_t * edgeData)
    {
      uint32_t position = __sync_add_and_fetch(&outedges_degree, 1) - 1;
      outedges[position] = GraphEdge(dest_vertex, edgeData);
    }
  };
  
  void GraphEdge::setData(value_t data){
    *this->data = data;
  }
  
  value_t GraphEdge::getData()
  {
    return *this->data;
  }
  
  GraphVertex * GraphEdge::getVertex()
  {
    return this->vertex;
  }
  
  vertex_t GraphEdge::getID()
  {
    return this->vertex->getID();
  }
  
  class GraphBox{
    uint32_t    current_iteration;
    uint32_t    iterations_num;
    
  public:
    
    Scheduler * scheduler;
    
    GraphBox(uint32_t iterations_num, uint32_t vertices_number): current_iteration(0), iterations_num(iterations_num)
    {
      CHECK(vertices_number);
      this->scheduler = new Scheduler(vertices_number);
    }
    
    ~GraphBox()
    {
      delete this->scheduler;
    }
    
    uint32_t get_current_iteration()
    {
      return this->current_iteration;
    }
    
    uint32_t get_iterations_num()
    {
      return this->iterations_num;
    }
    
    void increment_iteration()
    {
      this->current_iteration++;
    }
    
    void set_last_iteration(uint32_t iteration)
    {
      this->iterations_num = iteration;
    }
  };
}

#endif /* graph_objects_hpp */
