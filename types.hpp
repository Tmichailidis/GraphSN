/*
  types.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef types_hpp
#define types_hpp

namespace GraphSN {
  
  /* typedef */
  typedef uint32_t vertex_t;
  typedef uint32_t degree_t;
  typedef uint32_t index_t;
  typedef double value_t;
  
  /* structs */
  typedef struct EdgeWithValue_s {
    vertex_t src;
    vertex_t dst;
    value_t  value;
  }EdgeWithValue_t;
  
  typedef struct EdgeWithoutValue_s {
    vertex_t src;
    vertex_t dst;
  }EdgeWithoutValue_t;
  
  typedef struct DegreeData_s{
    vertex_t vID;
    degree_t degree;
  }DegreeData_t;
  
  typedef struct Outbound_s {
    vertex_t vID;
    index_t index;
  }Outbound_t;
  
  typedef struct VertexData_s{
    vertex_t ID;
    value_t  value;
  }VertexData_t;
  
  typedef struct Edge_s{
    vertex_t ID;
    value_t  value;
  }Edge_t;
  
  typedef struct Interval_s{
    vertex_t first_vid, last_vid; /* first and last destination vertex id */
    uint32_t destinations_num;
  }Interval_t;
  
  value_t (* data_funct)(value_t, value_t);
}
#endif /* types_hpp */
