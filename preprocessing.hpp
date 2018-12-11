/*
  preprocessing.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef preprocessing_hpp
#define preprocessing_hpp

#include "iostream"
#include "externalsorting.hpp"
#include <sstream>
#include <fstream>
#include <string>

#define line_err(msg,line) do { std::cout << __LINE__ << ": on line file " << line << msg << '\n'; exit(EXIT_FAILURE); } while (0)

namespace GraphSN {
  
  int has_edge_value;
  int chunk_size;
  
  class Preprocessing
  {
    
    void check_for_values()
    {
      int i,wsc = 0;
      char buffer[100];
      FILE * file = fopen(infile,"r");
      if (file == NULL)
      {
        handle_error("open input file");
        exit(-1);
      }
      i = 0;
      while (fgets(buffer, 100, file))
      {
        if (buffer[0] == '%' || buffer[0] == '#')
          continue;
        else
        {
          while(buffer[i] != '\0')
          {
            if (buffer[i] == '\t' || buffer[i] == ' ')
              wsc++;
            i++;
          }
          break;
        }
      }
      fclose(file);
      if (wsc == 2){
        has_edge_value = 1;
      }
    }
    
    void parse_without_value()
    {
      int fd,st;
      uint64_t i,line_num;
      char line[100];
      char *src,*vertex;
      char delims[] = "\t, ";
      vertex_t in,out;
      kway<EdgeWithoutValue_t> hKway;
      
      fd = open(infile, O_RDONLY);
      if (fd == -1)   handle_error("open input file");
      src = (char *)mmap(NULL, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
      if (src == MAP_FAILED)  handle_error("memory mapping input file");
      
      i = line_num = 0;
      timer.start("Creating Chunks");
      while(i < (uint64_t)fsize)
      {
        line_num++;
        st = 0;
        while(src[i] != '\n'){
          line[st++] = src[i++];
        }
        line[st] = '\0';
        i++;
        if (line[0] == '#' || line[0] == '%') //comment
          continue;
        vertex = strtok(line,delims);
        if (!vertex) line_err("\tWrong input file format\nExpected\"<from>\t<to>\".",line_num);
        in = atoi(vertex);
        
        vertex = strtok(NULL,delims);
        if (!vertex) line_err("\tWrong input file format\nExpected\"<from>\t<to>\".",line_num);
        out = atoi(vertex);
        
        if (strtok(NULL,delims)) line_err("\tWrong input file format\nExpected\"<from>\t<to>\".",line_num);
        hKway.addEdgeWithoutValue(in,out);
      }
      if (line_num%hKway.m){
        hKway.add_chunk();
      }
      if (close(fd) < 0)  handle_error("closing input file");
      if(munmap(src,fsize) == -1) handle_error("unmapping input file");
      timer.end("Creating Chunks");
      hKway.endFirstPhase();
    }
    
    void parse_with_value()
    {
      int fd,st;
      uint64_t i,line_num;
      char line[25];
      char *src, *vertex, *value;
      char delims[] = "\t, ";
      vertex_t  in,out;
      value_t   edge_value;
      kway<EdgeWithValue_t> hKway;
      
      fd = open(infile, O_RDONLY);
      if (fd == -1)   handle_error("open input file");
      src = (char *)mmap(NULL, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
      if (src == MAP_FAILED)  handle_error("memory mapping input file");
      
      i = line_num = 0;
      timer.start("Creating Chunks");
      while(i < (uint64_t)fsize)
      {
        line_num++;
        st = 0;
        while(src[i] != '\n')
          line[st++] = src[i++];
        line[st] = '\0';
        i++;
        if (line[0] == '#' || line[0] == '%') //comment
          continue;
        vertex = strtok(line,delims);
        if (!vertex) line_err("\tWrong input file format\nExpected\"<from>\t<to>\t<value>\".",line_num);
        in = atoi(vertex);
        
        vertex = strtok(NULL, delims);
        if (!vertex) line_err("\tWrong input file format\nExpected\"<from>\t<to>\t<value>\".",line_num);
        out = atoi(vertex);
        
        value = strtok(NULL, delims);
        if (!vertex) line_err("\tWrong input file format\nExpected\"<from>\t<to>\t<value>\".",line_num);
        edge_value = atof(value);
        
        if (strtok(NULL,delims))    line_err("\tWrong input file format\nExpected\"<from>\t<to>\".",line_num);
        hKway.addEdgeWithValue(in, out, edge_value);
      }
      if (line_num%hKway.m){
        hKway.add_chunk();
      }
      if (close(fd) < 0)  handle_error("closing input file");
      if(munmap(src,fsize) == -1) handle_error("unmapping input file");
      timer.end("Creating Chunks");
      
      hKway.endFirstPhase();
    }
    
    void edgelist_input()
    {
      if (has_edge_value){
        parse_with_value();
      }
      else{
        parse_without_value();
      }
    }
    
    void parse()
    {
      if (in_format == 0){ //edgelist
        edgelist_input();
      }
    }
    
  public:
    
    /**
     * CheckPreprocessing
     *
     * Checks if the required directories exist. If not, create them.
     *
     * @param   inFolder name of the initial folder
     * @return  void
     */
    void CheckPreprocessing(std::string inFolder)
    {
      std::string strShardInfo (inFolder+"shards.info");
      std::string strShardBaseName (inFolder+"Shards/shard_");
      std::ifstream fileShardInfo (strShardInfo,std::ifstream::binary);
      
      check_for_values();
      
      if (!check_file(strShardInfo)){
        LOG("File %s does not exist!\n",strShardInfo.c_str());
        goto Parse;
      }
      
      if (ModificationTimeCompare((GetSourceDirName() + "sharder.hpp").c_str(), strShardInfo.c_str())){
        LOG("There have been some changes in \"sharder.hpp\"\n");
        goto Parse;
      }
      
      if (ModificationTimeCompare(infile, strShardInfo.c_str())){
        LOG("There have been some changes in \"%s\"\n", infile);
        goto Parse;
      }
      
      fileShardInfo >> intervals_number;
      DBG_LOG("Number of shards = %u\n",intervals_number);
      
      /* check if every shard file exists */
      for (uint32_t shard = 0; shard < intervals_number; shard++){
        if (!check_file(strShardBaseName+std::to_string(shard))){
          LOG("Something went wrong in shard%u!\n",shard);
          goto Parse;
        }
      }
      /* check if the intervals file exists */
      if (!check_file(inFolder+"intervals.binary")){
        LOG("Intervals file is missing!\n");
        goto Parse;
      }
      LOG("Using shards/intervals from previous run!\n");
      return;
    Parse:
      LOG("Need to preprocess the file\n");
      parse();
    }
    
  };
}

#endif /* preprocessing_hpp */
