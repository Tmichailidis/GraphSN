/*
  core.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include <cstdlib>
#include <dirent.h>
#include <functional>

#include "log.hpp"
#include "files.hpp"
#include "graph_program.hpp"

#ifdef __APPLE__
#include <libiomp/omp.h>
#else
#include <omp.h>
#endif

namespace GraphSN {

  /* variables */
  off_t fsize;
  Timer timer;
  const char *infile;
  blksize_t block_size;
  uint8_t number_of_cores, in_format;
  uint16_t intervals_number = 0;
  uint32_t vertices_number;
  uint64_t edges_num;
  std::string inFolder;
  std::string edge_data_filename;
  std::string shard_filename;
    
  void GraphSNInit(int argc,const char **args)
  {
    struct stat sb;
    int fd;
    if (argc != 2){
      LOG("Invalid number of arguments: Expected: ./[exec] [file_name]\nExiting...\n");
      exit(1);
    }
    infile = args[1];
    inFolder.append("Files/");
    inFolder.append(infile);
    inFolder.append(".Folder/");
    edge_data_filename = inFolder + "EdgeData/edgedata_";
    shard_filename = inFolder + "Shards/shard_";
    in_format = 0; //0: edgelist
    fd = open(infile, O_RDONLY);
    number_of_cores = (int) sysconf( _SC_NPROCESSORS_ONLN );   //number of cores
    if (fd == -1){
      handle_error("opening file in core");
    }
    if (fstat(fd, &sb)==-1){
      handle_error("fstat file in core");
    }
    edges_num = 0;
    fsize = sb.st_size;
    block_size = sb.st_blksize;
    umask(000);

    println("\n============ GENERAL INFORMATION ============");
    println("Number of cores: %d",number_of_cores);
    println("File : %s",infile);
    println("Block size: %d",(int)block_size);
    println("File size: %lld",(long long int)fsize);
    println("=============================================\n");
    close(fd);

    /*required calls for setup*/
    InitRequiredDirectories(inFolder);
  }
}
