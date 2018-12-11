/*
  files.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef files_hpp
#define files_hpp

#include <string>
#include <iostream>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>

#include "log.hpp"

/**
 * check_file
 *
 * Checks if the required file exists.
 *
 * @param   filename name of the file
 * @return  void
 */
inline static bool check_file (const std::string& filename) {
  struct stat buffer;
  return (stat (filename.c_str(), &buffer) == 0);
}

/**
 * check_directory
 *
 * Checks if the required directory exists. If not, create it.
 *
 * @param   dirName name of the directory
 * @return  void
 */
inline static void check_directory(const char * dirName)
{
  DIR* dir;
  
  dir = opendir(dirName);
  if (dir){    /* Directory exist */
    DBG_LOG("Directory [%s] exists\n",dirName);
    if (closedir(dir) < 0)  handle_error("closing dir");
  }
  else if (ENOENT == errno){   /* Directory does not exist */
    DBG_LOG("Creating directory [%s]\n",dirName);
    if (mkdir(dirName,0777) < 0){
      handle_error("create dir");
    }
  }
  else{
    handle_error("opening dir");
  }
}

/**
 * GetSourceDirName
 *
 * Get the absolute path of the source directory.
 *
 * @return  void
 */
inline static std::string GetSourceDirName()
{
  std::string fname (__FILE__);
  return fname.substr(0, fname.find_last_of("\\/") + 1);
}

/**
 * init_dir
 *
 * Checks if the required directories exist. If not, create them.
 *
 * @param   initialFolderName name of the initial folder
 * @return  void
 */
inline void InitRequiredDirectories(std::string initialFolderName)
{
  const std::string subDir[] = {"Chunks","EdgeData","Shards", "Outbound"};
  uint8_t i;
  check_directory("Files");   //check for rootDir
  check_directory(initialFolderName.c_str()); //check for the input file's folder
  for (i = 0; i < (uint8_t)sizeof(subDir)/sizeof(subDir[0]); i++){
    check_directory((initialFolderName+subDir[i]).c_str()); //check for subfolders
  }
}

/**
 * write_sys
 *
 * Writing buffer to file
 *
 * @param   buffer          Buffer to be written to file
 * @param   number_of_bytes Number of bytes to be written
 * @param   fd              File descriptor
 * @return  void
 */
inline void write_sys(char * buffer, size_t number_of_bytes,const uint32_t fd)
{
  int64_t bytes_written = 0;
  int64_t bytes_remaining = number_of_bytes;
  while (bytes_remaining > 0)
  {
    bytes_written = write(fd, buffer, bytes_remaining);
    if (bytes_written == -1){
      handle_error("writing");
    }
    bytes_remaining -= bytes_written;
    buffer += bytes_written;
  }
}

/**
 * read_sys
 *
 * Reading file to buffer
 *
 * @param   buffer          Buffer to be read to file
 * @param   number_of_bytes Number of bytes to be read
 * @param   fd              File descriptor
 * @return  void
 */
inline void read_sys(char * buffer, size_t number_of_bytes,const uint32_t fd)
{
  int64_t bytes_read = 0;
  int64_t bytes_remaining = number_of_bytes;
  while (bytes_remaining > 0)
  {
    bytes_read = read(fd, buffer, bytes_remaining);
    if (bytes_read == -1){
      handle_error("reading");
    }
    bytes_remaining -= bytes_read;
    buffer += bytes_read;
  }
}

/**
 * pread_sys
 *
 * Reading chunk of file to buffer
 *
 * @param   buffer          Buffer to be read to file
 * @param   number_of_bytes Number of bytes be read
 * @param   fd              File descriptor
 * @return  void
 */
inline void pread_sys(char * buffer, size_t number_of_bytes, const off_t offset, const uint32_t fd)
{
  int64_t bytes_read = 0;
  int64_t bytes_remaining = number_of_bytes;
  while (bytes_remaining > 0)
  {
    bytes_read = pread(fd, buffer, number_of_bytes, offset);
    if (bytes_read == -1){
      handle_error("preading");
    }
    bytes_remaining -= bytes_read;
    buffer += bytes_read;
  }
}

/**
 * pwrite_sys
 *
 * Writing buffer to chunk of file
 *
 * @param   buffer          Buffer to be written to file
 * @param   number_of_bytes Number of bytes
 * @param   fd              File descriptor
 * @return  void
 */
inline void pwrite_sys(char * buffer, size_t number_of_bytes, const off_t offset, const uint32_t fd)
{
  int64_t bytes_written = 0;
  int64_t bytes_remaining = number_of_bytes;
  while (bytes_remaining > 0)
  {
    bytes_written = pwrite(fd, buffer, number_of_bytes, offset);
    if (bytes_written == -1){
      handle_error("writing");
    }
    bytes_remaining -= bytes_written;
    buffer += bytes_written;
  }
}

/**
 * GetFileSize
 *
 * get file size
 *
 * @param   filename  name of file
 * @return  file size
 */
inline long GetFileSize(std::string filename)
{
  struct stat stat_buf;
  int rc = stat(filename.c_str(), &stat_buf);
  CHECK(rc == 0);
  return stat_buf.st_size;
}

/**
 * GetElementsNumber
 *
 * get number of elements in file
 *
 * @param   filename        name of file
 * @param   element_size    size of a single element
 * @return  number of elements in file
 */
inline uint32_t GetElementsNumber(std::string filename, size_t element_size)
{
  return uint32_t (GetFileSize(filename) / element_size);
}

/**
 * TimestampCompare
 *
 * compare modification times of two files
 *
 * @param   first     first filename to compare
 * @param   second    second filename to compare
 * @return  true if first file has been modified later than the second, else false
 */
inline bool ModificationTimeCompare(const char * first, const char * second)
{
  struct stat stat_buffer_first, stat_buffer_second;
  stat(first, &stat_buffer_first);
  stat(second, &stat_buffer_second);
  
  return (std::difftime(stat_buffer_first.st_mtime,stat_buffer_second.st_mtime) > 0);
}

/**
 * CreateMmap
 *
 * creates memory mapping of a file
 *
 * @param   fd            file descriptor
 * @param   filename      name of file
 * @param   bytes         number of bytes
 * @param   offset        offset parameter of memory mapping
 * @return  memory mapped buffer
 */
inline void * CreateMmap(int32_t& fd, std::string filename, size_t bytes, off_t offset)
{
  void * buffer;
  
  fd = open(filename.c_str(), O_RDWR);
  if (fd == -1) handle_error(("opening "+ filename).c_str());
  buffer = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
  if (buffer == MAP_FAILED)  handle_error(("memory mapping " + filename).c_str());
  return buffer;
}

/**
 * CreateFixedMmap
 *
 * given an offset that is not multiple of the page size, this functions creates and adjusted memory mapping
 *
 * @param   fd            file descriptor
 * @param   filename      name of file
 * @param   number_of_elements
 * @param   offset        offset parameter of memory mapping
 * @param   element_size  single element size
 * @return  memory mapped buffer
 */
inline void * CreateFixedMmap(int32_t& fd, std::string filename, int32_t number_of_elements,
                              off_t offset, size_t element_size)
{
  int pagesize;
  uint16_t fixed_start;
  
  /* we need to check if "offset" is multiple of the page size. If it's not, then we have to adjust it */
  pagesize = getpagesize();
  fixed_start = offset % pagesize;
  if (fixed_start){
    offset -= fixed_start;
    number_of_elements += (fixed_start / element_size);
  }
  DBG_LOG("Offset = %llu and bytes = %lu\n",(long long int) offset, number_of_elements * element_size);
  return CreateMmap(fd, filename, number_of_elements * element_size, offset);
}

/**
 * CreateFixedMmap
 *
 * the difference with the previous function is that this one returns fixed_start and modified number_of_elements
 *
 * @param   fd            file descriptor
 * @param   filename      name of file
 * @param   number_of_elements
 * @param   fixed_start   the adjusted start of the memory mapping
 * @param   offset        offset parameter of memory mapping
 * @param   element_size  single element size
 * @return  memory mapped buffer
 */
inline void * CreateFixedMmap(int32_t& fd, std::string filename, int32_t& number_of_elements,
                              off_t offset, uint16_t& fixed_start, size_t element_size)
{
  int pagesize;
  /* we need to check if "offset" is multiple of the page size. If it's not, then we have to adjust it */
  pagesize = getpagesize();
  fixed_start = offset % pagesize;
  if (fixed_start){
    offset -= fixed_start;
    fixed_start /= element_size;
    number_of_elements += fixed_start;
  }
  return CreateMmap(fd, filename, number_of_elements * element_size, offset);
}

/**
 * DestroyMmap
 *
 * Destroys the memory mapping
 *
 * @param   fd            file descriptor
 * @param   filename      name of file
 * @param   buffer        memory mapped buffer
 * @param   bytes         number of bytes
 * @return
 */
inline void DestroyMmap(int32_t& fd, std::string filename, void * buffer, size_t bytes, bool sync = true)
{
  if (sync){
    if (msync(buffer, bytes, MS_SYNC) == -1) handle_error(("msyncing "+ filename).c_str());
  }
  if (munmap(buffer, bytes) == -1) handle_error(("unmapping "+ filename).c_str());
  close(fd);
}




#endif /* files_hpp */
