/*
  log.hpp
  Thesis
  Copyright © 2016 Theodore Michailidis. All rights reserved.
*/

#ifndef log_hpp
#define log_hpp

#include <stdio.h>
#include <stdarg.h>
#include <cstring>
#include <signal.h>

#define SILENCE (void)
#define nop asm ("nop");
#define variadic(...) #__VA_ARGS__
#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define RESET       "\033[0m"
#define BLACK       "\033[30m"             /* Black */
#define RED         "\033[31m"             /* Red */
#define GREEN       "\033[32m"             /* Green */
#define YELLOW      "\033[33m"             /* Yellow */
#define BLUE        "\033[34m"             /* Blue */
#define MAGENTA     "\033[35m"             /* Magenta */
#define CYAN        "\033[36m"             /* Cyan */
#define WHITE       "\033[37m"             /* White */
#define BOLDBLACK   "\033[1m\033[30m"      /* Bold Black */
#define BOLDRED     "\033[1m\033[31m"      /* Bold Red */
#define BOLDGREEN   "\033[1m\033[32m"      /* Bold Green */
#define BOLDYELLOW  "\033[1m\033[33m"      /* Bold Yellow */
#define BOLDBLUE    "\033[1m\033[34m"      /* Bold Blue */
#define BOLDMAGENTA "\033[1m\033[35m"      /* Bold Magenta */
#define BOLDCYAN    "\033[1m\033[36m"      /* Bold Cyan */
#define BOLDWHITE   "\033[1m\033[37m"      /* Bold White */

#define DBG_COLOR YELLOW
#define CHECK_COLOR RED
#define LOG_COLOR CYAN
#define PRN_COLOR GREEN
#define term getenv("TERM")

#define width1 20
#define width2 25

#define handle_error(msg) do { printf("[%-*s%-*s%4d]  ",width1,__FILENAME__,width2,__FUNCTION__,__LINE__); perror(msg); exit(EXIT_FAILURE); } while (0)

#define print(fmt,...)                                                      \
  do{                                                                       \
    if (term) fprintf(stderr,PRN_COLOR);                                    \
    fprintf(stderr,fmt, ##__VA_ARGS__);                                     \
    if (term) fprintf(stderr, RESET);                                       \
  }while(0)

#define println(fmt,...)                                                    \
  do{                                                                       \
    if (term) fprintf(stderr,PRN_COLOR);                                    \
    fprintf(stderr,fmt, ##__VA_ARGS__);                                     \
    fprintf(stderr,"\n");                                                   \
    if (term) fprintf(stderr, RESET);                                       \
  }while(0)

#define LOG(fmt,...)                                                                \
  do{                                                                               \
      if (term) printf(LOG_COLOR);                                                  \
      printf("[%-*s%-*s%4d]  ",width1,__FILENAME__,width2,__FUNCTION__,__LINE__);   \
      printf(fmt, ##__VA_ARGS__);                                                   \
      if (term) printf(RESET);                                                      \
  }while(0)

#define CHECK(expr)                                                                       \
      if (!(expr)) {                                                                      \
          if (term) fprintf(stderr, CHECK_COLOR);                                         \
          fprintf(stderr,                                                                 \
          "\nAssertion failed : File [%s], Line [%u], Function [%s]\nExpression:[%s] \n"  \
            ,__FILENAME__, __LINE__, __FUNCTION__,#expr);                                 \
          if (term) fprintf(stderr, RESET);                                               \
          raise(SIGTRAP);                                                                 \
}

#ifdef DEBUG
#define DBG_LOG(fmt,...)                                                                  \
      do{ if (term) printf(DBG_COLOR);                                                    \
          printf("[%-*s%-*s%4d]  ",width1,__FILENAME__,width2,__FUNCTION__,__LINE__);     \
          printf(fmt, ##__VA_ARGS__);                                                     \
          if (term) printf(RESET);                                                        \
      }while(0)

#else   //release
    #define DBG_LOG(fmt,...)
#endif //DEBUG endif
#endif /* log_hpp */
