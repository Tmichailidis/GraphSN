#include <stdio.h>     
#include <stdlib.h>     
#include <time.h>       

int main (int argc, char * argv[])
{
  int i,num_of_entries;
  char * filename;
  FILE * fp;

  if (argc == 2){
    num_of_entries = 1000;
    filename = argv[1];
  }
  else if (argc == 3){
    num_of_entries = atoi(argv[2]);
    filename = argv[1];
  }
  else{
    printf("Usage: ./executable filename [num_of_entries]");
    return 1;
  }
  printf("\n>>>>>Generating %d edges\n\n",num_of_entries);

  fp = fopen (filename, "w+");
  srand (time(NULL));

  for (i = 0; i < num_of_entries; i++){
    int src, dest;
    src = rand()%num_of_entries;
    dest = rand()%num_of_entries;
    if (src == dest){
      dest = rand()%num_of_entries;
    }
    fprintf(fp,"%d\t%d\n",src,dest);
  }
  fclose(fp);
  printf("Done!\n\n");
  return 0;
}
