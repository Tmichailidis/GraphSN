#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>

using namespace std;

int main (int argc, char * argv[])
{
  string line;
  ifstream infile(argv[1]);
  unordered_map<int,int> vertices_map;
  int a,b;
  
  if (argc != 2){
    cout << "File missing" << endl;
    return 1;
  }
  if (!infile.is_open()){
    cout << "File not opened" << endl;
    return 1;
  }

  while( infile >> a >> b){
    vertices_map[a]++;
  }
  cout << "Number of vertices: " << vertices_map.size() << endl;
}