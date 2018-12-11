CXX=g++
LDFLAGS= -lpthread
CXXFLAGS=-std=c++11 -fopenmp -Wall -Wextra -O3
EXECUTABLE=connectedcomponents shortestdistance

all: $(EXECUTABLE)

debug: CXXFLAGS += -DDEBUG -g
debug: $(EXECUTABLE)

connectedcomponents: connectedcomponents.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

shortestdistance: shortestdistance.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -rf Files/
	rm -f connectedcomponents shortestdistance