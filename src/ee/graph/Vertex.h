#ifndef VERTEX_H
#define VERTEX_H

#include "GraphElement.h"
#include "Edge.h"
#include <vector>
using namespace std;

namespace voltdb {

class Vertex 
	: public GraphElement
{
	class GraphView;
	friend class GraphView;
protected:
	//mohamed: we are using ids instead of pointers for two reasons
	// 1) this will abstract referencing the graph elements from thier memory locations (e.g., we can easily relocate the edges in the memory)
	// 2) graph elements may be hosted by other remote machines
	// vector<int> m_outEdgeIds;
	// vector<int> m_inEdgeIds;
	vector<unsigned> m_outEdgeIds; // LX FEAT2
	vector<unsigned> m_inEdgeIds; // LX FEAT2

public:
	Vertex(void);
	~Vertex(void);

	int fanOut();
	int fanIn();
	// int getOutEdgeId(int index);
	// int getInEdgeId(int index);
	// Edge* getOutEdge(int index);
	// Edge* getInEdge(int index);
	unsigned getOutEdgeId(int index); // LX FEAT2
	unsigned getInEdgeId(int index); // LX FEAT2
	Edge* getOutEdge(int index); // LX FEAT2
	Edge* getInEdge(int index); // LX FEAT2
	vector<unsigned> getAllOutEdges();
	vector<unsigned> getAllInEdges();
	
	void addOutEdge(Edge* edge);
	void addInEdge(Edge* edge);
	string toString();

	int vProp; //temporary, used for selectivity testing
	int Level; //used for BFS
};

}

#endif
