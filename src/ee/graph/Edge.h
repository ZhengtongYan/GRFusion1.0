#ifndef EDGE_H
#define EDGE_H

#include "GraphElement.h"
namespace voltdb {

class Edge 
	: public GraphElement
{
	class GraphView;
	friend class GraphView;
protected:
	// int m_startVertexId; 
	// int m_endVertexId;
	string m_startVertexId; // LX FEAT2
	string m_endVertexId; // LX FEAT2

public:
	Edge(void);
	~Edge(void);

	// int getStartVertexId();
	// int getEndVertexId();
	// void setStartVertexId(int id);
	// void setEndVertexId(int id);
	string getStartVertexId(); // LX FEAT2
	string getEndVertexId(); // LX FEAT2
	void setStartVertexId(string id); // LX FEAT2
	void setEndVertexId(string id); // LX FEAT2
	Vertex* getStartVertex();
	Vertex* getEndVertex();
	string toString();

	int eProp; //temporary, used for selectivity testing
};

}

#endif
