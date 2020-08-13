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
	unsigned m_startVertexId; // LX FEAT2
	unsigned m_endVertexId; // LX FEAT2

public:
	Edge(void);
	~Edge(void);

	// int getStartVertexId();
	// int getEndVertexId();
	// void setStartVertexId(int id);
	// void setEndVertexId(int id);
	unsigned getStartVertexId(); // LX FEAT2
	unsigned getEndVertexId(); // LX FEAT2
	void setStartVertexId(unsigned id); // LX FEAT2
	void setEndVertexId(unsigned id); // LX FEAT2
	Vertex* getStartVertex();
	Vertex* getEndVertex();
	string toString();

	int eProp; //temporary, used for selectivity testing
};

}

#endif
