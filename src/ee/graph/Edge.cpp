#include "Edge.h"
#include <string>
#include <sstream>

using namespace std;

namespace voltdb {


Edge::Edge(void)
{
}

// int Edge::getStartVertexId()
unsigned Edge::getStartVertexId() // LX FEAT2
{
	return m_startVertexId;
}
	
// int Edge::getEndVertexId()
unsigned Edge::getEndVertexId() // LX FEAT2
{
	return m_endVertexId;
}
	
Vertex* Edge::getStartVertex()
{
	return this->m_gview->getVertex(this->m_startVertexId);
}
	
Vertex* Edge::getEndVertex()
{
	return this->m_gview->getVertex(this->m_endVertexId);
}

string Edge::toString()
{
	std::ostringstream stream;
	stream << "(id = " << this->getId()
		   << ", eProp = " << this->eProp
			<< ", from = " << this->m_startVertexId << ", to = " <<
		this->m_endVertexId << ")";
	return stream.str();
}

// void Edge::setStartVertexId(int id)
void Edge::setStartVertexId(unsigned id) // LX FEAT2
{
	this->m_startVertexId = id;
}

// void Edge::setEndVertexId(int id)
void Edge::setEndVertexId(unsigned id) // LX FEAT2
{
	this->m_endVertexId = id;
}

Edge::~Edge(void)
{
}

}
