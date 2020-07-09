#ifndef GRAPHELEMENT_H
#define GRAPHELEMENT_H

/*
#include <cstdlib>
#include <sstream>
#include <cassert>
#include "common/tabletuple.h"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/FatalException.hpp"
*/
//#include "TableTuple.h"
#include "GraphView.h"

#ifndef NDEBUG
//#include "debuglog.h"
#endif /* !define(NDEBUG) */
namespace voltdb {

class GraphElement
{
protected:
	// int m_id;
	std::string m_id; // implement by LX, make id to be label::id
	char* m_tupleData;
	GraphView* m_gview;
	bool m_isRemote;
public:
	GraphElement(void);
	// GraphElement(int id, char* tupleData, GraphView* graphView, bool remote);
	GraphElement(std::string id, char* tupleData, GraphView* graphView, bool remote); //implement by LX FEAT2, make id to be label::id
	~GraphElement(void);

	// void setId(int id);
	void setId(std::string id); //implement by LX FEAT2
	// int getId();
	std::string getId(); //implement by LX FEAT2
	void setTupleData(char* tupleData);
	void setGraphView(GraphView* gView);
	char* getTupleData();
	GraphView* getGraphView();
	bool isRemote();
};

}

#endif
