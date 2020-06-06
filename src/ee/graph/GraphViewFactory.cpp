#include "GraphViewFactory.h"
#include "catalog/graphview.h"
#include "GraphView.h"
#include "logging/LogManager.h"
#include <iostream>

using namespace std;

namespace voltdb {

GraphViewFactory::GraphViewFactory(){};

GraphView* GraphViewFactory::createGraphView(string graphViewName, bool isDirected)
{
	GraphView* vw = new GraphView();
	vw->m_name = graphViewName;
	vw->m_isDirected = isDirected;
	//vw->constructPathSchema();
	return vw;
}

GraphView* GraphViewFactory::createGraphView(const std::string &graphViewName, const bool isDirected, 
			vector<std::string> vLabels,
		   vector<Table*> vTables, Table* eTable, Table* pTable, TupleSchema* vSchema, TupleSchema* eSchema,
		   vector<std::string> vertexColumnNames, vector<std::string> edgeColumnNames,
		   vector<int> columnIdsInVertexTable, vector<int> columnIdsInEdgeTable,
           voltdb::CatalogId databaseId, char *signature) // LX FEAT2
{
	GraphView* vw = new GraphView();
	vw->m_name = graphViewName;
	vw->m_isDirected = isDirected;
	// vw->m_vertexTable = vTable;
	// LX FEAT2
	int vLableCount = vLabels.size();
	vw->m_vertexLabels.resize(vLableCount);
	for (int i = 0; i < vLableCount; i++){
		vw->m_vertexLabels[i] = vLabels[i];
	}
	int vTableCount = vTables.size();
	vw->m_vertexTables.resize(vTableCount);
	for (int i = 0; i < vTableCount; i++){
		vw->m_vertexTables[i] = vTables[i];
	}

	vw->m_edgeTable = eTable;
	//construct the path schema
	vw->constructPathSchema();
	//construct the path temp table
	vw->constructPathTempTable();

	//set the vertex column names
	//int vColumnCount = vSchema->columnCount();
	int vColumnCount = vertexColumnNames.size();
	vw->m_vertexColumnNames.resize(vColumnCount);

	for(int i = 0; i < vColumnCount; i++)
	{
		vw->m_vertexColumnNames[i] = vertexColumnNames[i];
	}

	int colCountInVTable = columnIdsInVertexTable.size();
	vw->m_columnIDsInVertexTable.resize(colCountInVTable);

	for(int i = 0; i < colCountInVTable; i++)
	{
		vw->m_columnIDsInVertexTable[i] = columnIdsInVertexTable[i];
	}
	LogManager::GLog("GraphViewFactory", "create", 55, to_string(colCountInVTable));
	//set the edges columns
	//int eColumnCount = eSchema->columnCount();
	int eColumnCount = edgeColumnNames.size();
	vw->m_edgeColumnNames.resize(eColumnCount);

	for(int i = 0; i < eColumnCount; i++)
	{
		vw->m_edgeColumnNames[i] = edgeColumnNames[i];
	}

	int colCountInETable = columnIdsInEdgeTable.size();
	vw->m_columnIDsInEdgeTable.resize(colCountInETable);

	for(int i = 0; i < colCountInETable; i++)
	{
		vw->m_columnIDsInEdgeTable[i] = columnIdsInEdgeTable[i];
	}

	vw->m_vPropColumnIndex = -1;
	vw->m_ePropColumnIndex = -1;

	for(int i = 0; i < vw->m_vertexColumnNames.size(); i++)
	{
		// if (vw->m_vertexColumnNames[i] == "ID")
		// LX FEAT2
		int d = vw->m_vertexColumnNames[i].find(".");
		int s = vw->m_vertexColumnNames[i].size();
		if (d == -1) // currently FANIN FANOUT are not given col names 
			continue;
		// cout << "GraphViewFactory:96:" << vw->m_vertexColumnNames[i] << ", " << d << ", " << s << endl;
		if (vw->m_vertexColumnNames[i].substr(d, s-d) == ".ID")
		{
			// vw->m_vertexIdColumnIndex = vw->m_columnIDsInVertexTable[i];
			std::string label = vw->m_vertexColumnNames[i].substr(0, d);
			vw->m_vertexIdColIdxList.insert(std::pair<std::string, int>(label, vw->m_columnIDsInVertexTable[i]));
		}
		
		else if (vw->m_vertexColumnNames[i] == "VPROP")
		{
			vw->m_vPropColumnIndex = vw->m_columnIDsInVertexTable[i];
		}
	}
	//TODO: fix issue by setting vw->m_vertexIdColumnIndex dynamically
	//Fixed
	// vw->m_vertexIdColumnIndex = 0;

	for(int i = 0; i < vw->m_edgeColumnNames.size(); i++)
	{
		if (vw->m_edgeColumnNames[i] == "ID")
		{
			vw->m_edgeIdColumnIndex = vw->m_columnIDsInEdgeTable[i];
		}
		else if (vw->m_edgeColumnNames[i] == "FROM")
		{
			vw->m_edgeFromColumnIndex = vw->m_columnIDsInEdgeTable[i];
		}
		else if (vw->m_edgeColumnNames[i] == "TO")
		{
			vw->m_edgeToColumnIndex = vw->m_columnIDsInEdgeTable[i];
		}
		else if (vw->m_edgeColumnNames[i] == "EPROP")
		{
			vw->m_ePropColumnIndex = vw->m_columnIDsInEdgeTable[i];
		}
	}

	vw->m_databaseId = databaseId;
	::memcpy(&(vw->m_signature), signature, 20);

	vw->fillGraphFromRelationalTables();

	return vw;
}

/*
GraphView* GraphViewFactory::createGraphView(const catalog::GraphView &catalogGraphView,
           voltdb::CatalogId databaseId, Table* vTable, Table* eTable, char *signature)
{
	GraphView* vw = new GraphView();
	vw->m_name = catalogGraphView.name();
	//TODO: msaber should check this with tatiana, the directed attribute is not communicated write from the FE
	vw->m_isDirected = catalogGraphView.isDirected();
	//vw->m_isDirected = true;
	vw->m_vertexTable = vTable;
	vw->m_edgeTable = eTable;
	vw->m_databaseId = databaseId;
	::memcpy(&(vw->m_signature), signature, 20);

	vw->fillGraphFromRelationalTables();

	return vw;
}
*/


void GraphViewFactory::loadGraph(GraphView* vw, vector<Vertex* > vertexes, vector<Edge* > edges)
{
	int vCount = vertexes.size();
	int eCount = edges.size();

	for(int i = 0; i < vCount; i++)
	{
		vertexes[i]->setGraphView(vw);
		vw->addVertex( vertexes[i]->getId(), vertexes[i]);
	}

	Vertex* from;
	Vertex* to;

	for(int i = 0; i < eCount; i++)
	{
		edges[i]->setGraphView(vw);
		vw->addEdge(edges[i]->getId(), edges[i]);
		//update the endpoint vertexes in and out lists
		from = edges[i]->getStartVertex();
		to = edges[i]->getEndVertex();
		from->addOutEdge(edges[i]);
		to->addInEdge(edges[i]);
	}
}

void GraphViewFactory::printGraphView(GraphView* gview)
{
	cout << "Name: " << gview->name() << endl;
	int vCount, eCount;
	vCount = gview->numOfVertexes();
	eCount = gview->numOfEdges();
	cout << "#Vertexes = " << vCount << endl;
	cout << "#Edges = " << eCount << endl;
	cout << "Vertexes" << endl;
	Vertex* currentVertex;
	for(int i = 0; i < vCount; i++)
	{
		currentVertex = gview->getVertex(i);
		cout << "\t" << currentVertex->toString() << endl;
		cout << "\t\t" << "out: " << endl;
		for(int j = 0; j < currentVertex->fanOut(); j++)
		{
			cout << "\t\t\t" << currentVertex->getOutEdge(j)->toString() << endl;
		}
		cout << "\t\t" << "in: " << endl;
		for(int j = 0; j < currentVertex->fanIn(); j++)
		{
			cout << "\t\t\t" << currentVertex->getInEdge(j)->toString() << endl;
		}
	}
}


GraphViewFactory::~GraphViewFactory(void)
{
}

}
