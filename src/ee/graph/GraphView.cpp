#include "GraphView.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/TupleSchemaBuilder.h"
#include "logging/LogManager.h"
#include "PathIterator.h"
#include "Vertex.h"
#include "Edge.h"
#include <string>
#include <map>
#include <unordered_set>

#include <queue>
using namespace std;

namespace voltdb
{

GraphView::GraphView(void) //: m_pathIterator(this)
{
	m_pathIterator = new PathIterator(this);
}

float GraphView::shortestPath(int source, int destination, int costColumnId)
{
	//TODO: write real shortest path code that consults the edges table using costColumnId
	return (float)source * destination;
}

std::map<string, Vertex*> GraphView::getVertexMap() 
{
	return this->m_vertexes;
}

// Vertex* GraphView::getVertex(int id)
Vertex* GraphView::getVertex(string id) // LX FEAT2
{
	return this->m_vertexes[id];
}

// LX FEAT2
Table* GraphView::getVertexTableById(string id)
{
	return this->m_idToVTableMap[id];
}

void GraphView::setVertexTableMap(std::string id, Table* table) {
	this->m_idToVTableMap[id] = table;
}

// LX FEAT3
Table* GraphView::getEdgeTableById(string id)
{
	return this->m_idToETableMap[id];
}

// LX FEAT4
void GraphView::addToSubgraphList(string name) {
	this->m_subgraphList.push_back(name);
}

// LX FEAT4
void GraphView::addToSubgraphVertex(std::vector<Vertex*> subgraphVertex) {
	this->m_subgraphVertexList.push_back(subgraphVertex);
}

// LX FEAT4
void GraphView::addToSubgraphEdge(std::vector<Edge*> subgraphEdge) {
	this->m_subgraphEdgeList.push_back(subgraphEdge);
}

// TableTuple* GraphView::getVertexTuple(int id)
TableTuple* GraphView::getVertexTuple(string id) // LX FEAT2
{
	Vertex* v = this->getVertex(id);
	Table* t = this->getVertexTableById(id);
	return new TableTuple(v->getTupleData(), t->schema());
}

// Edge* GraphView::getEdge(int id)
Edge* GraphView::getEdge(string id) // LX FEAT2
{
	return this->m_edges[id];
}

// TableTuple* GraphView::getEdgeTuple(int id)
TableTuple* GraphView::getEdgeTuple(string id) // LX FEAT2
{
	Edge* e = this->getEdge(id);
	Table* t = this->getEdgeTableById(id);
	return new TableTuple(e->getTupleData(), t->schema());
}

// TableTuple* GraphView::getEdgeTuple(char* data)
// {
// 	return new TableTuple(data, this->m_edgeTable->schema());
// }

// void GraphView::addVertex(int id, Vertex* vertex)
void GraphView::addVertex(string id, Vertex* vertex) // LX FEAT2
{
	this->m_vertexes[id] = vertex;
}
	
// void GraphView::addEdge(int id, Edge* edge)
void GraphView::addEdge(string id, Edge* edge) // LX FEAT2
{
	this->m_edges[id] = edge;
}

// Table* GraphView::getVertexTable()
// {
// 	return this->m_vertexTables[0]; // LX FEAT2 keep for debugging purpose
// }

std::vector<Table*> GraphView::getVertexTables() {
	return this->m_vertexTables;
}

std::vector<Table*> GraphView::getEdgeTables() {
	return this->m_edgeTables;
}

std::vector<std::string> GraphView::getVertexLabels() {
	return this->m_vertexLabels;
}

std::vector<std::string> GraphView::getEdgeLabels() {
	return this->m_edgeLabels;
}

Table* GraphView::getVertexTableByIndex(int idx) {
	return this->m_vertexTables[idx];
}

Table* GraphView::getEdgeTableByIndex(int idx) {
	return this->m_edgeTables[idx];
}

std::string GraphView::getVertexLabelByIndex(int idx) {
	return this->m_vertexLabels[idx];
}

std::string GraphView::getEdgeLabelByIndex(int idx) {
	return this->m_edgeLabels[idx];
}


// LX FEAT2
Table* GraphView::getVertexTableFromLabel(string vlabel)
{
	for (int i = 0; i < (this->m_vertexLabels).size(); i++){
		if (((this->m_vertexLabels[i]).compare(vlabel)) == 0)
			return this->m_vertexTables[i];
	}
	return NULL;
}

// LX FEAT3
Table* GraphView::getEdgeTableFromLabel(string elabel)
{
	for (int i = 0; i < (this->m_edgeLabels).size(); i++){
		if (((this->m_edgeLabels[i]).compare(elabel)) == 0)
			return this->m_edgeTables[i];
	}
	return NULL;
}

// Table* GraphView::getEdgeTable()
// {
// 	return this->m_edgeTable;
// }

int GraphView::getVertexIdColIdxList(std::string label) {
	return this->m_vertexIdColIdxList[label];
}

Table* GraphView::getPathTable()
{
	return this->m_pathTable;
}

// LX FEAT4
Table* GraphView::getGraphTable()
{
	return this->m_graphTable;
}

int GraphView::numOfVertexes()
{
	return this->m_vertexes.size();
}

TupleSchema* GraphView::getVertexSchema()
{
	return m_vertexSchema;
}

TupleSchema* GraphView::getEdgeSchema()
{
	return m_edgeSchema;
}

TupleSchema* GraphView::getPathSchema()
{
	return m_pathSchema;
}

// LX FEAT4
TupleSchema* GraphView::getGraphSchema()
{
	return m_graphSchema;
}

void GraphView::setVertexSchema(TupleSchema* s)
{
	m_vertexSchema = s;
}

void GraphView::setEdgeSchema(TupleSchema* s)
{
	m_edgeSchema = s;
}

void GraphView::setPathSchema(TupleSchema* s)
{
	m_pathSchema = s;
}

// LX FEAT4
void GraphView::setGraphSchema(TupleSchema* s)
{
	m_graphSchema = s;
}
	
int GraphView::numOfEdges()
{
	return this->m_edges.size();
}

string GraphView::name()
{
	return m_name;
}
	
bool GraphView::isDirected()
{
	return m_isDirected;
}

int GraphView::getVertexIdColumnIndex()
{
	return m_vertexIdColumnIndex;
}

int GraphView::getEdgeIdColumnIndex()
{
	return m_edgeIdColumnIndex;
}

int GraphView::getEdgeFromColumnIndex()
{
	return m_edgeFromColumnIndex;
}

int GraphView::getEdgeToColumnIndex()
{
	return m_edgeToColumnIndex;
}

int GraphView::getColumnIdInVertexTable(int vertexAttributeId)
{
	// -1 means FanOut
	// -2 means FanIn
	// -3 invalid
	// >= 0 means columnIndex
	//int numOfVertexTableColumns = this->m_vertexTable->columnCount();
	//if(vertexAttributeId >= numOfVertexTableColumns)
	return m_columnIDsInVertexTable[vertexAttributeId];
}

int GraphView::getColumnIdInEdgeTable(int edgeAttributeId)
{
	return m_columnIDsInEdgeTable[edgeAttributeId];
}

string GraphView::getVertexAttributeName(int vertexAttributeId)
{
	return m_vertexColumnNames[vertexAttributeId];
}

string GraphView::getEdgeAttributeName(int edgeAttributeId)
{
	return m_edgeColumnNames[edgeAttributeId];
}

void GraphView::constructPathSchema()
{
	//
	//Path tuple will contain 5 attributes
	//0: StartVertex, Integer
	//1: EndVertex, Integer
	//2: Length, Integer
	//3: Cost, Float
	//4: Path: Varchar(256)
	//add the column names
	m_pathColumnNames.clear();
	m_pathColumnNames.push_back("STARTVERTEXID");
	m_pathColumnNames.push_back("ENDVERTEXID");
	m_pathColumnNames.push_back("LENGTH");
	m_pathColumnNames.push_back("COST");
	m_pathColumnNames.push_back("PATH");
	int numOfPathColumns = m_pathColumnNames.size();

	bool needsDRTimestamp = false; //TODO: we might revisit this
	TupleSchemaBuilder schemaBuilder(numOfPathColumns,
									 needsDRTimestamp ? 1 : 0); // number of hidden columns

	schemaBuilder.setColumnAtIndex (0, ValueType::tINTEGER, 4, false, false); //StartVertex
	schemaBuilder.setColumnAtIndex(1, ValueType::tINTEGER, 4, false, false); //EndVertex
	schemaBuilder.setColumnAtIndex(2, ValueType::tINTEGER, 4, false, false); //Length
	schemaBuilder.setColumnAtIndex(3, ValueType::tDOUBLE, 8, true, false); //Cost
	schemaBuilder.setColumnAtIndex(4, ValueType::tVARCHAR, 1024, true, false); //Path

	m_pathSchema = schemaBuilder.build();
}

void GraphView::constructPathTempTable()
{
	m_pathTable = TableFactory::buildTempTable(m_pathTableName, m_pathSchema, m_pathColumnNames, NULL);
}

// LX FEAT4
void GraphView::constructGraphSchema()
{
	m_graphColumnNames.clear();
	m_graphColumnNames.push_back("EDGEID");
	m_graphColumnNames.push_back("FROMVID");
	m_graphColumnNames.push_back("TOVID");
	int numOfGraphColumns = m_graphColumnNames.size();

	bool needsDRTimestamp = false; //TODO: we might revisit this
	TupleSchemaBuilder schemaBuilder(numOfGraphColumns,
									 needsDRTimestamp ? 1 : 0); // number of hidden columns

	schemaBuilder.setColumnAtIndex(0, ValueType::tVARCHAR, 1024, true, false); 
	schemaBuilder.setColumnAtIndex(1, ValueType::tVARCHAR, 1024, true, false); 
	schemaBuilder.setColumnAtIndex(2, ValueType::tVARCHAR, 1024, true, false); 

	m_graphSchema = schemaBuilder.build();
}

// LX FEAT4
void GraphView::constructGraphTempTable()
{
	m_graphTable = TableFactory::buildTempTable(m_graphTableName, m_graphSchema, m_graphColumnNames, NULL);
}

PathIterator* GraphView::iteratorDeletingAsWeGo(GraphOperationType opType) // LX
{
	//empty the paths table, which is the staging memory for the paths to be explored
	dummyPathExapansionState = 0;
	executeTraversal = true;
	m_pathTable->deleteAllTempTupleDeepCopies();
	//set the iterator for the temp table
	//create new tuple in the paths temp table
	// TableIterator tempByLu = m_pathTable->iteratorDeletingAsWeGo();
	// m_pathTableIterator = &tempByLu;
	// this->queryType = opType;// LX
	// m_pathTableIterator = &(m_pathTable->iteratorDeletingAsWeGo());
	m_pathTableIterator = NULL;
	return m_pathIterator;
}

PathIterator* GraphView::iteratorDeletingAsWeGo() // LX
{
	//empty the paths table, which is the staging memory for the paths to be explored
	dummyPathExapansionState = 0;
	executeTraversal = true;
	m_pathTable->deleteAllTempTupleDeepCopies();
	//set the iterator for the temp table
	//create new tuple in the paths temp table
	// TableIterator tempByLu = m_pathTable->iteratorDeletingAsWeGo();
	// m_pathTableIterator = &tempByLu;
	// m_pathTableIterator = &(m_pathTable->iteratorDeletingAsWeGo());
	m_pathTableIterator = NULL;
	return m_pathIterator;
}

void GraphView::expandCurrentPathOperation()
{
	//Check the current path operation type, and
	//advance the paths exploration accordingly
	//new entries should be added to the paths temp table
	//adding no new entries means that the exploration is completely done
	//and the iterator will have hasNext evaluated to false
	// stringstream paramsToPrint;
	
	// if(dummyPathExapansionState < 6)
	// {
	// 	//create new tuple in the paths temp table
	// 	TableTuple temp_tuple = m_pathTable->tempTuple();
	// 	//start vertex, end vertex, length, cost, path
	// 	temp_tuple.setNValue(0, ValueFactory::getIntegerValue(dummyPathExapansionState + 6));
	// 	temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dummyPathExapansionState + 11));
	// 	temp_tuple.setNValue(2, ValueFactory::getIntegerValue(dummyPathExapansionState + 16));
	// 	temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)(dummyPathExapansionState + 21)));
	// 	temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
	// 	m_pathTable->insertTempTuple(temp_tuple);
	// 	paramsToPrint << "current tuple count in m_pathTable = " << m_pathTable->activeTupleCount();
	// 	LogManager::GLog("GraphView", "expandCurrentPathOperation", 242,
	// 								paramsToPrint.str());
	// 	paramsToPrint.clear();
	// 	dummyPathExapansionState++;
	// }
	
	LogManager::GLog("GraphView.cpp", "expandCurrentPathOperation", 271, to_string(this->queryType));
	// cout << this->fromVertexId << ", " << this->toVertexId << "," << this->pathLength << endl;
	// this->queryType = 1;
	// this->fromVertexId = 1;
	if(executeTraversal)
	{
		// cout << "graphView:299:executeTraversal" << endl;
		switch(this->queryType)
		{
		//reachability, BFS,...
		case 1: //reachability BFS without selectivity
			this->BFS_Reachability_ByDepth(this->fromVertexId, this->pathLength);
			break;
		case 2: //reachaility BFS with edge selectivity
			this->BFS_Reachability_ByDepth_eSelectivity(this->fromVertexId, this->pathLength, this->eSelectivity);
			break;
		case 3: //reachability BFS with start and end
			this->BFS_Reachability_ByDestination(this->fromVertexId, this->toVertexId);
			break;
		//topological queries
		case 11: //vOnly selectivity
			this->SubGraphLoop(this->pathLength, this->vSelectivity, 100);
			break;
		case 12: //eOnly selectivity
			this->SubGraphLoop(this->pathLength, 100, this->eSelectivity);
					break;
		case 13: //vertex and edge selectivity
			this->SubGraphLoop(this->pathLength, this->vSelectivity, this->eSelectivity);
			break;
		case 14: //start from a specific vertex and allow vertex and edge selectivity
			this->SubGraphLoopFromStartVertex(this->fromVertexId, this->pathLength, this->vSelectivity, this->eSelectivity);
			break;
		//shortest paths
		case 21: //top k shortest paths
			this->SP_TopK(this->fromVertexId, this->toVertexId, this->topK);
			break;
		case 22: //top 1 shortest path with edge selectivity
			this->SP_EdgeSelectivity(this->fromVertexId, this->toVertexId, this->eSelectivity);
			break;
		case 23: //Single source to all vertexes shortest paths
			this->SP_ToAllVertexes_EdgeSelectivity(this->fromVertexId, this->eSelectivity);
			break;
		}
		executeTraversal = false;
	}
}

// void GraphView::SP_TopK(int src, int dest, int k)
void GraphView::SP_TopK(string src, string dest, int k) // LX FEAT2
{
	double minCost = DBL_MAX;
	int foundPathsSoFar = 0;
	//PQEntryWithLength.first is the cost, PQEntryWithLength.second.first is the vertexId, PQEntryWithLength.second.second is the path length
	priority_queue<PQEntryWithLength, vector<PQEntryWithLength>, std::greater<PQEntryWithLength>> pq;
	//map<int, int> costMap;
	//costMap[src] = 0;
	pq.push(make_pair(0, make_pair(src, 0))); //zero cost to reach Vertex from
	Vertex* v = NULL;
	Edge* e = NULL;
	string currVId;
	int fanOut = -1;
	string candVertexId = "";
	
	//upper-bound to avoid loops (considering an average fan-out of 10
	int maxPQOperations = this->numOfVertexes() * 10;
	int iterationNum = 0;
	TableTuple* edgeTuple;
	int length;

	while(!pq.empty())
	{
		//select next vertex to explore
		currVId = ((pair<string, int >)( pq.top().second)).first;
		length = ((pair<string, int >)( pq.top().second)).second;
		minCost = pq.top().first;
		if(currVId == dest)
		{
			foundPathsSoFar++;

			//add a tuple here
			TableTuple temp_tuple = m_pathTable->tempTuple();
			//start vertex, end vertex, length, cost, path
			// temp_tuple.setNValue(0, ValueFactory::getIntegerValue(src));
			// temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dest));
			temp_tuple.setNValue(0, ValueFactory::getStringValue(src)); // LX FEAT2
			temp_tuple.setNValue(1, ValueFactory::getStringValue(dest)); // LX FEAT2
			temp_tuple.setNValue(2, ValueFactory::getIntegerValue(length));
			temp_tuple.setNValue(3, ValueFactory::getDoubleValue(minCost));
			//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
			if(m_pathTable->activeTupleCount() <= 1000)
			m_pathTable->insertTempTuple(temp_tuple);
		}
		iterationNum++;

		if(foundPathsSoFar == k || iterationNum == maxPQOperations)
		{
			break;
		} 
		pq.pop();

		//explore the outgoing vertexes
		v = this->getVertex(currVId);
		fanOut = v->fanOut();
		double edgeCost = 1;
		for(int i = 0; i < fanOut; i++)
		{
			// e = v->getOutEdge(i);
			string es = v->getOutEdgeId(i);
			candVertexId = e->getEndVertex()->getId();

			if (spColumnIndexInEdgesTable >= 0)
			{
				// TODO: fix this later
				// edgeTuple = this->getEdgeTuple(e->getTupleData());
				edgeTuple = this->getEdgeTuple(es);
				edgeCost = ValuePeeker::peekDouble(edgeTuple->getNValue(spColumnIndexInEdgesTable));
			}


			//these lines are commented to allow top k search
			//if ( (costMap.find(candVertexId) == costMap.end()) ||
			//	 (costMap[candVertexId] > minCost + 1) )
			{
				//costMap[candVertexId] = minCost + 1;
				pq.push(make_pair(minCost + edgeCost, make_pair(candVertexId, length+1)));
			}

		}
	}

	stringstream paramsToPrint;
	paramsToPrint << "TopK SP: from = " << src << ", to = " << dest
			<< ", k = " << k
			<< ", foundPaths = " << foundPathsSoFar
			<< ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "TopK_SP", 334, paramsToPrint.str());
}

// void GraphView::SP_ToAllVertexes_EdgeSelectivity(int src, int edgeSelectivity)
void GraphView::SP_ToAllVertexes_EdgeSelectivity(string src, int edgeSelectivity) //LX FEAT2
{
	int minCost = INT_MAX;
	priority_queue<PQEntry, vector<PQEntry>, std::greater<PQEntry>> pq;
	map<string, int> costMap;
	costMap[src] = 0;
	pq.push(make_pair(0, src)); //zero cost to reach Vertex from
	Vertex* v = NULL;
	Edge* e = NULL;
	string currVId;
	int fanOut = -1;
	string candVertexId = "";

	while(!pq.empty())
	{
		//select next vertex to explore
		currVId = pq.top().second;
		minCost = pq.top().first;
		/*
		if(currVId == dest)
		{
			//add a tuple here
			TableTuple temp_tuple = m_pathTable->tempTuple();
			//start vertex, end vertex, length, cost, path
			temp_tuple.setNValue(0, ValueFactory::getIntegerValue(src));
			temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dest));
			temp_tuple.setNValue(2, ValueFactory::getIntegerValue(minCost));
			temp_tuple.setNValue(3, ValueFactory::getDoubleValue(minCost));
			//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
			if(m_pathTable->activeTupleCount() <= 100)
			m_pathTable->insertTempTuple(temp_tuple);
			break;
		}
		*/
		pq.pop();

		//explore the outgoing vertexes
		v = this->getVertex(currVId);
		fanOut = v->fanOut();
		for(int i = 0; i < fanOut; i++)
		{
			e = v->getOutEdge(i);
			candVertexId = e->getEndVertex()->getId();

			if(e->eProp > eSelectivity)
			{
				continue;
			}

			if ( (costMap.find(candVertexId) == costMap.end()) ||
				 (costMap[candVertexId] > minCost + 1) )
			{
				costMap[candVertexId] = minCost + 1;
				pq.push(make_pair(minCost + 1, candVertexId));
			}

		}
	}

	stringstream paramsToPrint;
	paramsToPrint << "SSSP with eSelectivity: from = "
			<< ", eSelectivity = " << edgeSelectivity
			<< ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "SP_eSelectivity", 398, paramsToPrint.str());
}


// void GraphView::SP_EdgeSelectivity(int src, int dest, int edgeSelectivity)
void GraphView::SP_EdgeSelectivity(string src, string dest, int edgeSelectivity) // LX FEAT2
{
	int minCost = INT_MAX;
	priority_queue<PQEntry, vector<PQEntry>, std::greater<PQEntry>> pq;
	map<string, int> costMap;
	costMap[src] = 0;
	pq.push(make_pair(0, src)); //zero cost to reach Vertex from
	Vertex* v = NULL;
	Edge* e = NULL;
	string currVId,  candVertexId = "";
	int fanOut = -1;

	while(!pq.empty())
	{
		//select next vertex to explore
		currVId = pq.top().second;
		minCost = pq.top().first;
		if(currVId == dest)
		{
			//add a tuple here
			TableTuple temp_tuple = m_pathTable->tempTuple();
			//start vertex, end vertex, length, cost, path
			temp_tuple.setNValue(0, ValueFactory::getStringValue(src));
			temp_tuple.setNValue(1, ValueFactory::getStringValue(dest));
			temp_tuple.setNValue(2, ValueFactory::getIntegerValue(minCost));
			temp_tuple.setNValue(3, ValueFactory::getDoubleValue(minCost));
			//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
			if(m_pathTable->activeTupleCount() <= 100)
				m_pathTable->insertTempTuple(temp_tuple);
			break;
		}
		pq.pop();

		//explore the outgoing vertexes
		v = this->getVertex(currVId);
		fanOut = v->fanOut();
		for(int i = 0; i < fanOut; i++)
		{
			e = v->getOutEdge(i);
			candVertexId = e->getEndVertex()->getId();

			if(e->eProp > eSelectivity)
			{
				continue;
			}

			if ( (costMap.find(candVertexId) == costMap.end()) ||
				 (costMap[candVertexId] > minCost + 1) )
			{
				costMap[candVertexId] = minCost + 1;
				pq.push(make_pair(minCost + 1, candVertexId));
			}

		}
	}

	stringstream paramsToPrint;
	paramsToPrint << "SP with eSelectivity: from = " << src << ", to = " << dest
			<< ", eSelectivity = " << edgeSelectivity
			<< ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "SP_eSelectivity", 398, paramsToPrint.str());
}

// void GraphView::BFS_Reachability_ByDepth_eSelectivity(int startVertexId, int depth, int eSelectivity)
void GraphView::BFS_Reachability_ByDepth_eSelectivity(string startVertexId, int depth, int eSelectivity) // LX FEAT2
{
	queue<Vertex*> q;
	Vertex* currentVertex = this->m_vertexes[startVertexId];
	if(NULL != currentVertex)
	{
		currentVertex->Level = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		while(!q.empty() && currentVertex->Level < depth)
		{
			currentVertex = q.front();
			q.pop();
			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);
				if(outEdge->eProp > eSelectivity)
				{
					continue;
				}
				outVertex = outEdge->getEndVertex();
				outVertex->Level = currentVertex->Level + 1;
				if( (depth > 0 && outVertex->Level == depth))
				{
					//Now, we reached the destination vertexes, where we should add tuples into the output table
					TableTuple temp_tuple = m_pathTable->tempTuple();
					//start vertex, end vertex, length, cost, path
					temp_tuple.setNValue(0, ValueFactory::getStringValue(startVertexId));
					temp_tuple.setNValue(1, ValueFactory::getStringValue(outVertex->getId()));
					temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
					temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)(outVertex->Level + 1)));
					//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
					if(m_pathTable->activeTupleCount() <= 100)
					m_pathTable->insertTempTuple(temp_tuple);
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}
	}

	stringstream paramsToPrint;
	paramsToPrint << "BFS_ByDepth: from = " << startVertexId << ", depth = " << depth << ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "BFS_ByDepth", 463, paramsToPrint.str());
}

// void GraphView::BFS_Reachability_ByDepth(int startVertexId, int depth)
void GraphView::BFS_Reachability_ByDepth(string startVertexId, int depth) // LX FEAT2
{
	queue<Vertex*> q;
	Vertex* currentVertex = this->m_vertexes[startVertexId];
	std::map<string, int> vertexToLevel;
	std::unordered_set<string> visited;
	// cout << "1" << endl;
	if(NULL != currentVertex)
	{
		vertexToLevel[currentVertex->getId()] = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		// cout << "2" << endl;
		while(!q.empty() && vertexToLevel[currentVertex->getId()] < depth)
		{
			currentVertex = q.front();
			// cout << "BFS:currentVertex:" << currentVertex << endl;
			q.pop();

			if (visited.find(currentVertex->getId()) == visited.end())
			{
				visited.insert(currentVertex->getId());
			}
			else
			{
				continue;
			}

			fanOut = currentVertex->fanOut();
			// cout << "4:" << fanOut << endl;
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);
				outVertex = outEdge->getEndVertex();
				// cout << "BFS:outVertex:" << outVertex << endl;
				if (visited.find(outVertex->getId()) != visited.end())
				{
					continue;
				}

				//outVertex->Level = currentVertex->Level + 1;
				vertexToLevel[outVertex->getId()] = vertexToLevel[currentVertex->getId()] + 1;
				// cout << "5" << endl;
				if( (depth > 0 && vertexToLevel[outVertex->getId()] == depth))
				{
					//Now, we reached the destination vertexes, where we should add tuples into the output table
					TableTuple temp_tuple = m_pathTable->tempTuple();
					//start vertex, end vertex, length, cost, path
					temp_tuple.setNValue(0, ValueFactory::getStringValue(startVertexId));
					temp_tuple.setNValue(1, ValueFactory::getStringValue(outVertex->getId()));
					temp_tuple.setNValue(2, ValueFactory::getIntegerValue(vertexToLevel[outVertex->getId()]));
					temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)(vertexToLevel[outVertex->getId()])));
					//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
					if(m_pathTable->activeTupleCount() <= 1000)
						m_pathTable->insertTempTuple(temp_tuple);
					// cout << "BFS: start: " << startVertexId << ", end: " << outVertex->getId() << endl;
					// cout << "BFS:660:" << m_pathTable->activeTupleCount() << endl;
					// cout << "BFS:661:" << m_pathTableIterator->m_activeTuples << endl;
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}
	}

	stringstream paramsToPrint;
	paramsToPrint << "BFS_ByDepth: from = " << startVertexId << ", depth = " << depth << ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "BFS_ByDepth", 463, paramsToPrint.str());
}

// void GraphView::BFS_Reachability_ByDestination(int startVertexId, int destVerexId)
void GraphView::BFS_Reachability_ByDestination(string startVertexId, string destVerexId) // LX FEAT2
{
	queue<Vertex*> q;
	Vertex* currentVertex = this->m_vertexes[startVertexId];
	std::map<string, int> vertexToLevel;
	std::unordered_set<string> visited;

	if(NULL != currentVertex)
	{
		vertexToLevel[currentVertex->getId()] = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		bool found = false;
		int level;
		while(!q.empty() && !found)
		{
			currentVertex = q.front();
			q.pop();

			if (visited.find(currentVertex->getId()) == visited.end())
			{
				visited.insert(currentVertex->getId());
			}
			else
			{
				continue;
			}

			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);
				outVertex = outEdge->getEndVertex();

				if (visited.find(outVertex->getId()) != visited.end())
				{
					continue;
				}

				vertexToLevel[outVertex->getId()] = vertexToLevel[currentVertex->getId()] + 1;
				if(outVertex->getId() == destVerexId)
				{
					level = vertexToLevel[outVertex->getId()] + 1;
					//Now, we reached the destination vertexes, where we should add tuples into the output table
					TableTuple temp_tuple = m_pathTable->tempTuple();
					//start vertex, end vertex, length, cost, path
					temp_tuple.setNValue(0, ValueFactory::getStringValue(startVertexId));
					temp_tuple.setNValue(1, ValueFactory::getStringValue(outVertex->getId()));
					temp_tuple.setNValue(2, ValueFactory::getIntegerValue(level));
					temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)level));
					temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
					if(m_pathTable->activeTupleCount() <= 1000)
					m_pathTable->insertTempTuple(temp_tuple);

					//outVertex->Level = outVertex->Level + 1;
					vertexToLevel[outVertex->getId()] = level;
					found = true;
					break;
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}
	}
	executeTraversal = false;

	stringstream paramsToPrint;
	paramsToPrint << "BFS_Reachability_ByDestination: from = " << startVertexId << ", to = " << destVerexId << ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "BFS_Reachability", 513, paramsToPrint.str());
}

// void GraphView::SubGraphLoopFromStartVertex(int startVertexId, int length, int vSelectivity, int eSelectivity)
void GraphView::SubGraphLoopFromStartVertex(string startVertexId, int length, int vSelectivity, int eSelectivity) // LX FEAT2
{
	queue<Vertex*> q;
	Vertex* currentVertex = NULL;
	//for(std::map<int, Vertex*>::iterator it = m_vertexes.begin(); it != m_vertexes.end(); ++it)
	{
		currentVertex = this->getVertex(startVertexId);

		currentVertex->Level = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		while(!q.empty() && currentVertex->Level < length)
		{
			currentVertex = q.front();
			q.pop();
			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);

				if(outEdge->eProp > eSelectivity)
				{
					continue;
				}

				outVertex = outEdge->getEndVertex();

				if(outVertex->vProp > vSelectivity)
				{
					continue;
				}

				outVertex->Level = currentVertex->Level + 1;
				if(outVertex->Level == length)
				{
					//we found a loop of the desired length
					if(outVertex->getId() == startVertexId)
					{
						//Now, we reached the destination vertexes, where we should add tuples into the output table
						TableTuple temp_tuple = m_pathTable->tempTuple();
						//start vertex, end vertex, length, cost, path
						//set the start vertex to the vertex having an edge that closes the loop (for debugging purposes)
						temp_tuple.setNValue(0, ValueFactory::getStringValue(outEdge->getStartVertex()->getId()));
						temp_tuple.setNValue(1, ValueFactory::getStringValue(startVertexId));
						temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
						temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)outVertex->Level));
						//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
						if(m_pathTable->activeTupleCount() <= 1000)
						m_pathTable->insertTempTuple(temp_tuple);
					}
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}

		//empty the queue
		while(!q.empty())
		{
			q.pop();
		}
	}

	stringstream paramsToPrint;
	paramsToPrint << "SubGraphLoop from a specific vertex: length = " << length << ", vSelectivity = " << vSelectivity
			<< ", eSelectivity = " << eSelectivity
			<< ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "SubGraphLoop", 703, paramsToPrint.str());
}


void GraphView::SubGraphLoop(int length, int vSelectivity, int eSelectivity)
{
	queue<Vertex*> q;
	Vertex* currentVertex = NULL;
	string startVertexId = NULL; // LX FEAT2
	for(std::map<string, Vertex*>::iterator it = m_vertexes.begin(); it != m_vertexes.end(); ++it)
	{
		currentVertex = it->second;
		if(currentVertex->vProp > vSelectivity)
		{
			continue;
		}
		startVertexId = currentVertex->getId();
		currentVertex->Level = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		while(!q.empty() && currentVertex->Level < length)
		{
			currentVertex = q.front();
			q.pop();
			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);

				if(outEdge->eProp > eSelectivity)
				{
					continue;
				}

				outVertex = outEdge->getEndVertex();
				outVertex->Level = currentVertex->Level + 1;
				if(outVertex->Level == length)
				{
					//we found a loop of the desired length
					if(outVertex->getId() == startVertexId)
					{
						//Now, we reached the destination vertexes, where we should add tuples into the output table
						TableTuple temp_tuple = m_pathTable->tempTuple();
						//start vertex, end vertex, length, cost, path
						//set the start vertex to the vertex having an edge that closes the loop (for debugging purposes)
						temp_tuple.setNValue(0, ValueFactory::getStringValue(outEdge->getStartVertex()->getId()));
						temp_tuple.setNValue(1, ValueFactory::getStringValue(startVertexId));
						temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
						temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)outVertex->Level));
						//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
						if(m_pathTable->activeTupleCount() <= 100)
						m_pathTable->insertTempTuple(temp_tuple);
					}
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}

		//empty the queue
		while(!q.empty())
		{
			q.pop();
		}
	}

	stringstream paramsToPrint;
	paramsToPrint << "SubGraphLoop: length = " << length << ", vSelectivity = " << vSelectivity
			<< ", eSelectivity = " << eSelectivity
			<< ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "SubGraphLoop", 625, paramsToPrint.str());
}

// void GraphView::SubGraphLoop(int startVertexId, int length)
void GraphView::SubGraphLoop(string startVertexId, int length) // LX FEAT2
{
	queue<Vertex*> q;
	Vertex* currentVertex = NULL;
	if(startVertexId != "")
	{
		currentVertex = this->m_vertexes[startVertexId];
	}
	if(NULL != currentVertex)
	{
		currentVertex->Level = 0;
		q.push(currentVertex);
		int fanOut;
		Edge* outEdge = NULL;
		Vertex* outVertex = NULL;
		while(!q.empty() && currentVertex->Level < length)
		{
			currentVertex = q.front();
			q.pop();
			fanOut = currentVertex->fanOut();
			for(int i = 0; i < fanOut; i++)
			{
				outEdge = currentVertex->getOutEdge(i);
				outVertex = outEdge->getEndVertex();
				outVertex->Level = currentVertex->Level + 1;
				if(outVertex->Level == length)
				{
					//we found a loop of the desired length
					if(outVertex->getId() == startVertexId)
					{
						//Now, we reached the destination vertexes, where we should add tuples into the output table
						TableTuple temp_tuple = m_pathTable->tempTuple();
						//start vertex, end vertex, length, cost, path
						temp_tuple.setNValue(0, ValueFactory::getStringValue(startVertexId));
						temp_tuple.setNValue(1, ValueFactory::getStringValue(outVertex->getId()));
						temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
						temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)outVertex->Level));
						//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
						if(m_pathTable->activeTupleCount() <= 100)
						m_pathTable->insertTempTuple(temp_tuple);
					}
				}
				else
				{
					//add to the queue, as currentDepth is less than depth
					q.push(outVertex);
				}
			}
		}
	}
	else
	{
		for (std::map<string, Vertex*>::iterator it = m_vertexes.begin(); it != m_vertexes.end(); ++it)
		{
			currentVertex = it->second;
			currentVertex->Level = 0;
			q.push(currentVertex);
			int fanOut;
			Edge* outEdge = NULL;
			Vertex* outVertex = NULL;
			while(!q.empty() && currentVertex->Level < length)
			{
				currentVertex = q.front();
				q.pop();
				fanOut = currentVertex->fanOut();
				for(int i = 0; i < fanOut; i++)
				{
					outEdge = currentVertex->getOutEdge(i);
					outVertex = outEdge->getEndVertex();
					outVertex->Level = currentVertex->Level + 1;
					if(outVertex->Level == length)
					{
						//we found a loop of the desired length
						if(outVertex->getId() == startVertexId)
						{
							//Now, we reached the destination vertexes, where we should add tuples into the output table
							TableTuple temp_tuple = m_pathTable->tempTuple();
							//start vertex, end vertex, length, cost, path
							temp_tuple.setNValue(0, ValueFactory::getStringValue(startVertexId));
							temp_tuple.setNValue(1, ValueFactory::getStringValue(outVertex->getId()));
							temp_tuple.setNValue(2, ValueFactory::getIntegerValue(outVertex->Level));
							temp_tuple.setNValue(3, ValueFactory::getDoubleValue((double)outVertex->Level));
							//temp_tuple.setNValue(4, ValueFactory::getStringValue("Test", NULL) );
							if(m_pathTable->activeTupleCount() <= 100)
							m_pathTable->insertTempTuple(temp_tuple);
						}
					}
					else
					{
						//add to the queue, as currentDepth is less than depth
						q.push(outVertex);
					}
				}
			}
			//empty the queue
			while(!q.empty())
			{
				q.pop();
			}
		}
	}
	executeTraversal = false;

	stringstream paramsToPrint;
	paramsToPrint << "SubGraphLoop: from = " << startVertexId << ", length = " << length << ", numOfRowsAdded = " << m_pathTable->activeTupleCount();
	LogManager::GLog("GraphView", "BFS", 302, paramsToPrint.str());
}

// LX FEAT6
void GraphView::processTupleInsertInGraphView(TableTuple& target, std::string tableName) {
	
	VoltDBEngine* engine = ExecutorContext::getEngine();
	engine->updateGraphViewDelegate(this->name());
	// cout << "" << endl;
	return;
}

void GraphView::fillGraphFromRelationalTables()
{
	this->m_vertexes.clear();
	this->m_edges.clear();

	const TupleSchema* schema;

	stringstream paramsToPrint;
	paramsToPrint << " vertex column names = ";
	for(int i = 0; i < m_vertexColumnNames.size(); i++)
	{
		paramsToPrint << m_vertexColumnNames[i] << ", ";
	}
	paramsToPrint << " ### vertexTable ColIDs= ";
	for(int i = 0; i < m_columnIDsInVertexTable.size(); i++)
	{
		paramsToPrint << m_columnIDsInVertexTable[i] << ", ";
	}

	paramsToPrint << " ### edge column names = ";
	for(int i = 0; i < m_edgeColumnNames.size(); i++)
	{
		paramsToPrint << m_edgeColumnNames[i] << ", ";
	}
	paramsToPrint << " ### edgeTable ColIDs= ";
	for(int i = 0; i < m_columnIDsInEdgeTable.size(); i++)
	{
		paramsToPrint << m_columnIDsInEdgeTable[i] << ", ";
	}

	paramsToPrint << " ##### vertexId= " << m_vertexIdColumnIndex << ", edgeId= " << m_edgeIdColumnIndex
			<< "from = " << m_edgeFromColumnIndex << ", to = " << m_edgeToColumnIndex
			<< "vPropColIndex = " << m_vPropColumnIndex << ", ePropColIndex = " << m_ePropColumnIndex;

	// LogManager::GLog("GraphView", "fill", 785, paramsToPrint.str());
	// cout << "graphView:1035:" << paramsToPrint.str() << endl;

	// assert(m_vertexIdColumnIndex >= 0 && m_edgeIdColumnIndex >= 0 && m_edgeFromColumnIndex >= 0 && m_edgeToColumnIndex >=0);

	bool vPropExists = (m_vPropColumnIndex >= 0);
	bool ePropExists = (m_ePropColumnIndex >= 0);

	string id = ""; // LX FEAT2
	//fill the vertex collection
	// LX FEAT2
	for (int i = 0; i < this->m_vertexTables.size(); i++){
		Table* curTable = this->m_vertexTables[i];
		string curLabel = this->m_vertexLabels[i];
		// cout << "GraphView:1046:" << m_vertexIdColumnIndex << endl;
		TableIterator iter = curTable->iterator();
		schema = curTable->schema();
		TableTuple tuple(schema);
		Vertex* vertex = NULL;
		if (curTable->activeTupleCount() != 0)
		{
			while (iter.next(tuple))
			{
				if (tuple.isActive())
				{
					// id = ValuePeeker::peekInteger(tuple.getNValue(m_vertexIdColumnIndex));
					id = curLabel + "." + to_string(ValuePeeker::peekInteger(tuple.getNValue(m_vertexIdColIdxList[curLabel])));
					vertex = new Vertex();
					vertex->setGraphView(this);
					vertex->setId(id);
					vertex->setTupleData(tuple.address());
					if(vPropExists)
					{
						// cout << "GraphView.cpp:1066:vPropExists!" << endl;
						vertex->vProp = ValuePeeker::peekInteger(tuple.getNValue(m_vPropColumnIndex));
					}
					this->addVertex(id, vertex);
					this->m_idToVTableMap[id] = curTable;
					//LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 77, "vertex: " + vertex->toString());
				}
				id = "";
			}
		}
	}
	
	string from = "", to = "";	
	// TODO: LX need to figure out how to set the vertex label for the from vertex and to vertex
	// TODO: add another layer for lookup
	// we know the vertex table for the edge node (by foreign key)
	// we then can lookup the label for this vertex table
	// then we can prepend the label to the vertex of the edge

	// hard-coded for now
	// string fromVertexLabel = "BALL.";
	// string toVertexLabel = "BALL.";
	//fill the edge collection
	for (int i = 0; i < this->m_edgeTables.size(); i++){
		Table* curTable = this->m_edgeTables[i];
		string curLabel = this->m_edgeLabels[i];
		string fromVertexLabel = this->m_startVLabels[i];
		string endVertexLabel = this->m_endVLabels[i];

		TableIterator iter = curTable->iterator();
		schema = curTable->schema();
		TableTuple edgeTuple(schema);
		Edge* edge = NULL;
		Vertex* vFrom = NULL;
		Vertex* vTo = NULL;

		if (curTable->activeTupleCount() != 0)
		{
			while (iter.next(edgeTuple))
			{
				if (edgeTuple.isActive())
				{
					id = curLabel + "." + to_string(ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeIdColIdxList[curLabel])));
					from = fromVertexLabel + "." + to_string(ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeFromColIdxList[curLabel])));
					to = endVertexLabel + "." + to_string(ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeToColIdxList[curLabel])));
					edge = new Edge();
					edge->setGraphView(this);
					edge->setId(id);
					edge->setTupleData(edgeTuple.address());
					edge->setStartVertexId(from);
					edge->setEndVertexId(to);
					if(ePropExists)
					{
						edge->eProp = ValuePeeker::peekInteger(edgeTuple.getNValue(m_ePropColumnIndex));
					}
					//update the endpoint vertexes in and out lists
					vFrom = edge->getStartVertex();
					vTo = edge->getEndVertex();
					vFrom->addOutEdge(edge);
					vTo->addInEdge(edge);
					if(!this->isDirected())
					{
						vTo->addOutEdge(edge);
						vFrom->addInEdge(edge);
					}
					this->addEdge(id, edge);
					this->m_idToETableMap[id] = curTable;
				}
				id = "";
			}
		}

	}	
	
		
		
	LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 159, "graph: " + this->debug());
	//LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 73, "vTable: " + this->m_vertexTable->debug());
	//LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 73, "eTable: " + this->m_edgeTable->debug());

}

string GraphView::debug()
{
	stringstream output;
	output << "Name: " << this->name() << endl;
	output << "Is directed? = " << this->isDirected() << endl;
	int vCount, eCount;
	vCount = this->numOfVertexes();
	eCount = this->numOfEdges();
	output << "#Vertexes = " << vCount << endl;
	output << "#Edges = " << eCount << endl;
	output << "Vertexes" << endl;
	Vertex* currentVertex;
	for (std::map<string,Vertex* >::iterator it= this->m_vertexes.begin(); it != this->m_vertexes.end(); ++it)
	{
		currentVertex = it->second;
		output << "\t" << currentVertex->toString() << endl;
		output << "\t\t" << "out: " << endl;
		for(int j = 0; j < currentVertex->fanOut(); j++)
		{
			output << "\t\t\t" << currentVertex->getOutEdge(j)->toString() << endl;
		}
		output << "\t\t" << "in: " << endl;
		for(int j = 0; j < currentVertex->fanIn(); j++)
		{
			output << "\t\t\t" << currentVertex->getInEdge(j)->toString() << endl;
		}
	}
	return output.str();
}

GraphView::~GraphView(void)
{
}

}
