#include "GraphView.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/TupleSchemaBuilder.h"
#include "logging/LogManager.h"
#include "expressions/expressionutil.h"
#include "PathIterator.h"
#include "Vertex.h"
#include "Edge.h"
#include <string>
#include <map>
#include <unordered_set>
#include "executors/executorutil.h"
#include "execution/ProgressMonitorProxy.h"

#include <queue>
using namespace std;

namespace voltdb
{

GraphView::GraphView(void) //: m_pathIterator(this)
{
	m_pathIterator = new PathIterator(this);
}


string GraphView::name()
{
	return m_name;
}
	
bool GraphView::isDirected()
{
	return m_isDirected;
}

////////////////////////////////////////////

// get vertex
int GraphView::numOfVertexes()
{
	return this->m_vertexes.size();
}

Vertex* GraphView::getVertex(unsigned id) // LX FEAT2
{
	return this->m_vertexes[id];
}

bool GraphView::hasVertex(unsigned id) 
{
	std::map<unsigned, Vertex*>::iterator it = this->m_vertexes.find(id);

	if(it != this->m_vertexes.end())
	{
		return true;
	}
	return false;
}

TableTuple* GraphView::getVertexTuple(unsigned id) // LX FEAT2
{
	Vertex* v = this->getVertex(id);
	Table* t = this->getVertexTableById(id);
	return new TableTuple(v->getTupleData(), t->schema());
}

Table* GraphView::getVertexTableFromLabel(string vlabel)
{
	for (int i = 0; i < (this->m_vertexLabels).size(); i++){
		if (((this->m_vertexLabels[i]).compare(vlabel)) == 0)
			return this->m_vertexTables[i];
	}
	return NULL;
}

Table* GraphView::getVertexTableById(unsigned id)
{
	return this->m_idToVTableMap[id];
}

std::vector<Table*> GraphView::getVertexTables() {
	return this->m_vertexTables;
}

std::vector<std::string> GraphView::getVertexLabels() {
	return this->m_vertexLabels;
}

Table* GraphView::getVertexTableByIndex(int idx) {
	return this->m_vertexTables[idx];
}

std::string GraphView::getVertexLabelByIndex(int idx) {
	return this->m_vertexLabels[idx];
}

TupleSchema* GraphView::getVertexSchema()
{
	return m_vertexSchema;
}

int GraphView::getVertexIdColumnIndex()
{
	return m_vertexIdColumnIndex;
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

string GraphView::getVertexAttributeName(int vertexAttributeId)
{
	return m_vertexColumnNames[vertexAttributeId];
}

std::map<unsigned, Vertex*> GraphView::getVertexMap() 
{
	return this->m_vertexes;
}

int GraphView::getIndexFromVertexLabels(string label) 
{
	std::vector<string>::iterator it;
	it = find(this->m_vertexLabels.begin(), this->m_vertexLabels.end(), label);
	if (it != this->m_vertexLabels.end())
		return distance(this->m_vertexLabels.begin(), it);
	return 0;
}

void GraphView::setVertexSchema(TupleSchema* s)
{
	m_vertexSchema = s;
}

void GraphView::addVertex(unsigned id, Vertex* vertex) // LX FEAT2
{
	this->m_vertexes[id] = vertex;
}
	
////////////////////////////////////////////

// get edge
int GraphView::numOfEdges()
{
	return this->m_edges.size();
}

std::map<unsigned, Edge*> GraphView::getEdgeMap() 
{
	return this->m_edges;
}

Table* GraphView::getEdgeTableById(unsigned id)
{
	return this->m_idToETableMap[id];
}

Edge* GraphView::getEdge(unsigned id) // LX FEAT2
{
	return this->m_edges[id];
}

bool GraphView::hasEdge(unsigned id) 
{
	std::map<unsigned, Edge*>::iterator it = this->m_edges.find(id);

	if(it != this->m_edges.end())
	{
		return true;
	}
	return false;
}

TableTuple* GraphView::getEdgeTuple(unsigned id) // LX FEAT2
{
	Edge* e = this->getEdge(id);
	Table* t = this->getEdgeTableById(id);
	return new TableTuple(e->getTupleData(), t->schema());
}

std::vector<Table*> GraphView::getEdgeTables() {
	return this->m_edgeTables;
}

std::vector<std::string> GraphView::getEdgeLabels() {
	return this->m_edgeLabels;
}

std::vector<std::string> GraphView::getStartVertexLabels() {
	return this->m_startVLabels;
}

std::vector<std::string> GraphView::getEndVertexLabels() {
	return this->m_endVLabels;
}

Table* GraphView::getEdgeTableByIndex(int idx) {
	return this->m_edgeTables[idx];
}

std::string GraphView::getEdgeLabelByIndex(int idx) {
	return this->m_edgeLabels[idx];
}

Table* GraphView::getEdgeTableFromLabel(string elabel)
{
	for (int i = 0; i < (this->m_edgeLabels).size(); i++){
		if (((this->m_edgeLabels[i]).compare(elabel)) == 0)
			return this->m_edgeTables[i];
	}
	return NULL;
}

TupleSchema* GraphView::getEdgeSchema()
{
	return m_edgeSchema;
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

int GraphView::getColumnIdInEdgeTable(int edgeAttributeId)
{
	return m_columnIDsInEdgeTable[edgeAttributeId];
}

string GraphView::getEdgeAttributeName(int edgeAttributeId)
{
	return m_edgeColumnNames[edgeAttributeId];
}

int GraphView::getIndexFromEdgeLabels(string label) 
{
	std::vector<string>::iterator it;
	it = find(this->m_edgeLabels.begin(), this->m_edgeLabels.end(), label);
	if (it != this->m_edgeLabels.end())
		return distance(this->m_edgeLabels.begin(), it);
	return 0;
}

void GraphView::addEdge(unsigned id, Edge* edge) // LX FEAT2
{
	this->m_edges[id] = edge;
}

void GraphView::setEdgeSchema(TupleSchema* s)
{
	m_edgeSchema = s;
}
////////////////////////////////////////////

// path
Table* GraphView::getPathTable()
{
	return this->m_pathTable;
}

// LX FEAT4
Table* GraphView::getGraphTable()
{
	return this->m_graphTable;
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

void GraphView::setPathSchema(TupleSchema* s)
{
	m_pathSchema = s;
}

// LX FEAT4
void GraphView::setGraphSchema(TupleSchema* s)
{
	m_graphSchema = s;
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
float GraphView::shortestPath(int source, int destination, int costColumnId)
{
	//TODO: write real shortest path code that consults the edges table using costColumnId
	return (float)source * destination;
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

////////////////////////////////////////////

// traverse graph
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

void GraphView::SP_TopK(unsigned src, unsigned dest, int k)
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
	unsigned currVId;
	int fanOut = -1;
	unsigned candVertexId ;
	
	//upper-bound to avoid loops (considering an average fan-out of 10
	int maxPQOperations = this->numOfVertexes() * 10;
	int iterationNum = 0;
	TableTuple* edgeTuple;
	int length;

	while(!pq.empty())
	{
		//select next vertex to explore
		currVId = ((pair<unsigned, int >)( pq.top().second)).first;
		length = ((pair<unsigned, int >)( pq.top().second)).second;
		minCost = pq.top().first;
		if(currVId == dest)
		{
			foundPathsSoFar++;

			//add a tuple here
			TableTuple temp_tuple = m_pathTable->tempTuple();
			//start vertex, end vertex, length, cost, path
			// temp_tuple.setNValue(0, ValueFactory::getIntegerValue(src));
			// temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dest));
			temp_tuple.setNValue(0, ValueFactory::getIntegerValue(src)); // LX FEAT2
			temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dest)); // LX FEAT2
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
			unsigned es = v->getOutEdgeId(i);
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

void GraphView::SP_ToAllVertexes_EdgeSelectivity(unsigned src, int edgeSelectivity)
{
	int minCost = INT_MAX;
	priority_queue<PQEntry, vector<PQEntry>, std::greater<PQEntry>> pq;
	map<unsigned, int> costMap;
	costMap[src] = 0;
	pq.push(make_pair(0, src)); //zero cost to reach Vertex from
	Vertex* v = NULL;
	Edge* e = NULL;
	unsigned currVId;
	int fanOut = -1;
	unsigned candVertexId ;

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


void GraphView::SP_EdgeSelectivity(unsigned src, unsigned dest, int edgeSelectivity)
{
	int minCost = INT_MAX;
	priority_queue<PQEntry, vector<PQEntry>, std::greater<PQEntry>> pq;
	map<unsigned, int> costMap;
	costMap[src] = 0;
	pq.push(make_pair(0, src)); //zero cost to reach Vertex from
	Vertex* v = NULL;
	Edge* e = NULL;
	unsigned currVId,  candVertexId ;
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
			temp_tuple.setNValue(0, ValueFactory::getIntegerValue(src));
			temp_tuple.setNValue(1, ValueFactory::getIntegerValue(dest));
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

void GraphView::BFS_Reachability_ByDepth_eSelectivity(unsigned startVertexId, int depth, int eSelectivity)
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
					temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
					temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
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

void GraphView::BFS_Reachability_ByDepth(unsigned startVertexId, int depth)
{
	queue<Vertex*> q;
	Vertex* currentVertex = this->m_vertexes[startVertexId];
	std::map<unsigned, int> vertexToLevel;
	std::unordered_set<unsigned> visited;
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
					temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
					temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
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

void GraphView::BFS_Reachability_ByDestination(unsigned startVertexId, unsigned destVerexId)
{
	queue<Vertex*> q;
	Vertex* currentVertex = this->m_vertexes[startVertexId];
	std::map<unsigned, int> vertexToLevel;
	std::unordered_set<unsigned> visited;

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
					temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
					temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
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

void GraphView::SubGraphLoopFromStartVertex(unsigned startVertexId, int length, int vSelectivity, int eSelectivity)
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
						temp_tuple.setNValue(0, ValueFactory::getIntegerValue(outEdge->getStartVertex()->getId()));
						temp_tuple.setNValue(1, ValueFactory::getIntegerValue(startVertexId));
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
	unsigned startVertexId ; // LX FEAT2
	for(std::map<unsigned, Vertex*>::iterator it = m_vertexes.begin(); it != m_vertexes.end(); ++it)
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
						temp_tuple.setNValue(0, ValueFactory::getIntegerValue(outEdge->getStartVertex()->getId()));
						temp_tuple.setNValue(1, ValueFactory::getIntegerValue(startVertexId));
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

void GraphView::SubGraphLoop(unsigned startVertexId, int length)
{
	queue<Vertex*> q;
	Vertex* currentVertex = NULL;
	if(startVertexId != 0)
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
						temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
						temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
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
		for (std::map<unsigned, Vertex*>::iterator it = m_vertexes.begin(); it != m_vertexes.end(); ++it)
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
							temp_tuple.setNValue(0, ValueFactory::getIntegerValue(startVertexId));
							temp_tuple.setNValue(1, ValueFactory::getIntegerValue(outVertex->getId()));
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

	// string id = ""; // LX FEAT2 string takes too much space, use int instead
	// LX this is a decimal number. The least significant position holds the index of label (at most 9 labels)
	unsigned id; // the maximum number is 4294967295

	//fill the vertex collection
	// LX FEAT2
	for (int i = 0; i < this->m_vertexTables.size(); i++){
		Table* curTable = this->m_vertexTables[i];
		string curLabel = this->m_vertexLabels[i];

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
					// id = curLabel + "." + to_string(ValuePeeker::peekInteger(tuple.getNValue(m_vertexIdColIdxList[curLabel])));
					id = ValuePeeker::peekInteger(tuple.getNValue(m_vertexIdColIdxList[curLabel])) * 10 + i;
					vertex = new Vertex();
					vertex->setGraphView(this);
					vertex->setId(id);
					vertex->setTupleData(tuple.address());
					// cout << "GraphView:1258:vertex id is " << id << endl;
					if(vPropExists)
					{
						// cout << "GraphView.cpp:1066:vPropExists!" << endl;
						vertex->vProp = ValuePeeker::peekInteger(tuple.getNValue(m_vPropColumnIndex));
					}
					this->addVertex(id, vertex);
					this->m_idToVTableMap[id] = curTable;
					//LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 77, "vertex: " + vertex->toString());
				}
				// id = "";
			}
		}
	}
	
	unsigned from, to;	
	// we know the vertex table for the edge node (by foreign key)
	// we then can lookup the label for this vertex table
	// then we can prepend the label to the vertex of the edge

	//fill the edge collection
	for (int i = 0; i < this->m_edgeTables.size(); i++){
		Table* curTable = this->m_edgeTables[i];
		string curLabel = this->m_edgeLabels[i];
		string fromVertexLabel = this->m_startVLabels[i];
		string endVertexLabel = this->m_endVLabels[i];
		int fromIdx = getIndexFromVertexLabels(fromVertexLabel);
		int toIdx = getIndexFromVertexLabels(endVertexLabel);

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
					// id = curLabel + "." + to_string(ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeIdColIdxList[curLabel])));
					// from = fromVertexLabel + "." + to_string(ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeFromColIdxList[curLabel])));
					// to = endVertexLabel + "." + to_string(ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeToColIdxList[curLabel])));
					id = ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeIdColIdxList[curLabel])) * 10 + i;
					from = ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeFromColIdxList[curLabel])) * 10 + fromIdx;
					to = ValuePeeker::peekInteger(edgeTuple.getNValue(m_edgeToColIdxList[curLabel])) * 10 + toIdx;
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
					// cout << "GraphView:1335:edge id is " << id << endl; 
					this->m_idToETableMap[id] = curTable;
				}
				// id = "";
			}
		}

	}			
	// LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 159, "graph: " + this->debug());
	//LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 73, "vTable: " + this->m_vertexTable->debug());
	//LogManager::GLog("GraphView", "fillGraphFromRelationalTables", 73, "eTable: " + this->m_edgeTable->debug());

}

Vertex* GraphView::createAndAddVertex(unsigned vid, char* tupleData) 
{
	Vertex *newVertex = NULL;
	newVertex = new Vertex();
	newVertex->setGraphView(this);
	newVertex->setId(vid);
	newVertex->setTupleData(tupleData);
	this->addVertex(vid, newVertex);
	return newVertex;
}

Edge* GraphView::createEdge(unsigned eid, char* tupleData, unsigned fromId, unsigned toId)
{
	Edge* newEdge = NULL;
	newEdge = new Edge();
	newEdge->setGraphView(this);
	newEdge->setId(eid);
	newEdge->setTupleData(tupleData);
	newEdge->setStartVertexId(fromId);
	newEdge->setEndVertexId(toId);
	return newEdge;
}

void GraphView::selectOnlyBoundVerticesFromVertex(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView) 
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);

	// predicate is about vertices and we want to start from vertices
	// get all the vertices from the old graph
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	// traverse all the vertices and get their tuple data
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{

		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tuple = new TableTuple(curVertex->getTupleData(), input_table->schema());

		//check if the tuple satisfy the predicate
		if (postfilter.eval(tuple, NULL)) {
			createAndAddVertex(curVertexId, curVertex->getTupleData());
		}
	}
}

void GraphView::filterGraphVertexFromVertexTable(Table* input_table, int labelIdx, AbstractExpression* predicate, GraphView* oldGraphView) 
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);

    TableTuple tuple(input_table->schema());
    TableIterator iterator = input_table->iteratorDeletingAsWeGo();
	
	int vertexId;
	while (postfilter.isUnderLimit() && iterator.next(tuple)) {
		if (postfilter.eval(&tuple, NULL)) {
            //get the vertex id
            vertexId = ValuePeeker::peekInteger(tuple.getNValue(0)) * 10 + labelIdx;
            createAndAddVertex(vertexId, tuple.address());
        }
	}

	// has to scan oldGraphView edges again to add edges if both end nodes are selected
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	Edge *curEdge;
	unsigned curEdgeId;
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		curEdgeId = it->first;
		curEdge = it->second;
		unsigned from = curEdge->getStartVertexId();
		unsigned to = curEdge->getEndVertexId();
		if ((this->hasVertex(from)) && (this->hasVertex(to))){
			Edge* newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);

			Vertex* vFrom = newEdge->getStartVertex();
			Vertex* vTo = newEdge->getEndVertex();
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);
			this->addEdge(curEdgeId, newEdge);
		}
	}
}

void GraphView::selectOnlyBoundVerticesFromEdge(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);

    // predicate is about vertices and we want to start from edges
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	Edge *curEdge;
	unsigned from, to;
	Vertex* vFrom;
	Vertex* vTo;
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		// curEdgeId = it->first;
		curEdge = it->second;
		vFrom = curEdge->getStartVertex();
		vTo = curEdge->getEndVertex();
		TableTuple* fromtuple = new TableTuple(vFrom->getTupleData(), input_table->schema());
		TableTuple* totuple = new TableTuple(vTo->getTupleData(), input_table->schema());
		if (postfilter.eval(fromtuple, NULL)) {
			from = curEdge->getStartVertexId();
			createAndAddVertex(from, vFrom->getTupleData());
		}
		if (postfilter.eval(totuple, NULL)) {
			to = curEdge->getEndVertexId();
			createAndAddVertex(to, vTo->getTupleData());
		}
	}

	// Some vertices may not be reachable from any vertices
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tuple = new TableTuple(curVertex->getTupleData(), input_table->schema());

		//check if the tuple satisfy the predicate
		if (postfilter.eval(tuple, NULL)) {
			createAndAddVertex(curVertexId, curVertex->getTupleData());
		}
	}
}

void GraphView::selectOnlyFreeVerticesFromVertex(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView) 
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);

	Vertex* curVertex;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertex = it->second;
		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();

		unsigned from, to;
		// iterate all related edges (all edges are visited twice)
		for (unsigned edgeId: edgeIds) {
			visited_fanout++;
			Edge *edge = oldGraphView->getEdge(edgeId);
			TableTuple* tuple = new TableTuple(edge->getTupleData(), input_table->schema());
			if (postfilter.eval(tuple, NULL)) {
				from = edge->getStartVertexId();
				to = edge->getEndVertexId();
				createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
			}
		}				
	}
	cout << "selectOnlyFreeVerticesFromVertex: visited fanout = " << visited_fanout << endl;
}

void GraphView::selectOnlyFreeVerticesFromEdge(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);

    // predicate is about vertices and we want to start from edges
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	Edge *curEdge;
	unsigned from, to;
	Vertex* vFrom;
	Vertex* vTo;
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		// curEdgeId = it->first;
		curEdge = it->second;
		vFrom = curEdge->getStartVertex();
		vTo = curEdge->getEndVertex();
		TableTuple* tuple = new TableTuple(curEdge->getTupleData(), input_table->schema());
		if (postfilter.eval(tuple, NULL)) {
			from = curEdge->getStartVertexId();
			to = curEdge->getEndVertexId();
			createAndAddVertex(from, vFrom->getTupleData());
			createAndAddVertex(to, vTo->getTupleData());
		}
	}
}

void GraphView::selectFreeBoundVerticesFromVertex(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{

		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tupleV = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

		//check if the tuple satisfy the predicate
		if (postfilterV.eval(tupleV, NULL)) {
			// bound vertices
			createAndAddVertex(curVertexId, curVertex->getTupleData());
		}
		else {
			// free vertices
			std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
			unsigned from, to;
			
			for (unsigned edgeId: edgeIds) {
				visited_fanout++;
				Edge *edge = oldGraphView->getEdge(edgeId);
				TableTuple* tupleE = new TableTuple(edge->getTupleData(), input_etable->schema());
				if (postfilterE.eval(tupleE, NULL)) {
					from = edge->getStartVertexId();
					to = edge->getEndVertexId();
					createAndAddVertex(from, edge->getEndVertex()->getTupleData());
					createAndAddVertex(to, edge->getStartVertex()->getTupleData());
				}
			}
		}						
	}
	cout << "selectFreeBoundVerticesFromVertex: visitedfanout = " << visited_fanout << endl;
}

void GraphView::selectFreeBoundVerticesFromEdge(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Edge *curEdge;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		curEdge = it->second;
		TableTuple* tupleE = new TableTuple(curEdge->getTupleData(), input_etable->schema());

		Vertex* vFrom = curEdge->getStartVertex();
		TableTuple* tupleV1 = new TableTuple(vFrom->getTupleData(), input_vtable->schema());
		Vertex* vTo = curEdge->getEndVertex();
		TableTuple* tupleV2 = new TableTuple(vTo->getTupleData(), input_vtable->schema());

		//check if the tuple satisfy the predicate
		if (postfilterE.eval(tupleE, NULL)) {
			createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
			createAndAddVertex(vTo->getId(), vTo->getTupleData());
		}
		else {
			if (postfilterV.eval(tupleV1, NULL))
				createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
			if (postfilterV.eval(tupleV2, NULL))
				createAndAddVertex(vTo->getId(), vTo->getTupleData());
		}
	}
	// Some vertices may not be reachable from any vertices
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tuple = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

		//check if the tuple satisfy the predicate
		if (postfilterV.eval(tuple, NULL)) {
			createAndAddVertex(curVertexId, curVertex->getTupleData());
		}
	}
}

void GraphView::selectFreeIntersectBoundVerticesFromVertex(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{

		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tupleV = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

		//check if the tuple satisfy the predicate
		if (!postfilterV.eval(tupleV, NULL)) 
			continue;
		
		// free vertices
		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();

		for (unsigned edgeId: edgeIds) {
			visited_fanout++;
			Edge *edge = oldGraphView->getEdge(edgeId);
			TableTuple* tupleE = new TableTuple(edge->getTupleData(), input_etable->schema());
			if (postfilterE.eval(tupleE, NULL)) {
				createAndAddVertex(curVertexId, curVertex->getTupleData());
			}
		}			
	}
	cout << "selectFreeIntersectBoundVerticesFromVertex: visited_fanout = " << visited_fanout << endl;
}

void GraphView::selectFreeIntersectBoundVerticesFromEdge(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Edge *curEdge;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		curEdge = it->second;
		TableTuple* tupleE = new TableTuple(curEdge->getTupleData(), input_etable->schema());

		Vertex* vFrom = curEdge->getStartVertex();
		TableTuple* tupleV1 = new TableTuple(vFrom->getTupleData(), input_vtable->schema());
		Vertex* vTo = curEdge->getEndVertex();
		TableTuple* tupleV2 = new TableTuple(vTo->getTupleData(), input_vtable->schema());

		//check if the tuple satisfy the predicate
		if (postfilterE.eval(tupleE, NULL)) {
			if (postfilterV.eval(tupleV1, NULL)){
				createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
			} 
			if (postfilterV.eval(tupleV2, NULL)) {
				createAndAddVertex(vTo->getId(), vTo->getTupleData());
			}
		}
	}
}

void GraphView::selectBoundEdgesFromVertex(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	// predicate is about edges and we want to start from vertices
	// get all the vertices from the old graph
	Vertex* curVertex;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	// traverse all the vertices
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertex = it->second;
		std::vector<unsigned> edgeIds;

		// get the edges from this vertex's adjacency list
		if (oldGraphView->isDirected()) {
			// has to scan both inedges and out edges
			edgeIds = curVertex->getAllOutEdges();
			vector<unsigned> another = curVertex->getAllInEdges();
			edgeIds.insert(edgeIds.end(), another.begin(), another.end() );
		}
		else {
			// only needs to scan inedges list or outedges list
			edgeIds = curVertex->getAllOutEdges();
		}

		unsigned from, to;
		// iterate all related edges (all edges are visited twice)
		for (unsigned edgeId: edgeIds) {
			visited_fanout++;
			Edge *edge = oldGraphView->getEdge(edgeId);
			TableTuple* tuple = new TableTuple(edge->getTupleData(), input_table->schema());
			if (postfilter.eval(tuple, NULL)) {
				from = edge->getStartVertexId();
				to = edge->getEndVertexId();
				Edge *newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
	    		
				// add these two vertices to new graph
				Vertex* vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				Vertex* vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

				//update the endpoint vertexes in and out lists
				vFrom = newEdge->getStartVertex();
				vTo = newEdge->getEndVertex();
				vFrom->addOutEdge(newEdge);
				vTo->addInEdge(newEdge);

				if(!this->isDirected())
				{
					vTo->addOutEdge(newEdge);
					vFrom->addInEdge(newEdge);
				}
				this->addEdge(edgeId, newEdge);
			}
		}				
	}
	cout << "selectBoundEdgesFromVertex: visited_fanout = " << visited_fanout << endl;
}

void GraphView::selectBoundEdgesFromEdge(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	// predicate is about edges and we want to start from edges
	// get all the edges from the old graph
	Edge* curEdge;
	unsigned curEdgeId, from, to;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{

		curEdgeId = it->first;
		curEdge = it->second;
		TableTuple* tuple = new TableTuple(curEdge->getTupleData(), input_table->schema());

		//check if the tuple satisfy the predicate
		if (postfilter.eval(tuple, NULL)) {
			from = curEdge->getStartVertexId();
			to = curEdge->getEndVertexId();
			Edge *newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);
    		
			// add these two vertices to new graph
			Vertex* vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			Vertex* vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

			//update the endpoint vertexes in and out lists
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);

			if(!this->isDirected())
			{
				vTo->addOutEdge(newEdge);
				vFrom->addInEdge(newEdge);
			}
			this->addEdge(curEdgeId, newEdge);
		}
	}
}

void GraphView::selectFreeEdgesFromVertex(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	// predicate is about edges and we want to start from vertices
	// get all the vertices from the old graph
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	// traverse all the vertices
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tupleV1 = new TableTuple(curVertex->getTupleData(), input_table->schema());
		if (!postfilter.eval(tupleV1, NULL)) 
			continue;

		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();

		unsigned from, to;
		// iterate all related edges (all edges are visited twice)
		for (unsigned edgeId: edgeIds) {
			visited_fanout++;
			Edge *edge = oldGraphView->getEdge(edgeId);
			from = edge->getStartVertexId();
			to = edge->getEndVertexId();
			TableTuple* tupleV2;
			if (curVertexId == from)
				tupleV2 = new TableTuple(edge->getEndVertex()->getTupleData(), input_table->schema());
			else
				tupleV2 = new TableTuple(edge->getStartVertex()->getTupleData(), input_table->schema());
			if (postfilter.eval(tupleV2, NULL)) {
				from = edge->getStartVertexId();
				to = edge->getEndVertexId();
				Edge *newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
	    		
				// add these two vertices to new graph
				Vertex* vNewFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				Vertex* vNewTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

				//update the endpoint vertexes in and out lists
				vNewFrom->addOutEdge(newEdge);
				vNewTo->addInEdge(newEdge);

				if(!this->isDirected())
				{
					vNewTo->addOutEdge(newEdge);
					vNewFrom->addInEdge(newEdge);
				}
				this->addEdge(edgeId, newEdge);
			}
		}				
	}
	cout << "selectFreeEdgesFromVertex: visited_fanout = " << visited_fanout << endl;
}

void GraphView::selectFreeEdgesFromEdge(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	// predicate is about edges and we want to start from edges
	// get all the edges from the old graph
	Edge* curEdge;
	unsigned curEdgeId, from, to;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{

		curEdgeId = it->first;
		curEdge = it->second;
		Vertex* vFrom = curEdge->getStartVertex();
		Vertex* vTo = curEdge->getEndVertex();
		TableTuple* tuple1 = new TableTuple(vFrom->getTupleData(), input_table->schema());
		TableTuple* tuple2 = new TableTuple(vTo->getTupleData(), input_table->schema());

		//check if the tuple satisfy the predicate
		if (postfilter.eval(tuple1, NULL) && postfilter.eval(tuple2, NULL)) {
			from = curEdge->getStartVertexId();
			to = curEdge->getEndVertexId();
			Edge *newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);
    		
			// add these two vertices to new graph
			Vertex* vNewFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			Vertex* vNewTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

			//update the endpoint vertexes in and out lists
			vNewFrom->addOutEdge(newEdge);
			vNewTo->addInEdge(newEdge);

			if(!this->isDirected())
			{
				vNewTo->addOutEdge(newEdge);
				vNewFrom->addInEdge(newEdge);
			}
			this->addEdge(curEdgeId, newEdge);
		}
	}
}


void GraphView::selectFreeBoundEdgesFromVertex(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{

		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tupleV = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

		//check if the tuple satisfy the predicate
		if (postfilterV.eval(tupleV, NULL)) {
			// bound vertices
			createAndAddVertex(curVertexId, curVertex->getTupleData());
		}
		else {
			// free vertices
			std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
			unsigned from, to;

			for (unsigned edgeId: edgeIds) {
				visited_fanout++;
				Edge *edge = oldGraphView->getEdge(edgeId);
				TableTuple* tupleE = new TableTuple(edge->getTupleData(), input_etable->schema());
				if (postfilterE.eval(tupleE, NULL)) {
					from = edge->getStartVertexId();
					to = edge->getEndVertexId();
					createAndAddVertex(from, edge->getEndVertex()->getTupleData());
					createAndAddVertex(to, edge->getStartVertex()->getTupleData());
				}
			}
		}						
	}

	// has to scan oldGraphView edges again to add edges if both end nodes are selected
	Edge* curEdge;
	unsigned curEdgeId;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		curEdgeId = it->first;
		curEdge = it->second;
		unsigned from = curEdge->getStartVertexId();
		unsigned to = curEdge->getEndVertexId();
		if ((this->hasVertex(from)) && (this->hasVertex(to))){
			Edge* newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);

			Vertex* vFrom = newEdge->getStartVertex();
			Vertex* vTo = newEdge->getEndVertex();
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);
			this->addEdge(curEdgeId, newEdge);
		}
	}
	cout << "selectFreeBoundEdgesFromVertex: visited_fanout = " << visited_fanout << endl;
}

void GraphView::selectFreeBoundEdgesFromEdge(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Edge *curEdge;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		curEdge = it->second;
		TableTuple* tupleE = new TableTuple(curEdge->getTupleData(), input_etable->schema());

		Vertex* vFrom = curEdge->getStartVertex();
		TableTuple* tupleV1 = new TableTuple(vFrom->getTupleData(), input_vtable->schema());
		Vertex* vTo = curEdge->getEndVertex();
		TableTuple* tupleV2 = new TableTuple(vTo->getTupleData(), input_vtable->schema());

		//check if the tuple satisfy the predicate
		if (postfilterE.eval(tupleE, NULL)) {
			createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
			createAndAddVertex(vTo->getId(), vTo->getTupleData());
		}
		else {
			if (postfilterV.eval(tupleV1, NULL))
				createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
			if (postfilterV.eval(tupleV2, NULL))
				createAndAddVertex(vTo->getId(), vTo->getTupleData());
		}
	}
	// Some vertices may not be reachable from any vertices
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tuple = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

		//check if the tuple satisfy the predicate
		if (postfilterV.eval(tuple, NULL)) {
			createAndAddVertex(curVertexId, curVertex->getTupleData());
		}
	}

	// has to scan oldGraphView edges again to add edges if both end nodes are selected
	unsigned curEdgeId;
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		curEdgeId = it->first;
		curEdge = it->second;
		unsigned from = curEdge->getStartVertexId();
		unsigned to = curEdge->getEndVertexId();
		if ((this->hasVertex(from)) && (this->hasVertex(to))){
			Edge* newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);

			Vertex* vFrom = newEdge->getStartVertex();
			Vertex* vTo = newEdge->getEndVertex();
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);
			this->addEdge(curEdgeId, newEdge);
		}
	}
}

void GraphView::selectFreeIntersectBoundEdgesFromVertex(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{

		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tupleV = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

		//check if the tuple satisfy the predicate
		if (!postfilterV.eval(tupleV, NULL)) 
			continue;
		
		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
		unsigned from , to;
		for (unsigned edgeId: edgeIds) {
			visited_fanout++;	
			Edge *edge = oldGraphView->getEdge(edgeId);
			TableTuple* tupleE = new TableTuple(edge->getTupleData(), input_etable->schema());
			from = edge->getStartVertexId();
			to = edge->getEndVertexId();
			TableTuple* tupleV2;
			if (curVertexId == from)
				tupleV2 = new TableTuple(edge->getEndVertex()->getTupleData(), input_vtable->schema());
			else
				tupleV2 = new TableTuple(edge->getStartVertex()->getTupleData(), input_vtable->schema());
			if (postfilterE.eval(tupleE, NULL) && postfilterV.eval(tupleV2, NULL)) {
				Edge *newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
	    		
				Vertex* vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				Vertex* vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

				vFrom->addOutEdge(newEdge);
				vTo->addInEdge(newEdge);

				if(!this->isDirected())
				{
					vTo->addOutEdge(newEdge);
					vFrom->addInEdge(newEdge);
				}
				this->addEdge(edgeId, newEdge);
			}
		}			
	}
	cout << "selectFreeIntersectBoundEdgesFromVertex: visited_fanout = " << visited_fanout << endl;
}

void GraphView::selectFreeIntersectBoundEdgesFromEdge(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Edge *curEdge;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	unsigned from, to;
	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		curEdge = it->second;
		TableTuple* tupleE = new TableTuple(curEdge->getTupleData(), input_etable->schema());

		Vertex* vFrom = curEdge->getStartVertex();
		TableTuple* tupleV1 = new TableTuple(vFrom->getTupleData(), input_vtable->schema());
		Vertex* vTo = curEdge->getEndVertex();
		TableTuple* tupleV2 = new TableTuple(vTo->getTupleData(), input_vtable->schema());

		from = vFrom->getId();
		to = vTo->getId();
		//check if the tuple satisfy the predicate
		if (postfilterE.eval(tupleE, NULL) && postfilterV.eval(tupleV1, NULL) && postfilterV.eval(tupleV2, NULL)) {
			Edge *newEdge = createEdge(curEdge->getId(), curEdge->getTupleData(), from, to);
	    		
			Vertex* vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			Vertex* vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);

			if(!this->isDirected())
			{
				vTo->addOutEdge(newEdge);
				vFrom->addInEdge(newEdge);
			}
			this->addEdge(curEdge->getId(), newEdge);
		}
	}
}

void GraphView::filterGraphEdgeFromVertex(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	// predicate is about edges and we want to start from vertices
	// get all the vertices from the old graph
	Vertex* curVertex;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	// traverse all the vertices
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertex = it->second;

		std::vector<unsigned> edgeIds;

		// get the edges from this vertex's adjacency list
		if (oldGraphView->isDirected()) {
			// has to scan both inedges and out edges
			edgeIds = curVertex->getAllOutEdges();
			vector<unsigned> another = curVertex->getAllInEdges();
			edgeIds.insert(edgeIds.end(), another.begin(), another.end() );
		}
		else {
			// only needs to scan inedges list or outedges list
			edgeIds = curVertex->getAllOutEdges();
		}

		unsigned from, to;
		// iterate all related edges (all edges are visited twice)
		for (unsigned edgeId: edgeIds) {
			Edge *edge = oldGraphView->getEdge(edgeId);
			TableTuple* tuple = new TableTuple(edge->getTupleData(), input_table->schema());
			if (postfilter.eval(tuple, NULL)) {
				from = edge->getStartVertexId();
				to = edge->getEndVertexId();
				Edge *newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
	    		
				// add these two vertices to new graph
				Vertex* vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				Vertex* vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

				//update the endpoint vertexes in and out lists
				vFrom = newEdge->getStartVertex();
				vTo = newEdge->getEndVertex();
				vFrom->addOutEdge(newEdge);
				vTo->addInEdge(newEdge);

				if(!this->isDirected())
				{
					vTo->addOutEdge(newEdge);
					vFrom->addInEdge(newEdge);
				}
				this->addEdge(edgeId, newEdge);
			}
		}				
	}
}

void GraphView::filterGraphEdgeFromEdge(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	// predicate is about edges and we want to start from edges
	// get all the edges from the old graph
	Edge* curEdge;
	unsigned curEdgeId, from, to;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{

		curEdgeId = it->first;
		curEdge = it->second;
		TableTuple* tuple = new TableTuple(curEdge->getTupleData(), input_table->schema());

		//check if the tuple satisfy the predicate
		if (postfilter.eval(tuple, NULL)) {
			from = curEdge->getStartVertexId();
			to = curEdge->getEndVertexId();
			Edge *newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);
    		
			// add these two vertices to new graph
			Vertex* vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			Vertex* vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

			//update the endpoint vertexes in and out lists
			// vFrom = newEdge->getStartVertex();
			// vTo = newEdge->getEndVertex();
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);

			if(!this->isDirected())
			{
				vTo->addOutEdge(newEdge);
				vFrom->addInEdge(newEdge);
			}
			this->addEdge(curEdgeId, newEdge);
		}
	}
}

void GraphView::filterGraphEdgeFromEdgeTable(Table* input_table, int labelIdx, AbstractExpression* predicate, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	
	TableTuple tuple(input_table->schema());
    TableIterator iterator = input_table->iteratorDeletingAsWeGo();
	
	int edgeId;
	unsigned from, to;
	Edge* oldEdge;
	while (postfilter.isUnderLimit() && iterator.next(tuple)) {
		if (postfilter.eval(&tuple, NULL)) {
            edgeId = ValuePeeker::peekInteger(tuple.getNValue(0)) * 10 + labelIdx;
            oldEdge = oldGraphView->getEdge(edgeId);
            from = oldEdge->getStartVertexId();
            to = oldEdge->getEndVertexId();
            Edge *newEdge = createEdge(edgeId, tuple.address(), from, to);

            // add these two vertices to new graph
			Vertex* vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			Vertex* vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

			//update the endpoint vertexes in and out lists
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);

			if(!this->isDirected())
			{
				vTo->addOutEdge(newEdge);
				vFrom->addInEdge(newEdge);
			}
			this->addEdge(edgeId, newEdge);
        }
	}
}

void GraphView::fillGraphByIntersection(AbstractExpression* vpred, AbstractExpression* epred, Table* input_vtable, Table* input_etable, bool useV, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

    if (useV) {
    	// vertices must satisfy the vertex predicate
    	// the attached edges must satisfy the edge predicate
    	// first scan adds the intersected vertices
    	// second scan adds the edges if both end nodes are selected
    	cout << "GraphView: Graph Intersection V" << endl;
    	Vertex* curVertex;
		unsigned curVertexId;
		std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
		int selectivity = 0;

		for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
		{

			curVertexId = it->first;
			curVertex = it->second;
			TableTuple* tupleV = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

			//check if the tuple satisfy the predicate
			if (!postfilterV.eval(tupleV, NULL)) 
				continue;
			selectivity++;
			// then check the attached edges
			// TODO: each edge is checked twice, optimize later to remember who have been checked
			std::vector<unsigned> edgeIds;
			if (oldGraphView->isDirected()) {
				// has to scan both inedges and out edges
				edgeIds = curVertex->getAllOutEdges();
				vector<unsigned> another = curVertex->getAllInEdges();
				edgeIds.insert(edgeIds.end(), another.begin(), another.end() );
			}
			else {
				edgeIds = curVertex->getAllOutEdges();
			}
			for (unsigned edgeId: edgeIds) {
				Edge *edge = oldGraphView->getEdge(edgeId);
				TableTuple* tupleE = new TableTuple(edge->getTupleData(), input_etable->schema());
				if (!postfilterE.eval(tupleE, NULL)) {
					continue;
				}
				// check the other end
				unsigned from = edge->getStartVertexId();
				unsigned to = edge->getEndVertexId();
				TableTuple* tupleV2;
				if (from == curVertexId)
					tupleV2 = new TableTuple(edge->getEndVertex()->getTupleData(), input_vtable->schema());
				else
					tupleV2 = new TableTuple(edge->getStartVertex()->getTupleData(), input_vtable->schema());
				if (!postfilterV.eval(tupleV2, NULL))
					continue;
				Edge* newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
				Vertex* vFrom = createAndAddVertex(curVertexId, curVertex->getTupleData());
				Vertex* vTo = createAndAddVertex(to, edge->getStartVertex()->getTupleData());
				vFrom->addOutEdge(newEdge);
				vTo->addInEdge(newEdge);
				this->addEdge(edgeId, newEdge);
			}			
		}
    }
    else {
    	// edges must satisfy the edge predicate
    	// the end nodes must satisfy the vertex predicate
    	cout << "GraphView: Graph Intersection E" << endl;
    	Edge *curEdge;
		unsigned curEdgeId;
		std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

		// traverse all the edges and get their tuple data
		for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
		{
	    	curEdgeId = it->first;
			curEdge = it->second;
			TableTuple* tupleE = new TableTuple(curEdge->getTupleData(), input_etable->schema());

			//check if the tuple satisfy the predicate
			if (!postfilterE.eval(tupleE, NULL)) 
				continue;

			Vertex* vFrom = curEdge->getStartVertex();
			TableTuple* tupleV1 = new TableTuple(vFrom->getTupleData(), input_vtable->schema());
			Vertex* vTo = curEdge->getEndVertex();
			TableTuple* tupleV2 = new TableTuple(vTo->getTupleData(), input_vtable->schema());
			
			if (postfilterV.eval(tupleV1, NULL) &&postfilterV.eval(tupleV2, NULL) ) {
				Edge* newEdge = createEdge(curEdgeId, curEdge->getTupleData(), vFrom->getId(), vTo->getId());
				Vertex* newFrom = createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
				Vertex* newTo = createAndAddVertex(vTo->getId(), vTo->getTupleData());
				newFrom->addOutEdge(newEdge);
				newTo->addInEdge(newEdge);
				this->addEdge(curEdgeId, newEdge);
			}
		}
    }
}

void GraphView::fillGraphByUnion(AbstractExpression* vpred, AbstractExpression* epred, Table* input_vtable, Table* input_etable, bool useV, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
    CountingPostfilter postfilterE(epred, limit, offset);

    if (useV) {
    	// vertices must satisfy the vertex predicate
    	// the attached edges must satisfy the edge predicate
    	// first scan adds the intersected vertices
    	// second scan adds the edges if both end nodes are selected
    	Vertex* curVertex;
		unsigned curVertexId;
		std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
		int selectivity = 0;
		std::unordered_set<unsigned> visitedEdges;
		for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
		{

			curVertexId = it->first;
			curVertex = it->second;
			TableTuple* tupleV = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

			//check if the tuple satisfy the predicate
			if (postfilterV.eval(tupleV, NULL)) {
				createAndAddVertex(curVertexId, curVertex->getTupleData());
				selectivity++;
				continue;
			}
			// then check the attached edges
			// TODO: each edge is checked twice, optimize later to remember who have been checked
			std::vector<unsigned> edgeIds;
			if (oldGraphView->isDirected()) {
				// has to scan both inedges and out edges
				edgeIds = curVertex->getAllOutEdges();
				vector<unsigned> another = curVertex->getAllInEdges();
				edgeIds.insert(edgeIds.end(), another.begin(), another.end() );
			}
			else {
				edgeIds = curVertex->getAllOutEdges();
			}
			for (unsigned edgeId: edgeIds) {
				if (visitedEdges.find(edgeId) != visitedEdges.end())
					continue;
				visitedEdges.insert(edgeId);
				Edge *edge = oldGraphView->getEdge(edgeId);
				TableTuple* tupleE = new TableTuple(edge->getTupleData(), input_etable->schema());
				if (postfilterE.eval(tupleE, NULL)) {
					createAndAddVertex(curVertexId, curVertex->getTupleData());
					unsigned nextVertexId;
					if (curVertexId == edge->getStartVertexId()) {
						nextVertexId = edge->getEndVertexId();
					}
					else {
						nextVertexId = edge->getStartVertexId();
					}
					createAndAddVertex(nextVertexId, oldGraphView->getVertex(nextVertexId)->getTupleData());
				}
			}			
		}

		// has to scan oldGraphView edges again to add edges if both end nodes are selected
		std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
		Edge *curEdge;
		unsigned curEdgeId;
		for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
		{
			curEdgeId = it->first;
			curEdge = it->second;
			unsigned from = curEdge->getStartVertexId();
			unsigned to = curEdge->getEndVertexId();
			if ((this->hasVertex(from)) && (this->hasVertex(to))){
				Edge* newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);

				Vertex* vFrom = newEdge->getStartVertex();
				Vertex* vTo = newEdge->getEndVertex();
				vFrom->addOutEdge(newEdge);
				vTo->addInEdge(newEdge);
				this->addEdge(curEdgeId, newEdge);
			}
		}
		cout << "GraphView: graph union, selectivity = " << selectivity << endl;
    }
    else {
    	// edges must satisfy the edge predicate
    	// the end nodes must satisfy the vertex predicate
    	Edge* curEdge;
		unsigned curEdgeId;
		std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

		std::unordered_set<unsigned> visitedVertices;
		// traverse all the edges and get their tuple data
		for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
		{

	    	curEdgeId = it->first;
			curEdge = it->second;
			TableTuple* tupleE = new TableTuple(curEdge->getTupleData(), input_etable->schema());

			//check if the tuple satisfy the predicate
			if (postfilterE.eval(tupleE, NULL)) {
				Vertex* vFrom = curEdge->getStartVertex();
				createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
				Vertex* vTo = curEdge->getEndVertex();
				createAndAddVertex(vTo->getId(), vTo->getTupleData());
				continue;
			}
			Vertex* vFrom = curEdge->getStartVertex();
			TableTuple* tupleV1 = new TableTuple(vFrom->getTupleData(), input_vtable->schema());
			if (postfilterV.eval(tupleV1, NULL)) {
				createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
			}
			Vertex* vTo = curEdge->getEndVertex();
			TableTuple* tupleVe = new TableTuple(vTo->getTupleData(), input_vtable->schema());
			if (postfilterV.eval(tupleVe, NULL)) {
				createAndAddVertex(vTo->getId(), vTo->getTupleData());
			}
			visitedVertices.insert(vFrom->getId());
			visitedVertices.insert(vTo->getId());
		}

		// Some vertices may not be reachable from any vertices
		Vertex* curVertex;
		unsigned curVertexId;
		std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

		for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
		{
			curVertexId = it->first;
			if (visitedVertices.find(curVertexId) != visitedVertices.end())
				continue;
			curVertex = it->second;
			TableTuple* tupleV = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

			//check if the tuple satisfy the predicate
			if (postfilterV.eval(tupleV, NULL)) {
				createAndAddVertex(curVertexId, curVertex->getTupleData());
				continue;
			}
		}

		// has to scan oldGraphView edges again to add edges if both end nodes are selected
		for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
		{
			curEdgeId = it->first;
			curEdge = it->second;
			unsigned from = curEdge->getStartVertexId();
			unsigned to = curEdge->getEndVertexId();
			if ((this->hasVertex(from)) && (this->hasVertex(to))){
				Edge* newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);

				Vertex* vFrom = newEdge->getStartVertex();
				Vertex* vTo = newEdge->getEndVertex();
				vFrom->addOutEdge(newEdge);
				vTo->addInEdge(newEdge);
				this->addEdge(curEdgeId, newEdge);
			}
		}
    }
}

void GraphView::filterEndVertexPredicateFromVertex1(Table* input_table, AbstractExpression* vpred, AbstractExpression* vpred2, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter1(vpred, limit, offset);
    CountingPostfilter postfilter2(vpred2, limit, offset);

    // std::unordered_set<unsigned> visitedEdges; // so that we don't visit edges twice
    // get all the vertices from the old graph
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	// traverse all the vertices and get their tuple data
	int selectivity = 0;
	int totalFanOut = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tuple1 = new TableTuple(curVertex->getTupleData(), input_table->schema());
		if (!postfilter1.eval(tuple1, NULL))
			continue;
		selectivity++;
		std::vector<unsigned> edgeIds;
		if (oldGraphView->isDirected()) {
			edgeIds = curVertex->getAllOutEdges();
			vector<unsigned> another = curVertex->getAllInEdges();
			edgeIds.insert(edgeIds.end(), another.begin(), another.end() );
		}
		else {
			edgeIds = curVertex->getAllOutEdges();
		}
		totalFanOut = totalFanOut + edgeIds.size();

		unsigned from, to;
		Edge* edge;
		for (unsigned edgeId: edgeIds) {
			// if (visitedEdges.find(edgeId) != visitedEdges.end())
			// 	continue;
			// visitedEdges.insert(edgeId);
			edge = oldGraphView->getEdge(edgeId);
			from = edge->getStartVertexId();
			to = edge->getEndVertexId();
			TableTuple* tuple2;
			if (curVertexId == from) {
				tuple2 = new TableTuple(oldGraphView->getVertex(to)->getTupleData(), input_table->schema());
			}
			else {
				tuple2 = new TableTuple(oldGraphView->getVertex(from)->getTupleData(), input_table->schema());
			}
			if (postfilter2.eval(tuple2, NULL)) {
				Edge* newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
				Vertex* newFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				Vertex* newTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
				newFrom->addOutEdge(newEdge);
				newTo->addInEdge(newEdge);

				this->addEdge(edgeId, newEdge);
			}
		}
	}
	cout << "GraphView:filterEndVertexPredicateFromVertex1: selectivity = " << selectivity << ", totalFanOut = " << totalFanOut << endl;
}

void GraphView::filterEndVertexPredicateFromVertex2(Table* input_table, AbstractExpression* vpred, AbstractExpression* vpred2, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter1(vpred, limit, offset);
    CountingPostfilter postfilter2(vpred2, limit, offset);

    std::unordered_set<unsigned> visitedEdges; // so that we don't visit edges twice
    // get all the vertices from the old graph
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	// traverse all the vertices and get their tuple data
	int selectivity = 0;
	int totalFanOut = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tuple1 = new TableTuple(curVertex->getTupleData(), input_table->schema());
		if (!postfilter2.eval(tuple1, NULL))
			continue;
		selectivity++;
		std::vector<unsigned> edgeIds;
		if (oldGraphView->isDirected()) {
			edgeIds = curVertex->getAllOutEdges();
			vector<unsigned> another = curVertex->getAllInEdges();
			edgeIds.insert(edgeIds.end(), another.begin(), another.end() );
		}
		else {
			edgeIds = curVertex->getAllOutEdges();
		}
		totalFanOut = totalFanOut + edgeIds.size();

		unsigned from, to;
		Edge* edge;
		for (unsigned edgeId: edgeIds) {
			if (visitedEdges.find(edgeId) != visitedEdges.end())
				continue;
			visitedEdges.insert(edgeId);
			edge = oldGraphView->getEdge(edgeId);
			from = edge->getStartVertexId();
			to = edge->getEndVertexId();
			TableTuple* tuple2;
			if (curVertexId == from) {
				tuple2 = new TableTuple(oldGraphView->getVertex(to)->getTupleData(), input_table->schema());
			}
			else {
				tuple2 = new TableTuple(oldGraphView->getVertex(from)->getTupleData(), input_table->schema());
			}
			if (postfilter1.eval(tuple2, NULL) ) {
				Edge* newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
				Vertex* newFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				Vertex* newTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
				newFrom->addOutEdge(newEdge);
				newTo->addInEdge(newEdge);

				this->addEdge(edgeId, newEdge);
			}
		}
	}
	cout << "GraphView:filterEndVertexPredicateFromVertex2: selectivity = " << selectivity << ", totalFanOut = " << totalFanOut << endl;
}

void GraphView::filterEndVertexPredicateFromVertex(Table* input_table, AbstractExpression* vpred, AbstractExpression* vpred2, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter1(vpred, limit, offset);
    CountingPostfilter postfilter2(vpred2, limit, offset);

    std::unordered_set<unsigned> visitedEdges; // so that we don't visit edges twice
    // get all the vertices from the old graph
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	// traverse all the vertices and get their tuple data
	int selectivity = 0; 
	int totalFanOut = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tuple1 = new TableTuple(curVertex->getTupleData(), input_table->schema());
		if ((!postfilter1.eval(tuple1, NULL)) && (!postfilter2.eval(tuple1, NULL)))
			continue;
		selectivity++;
		std::vector<unsigned> edgeIds;
		if (oldGraphView->isDirected()) {
			edgeIds = curVertex->getAllOutEdges();
			vector<unsigned> another = curVertex->getAllInEdges();
			edgeIds.insert(edgeIds.end(), another.begin(), another.end() );
		}
		else {
			edgeIds = curVertex->getAllOutEdges();
		}
		totalFanOut = totalFanOut + edgeIds.size();

		unsigned from, to;
		Edge* edge;
		for (unsigned edgeId: edgeIds) {
			if (visitedEdges.find(edgeId) != visitedEdges.end())
				continue;
			visitedEdges.insert(edgeId);
			edge = oldGraphView->getEdge(edgeId);
			from = edge->getStartVertexId();
			to = edge->getEndVertexId();
			TableTuple* tuple2;
			if (curVertexId == from) {
				tuple2 = new TableTuple(oldGraphView->getVertex(to)->getTupleData(), input_table->schema());
			}
			else {
				tuple2 = new TableTuple(oldGraphView->getVertex(from)->getTupleData(), input_table->schema());
			}
			if ((postfilter1.eval(tuple1, NULL) && postfilter2.eval(tuple2, NULL)) || (postfilter1.eval(tuple2, NULL) && postfilter2.eval(tuple1, NULL))) {
				Edge* newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
				Vertex* newFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				Vertex* newTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
				newFrom->addOutEdge(newEdge);
				newTo->addInEdge(newEdge);

				this->addEdge(edgeId, newEdge);
			}
		}
	}
	cout << "GraphView:filterEndVertexPredicateFromVertex: selectivity = " << selectivity << ", totalFanOut = " << totalFanOut << endl;
}

void GraphView::filterEndVertexPredicateFromEdge(Table* input_table, AbstractExpression* vpred, AbstractExpression* vpred2, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter1(vpred, limit, offset);
    CountingPostfilter postfilter2(vpred2, limit, offset);

    // get all the edges from the old graph
	Edge* curEdge;
	unsigned curEdgeId;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

	// traverse all the edges 
	int selectivity = 0;
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		curEdgeId = it->first;
		curEdge = it->second;
		Vertex* vFrom = curEdge->getStartVertex();
		Vertex* vTo = curEdge->getEndVertex();

		TableTuple* tuple1 = new TableTuple(vFrom->getTupleData(), input_table->schema());
		TableTuple* tuple2 = new TableTuple(vTo->getTupleData(), input_table->schema());
		if ((postfilter1.eval(tuple1, NULL) && postfilter2.eval(tuple2, NULL)) || (postfilter1.eval(tuple2, NULL) && postfilter2.eval(tuple1, NULL))) {
			selectivity++;
			Edge* newEdge = createEdge(curEdgeId, curEdge->getTupleData(), curEdge->getStartVertexId(), curEdge->getEndVertexId());
			Vertex* newFrom = createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
			Vertex* newTo = createAndAddVertex(vTo->getId(), vTo->getTupleData());
			newFrom->addOutEdge(newEdge);
			newTo->addInEdge(newEdge);

			this->addEdge(curEdgeId, newEdge);
		}
	}
	cout << "GraphView:filterEndVertexPredicateFromEdge: selectivity = " << selectivity << endl;
}

void GraphView::filterEndVertexEdgePredicateFromVertex(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* vpred2, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter1(vpred, limit, offset);
    CountingPostfilter postfilter2(vpred2, limit, offset);
    CountingPostfilter postfilterE(epred, limit, offset);

    // get all the vertices from the old graph
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	// std::unordered_set<unsigned> visitedEdges;
	// traverse all the vertices and get their tuple data
	int selectivity = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tuple1 = new TableTuple(curVertex->getTupleData(), input_vtable->schema());
		if (!postfilter1.eval(tuple1, NULL)) 
			continue;
		selectivity++;
		std::vector<unsigned> edgeIds;
		if (oldGraphView->isDirected()) {
			edgeIds = curVertex->getAllOutEdges();
			vector<unsigned> another = curVertex->getAllInEdges();
			edgeIds.insert(edgeIds.end(), another.begin(), another.end() );
		}
		else {
			edgeIds = curVertex->getAllOutEdges();
		}

		unsigned from, to;
		Edge* edge;
		for (unsigned edgeId: edgeIds) {
			// if (visitedEdges.find(edgeId) != visitedEdges.end())
			// 	continue;
			// visitedEdges.insert(edgeId);
			edge = oldGraphView->getEdge(edgeId);
			TableTuple* tupleE = new TableTuple(edge->getTupleData(), input_etable->schema());
			if (!postfilterE.eval(tupleE, NULL)) 
				continue;
			
			from = edge->getStartVertexId();
			to = edge->getEndVertexId();
			TableTuple* tuple2;
			if (curVertexId == from) {
				tuple2 = new TableTuple(oldGraphView->getVertex(to)->getTupleData(), input_vtable->schema());
			}
			else {
				tuple2 = new TableTuple(oldGraphView->getVertex(from)->getTupleData(), input_vtable->schema());
			}
			if (postfilter2.eval(tuple2, NULL)) {
				Edge* newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
				Vertex* newFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				Vertex* newTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
				newFrom->addOutEdge(newEdge);
				newTo->addInEdge(newEdge);

				this->addEdge(edgeId, newEdge);
			}
		}
	}
	cout << "GraphView:From vertex, selectivity = " << selectivity << endl;
}

void GraphView::filterEndVertexEdgePredicateFromEdge(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* vpred2, AbstractExpression* epred, GraphView* oldGraphView)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter1(vpred, limit, offset);
    CountingPostfilter postfilter2(vpred2, limit, offset);
    CountingPostfilter postfilterE(epred, limit, offset);

    // get all the edges from the old graph
	Edge* curEdge;
	unsigned curEdgeId;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

	// traverse all the edges 
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		curEdgeId = it->first;
		curEdge = it->second;
		Vertex* vFrom = curEdge->getStartVertex();
		Vertex* vTo = curEdge->getEndVertex();
		TableTuple* tuple1 = new TableTuple(vFrom->getTupleData(), input_vtable->schema());
		TableTuple* tuple2 = new TableTuple(vTo->getTupleData(), input_vtable->schema());
		TableTuple* tuple3 = new TableTuple(curEdge->getTupleData(), input_etable->schema());
		if (postfilterE.eval(tuple3, NULL)) {
			if ((postfilter1.eval(tuple1, NULL) && postfilter2.eval(tuple2, NULL)) || (postfilter1.eval(tuple2, NULL) && postfilter2.eval(tuple1, NULL))) {
				Edge* newEdge = createEdge(curEdgeId, curEdge->getTupleData(), curEdge->getStartVertexId(), curEdge->getEndVertexId());
				Vertex* newFrom = createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
				Vertex* newTo = createAndAddVertex(vTo->getId(), vTo->getTupleData());
				newFrom->addOutEdge(newEdge);
				newTo->addInEdge(newEdge);

				this->addEdge(curEdgeId, newEdge);
			}
		}
	}
}

AbstractExpression* GraphView::getPredicateFromWhere(const string& pred) 
{
	AbstractExpression* predicate = NULL;
	vassert(pred.length() % 2 == 0);
    int bufferLength = (int)pred.size() / 2 + 1;
    boost::shared_array<char> buffer(new char[bufferLength]);
    catalog::Catalog::hexDecodeString(pred, buffer.get());

    PlannerDomRoot domRoot(buffer.get());
    if (domRoot.isNull()) {
        return NULL;
    }
    PlannerDomValue expr = domRoot();

	predicate = AbstractExpression::buildExpressionTree(expr);
	return predicate;
}

void GraphView::fillSubGraphFromRelationalTables(const string& subGraphVPredicate, const string& subGraphVPredicate2, const string& subGraphEPredicate, std::string graphPredicate, std::string joinVEPredicate, GraphView* oldGraphView, std::string vlabelName, std::string elabelName, bool useV)
{
	// stringstream output;
	// output << graphPredicate << ", " << joinVEPredicate << ", " << useV << endl;
	// LogManager::GLog("GraphView", "full subgraph", 2016, output.str());
	this->m_vertexes.clear();
	this->m_edges.clear();

	// LX FEAT4
	// start from the oldGraphView, check vertex by vertex, or edge by edge
	// filter by the subgraphPredicate

	// first check the predicate is about vertex or edge
    std::string curLabel = "";
    Table* input_vtable = NULL;
    Table* input_etable = NULL;
    bool isV = false;
    bool isV2 = false;
    bool isE = false;
    AbstractExpression* vpred = NULL;
    // AbstractExpression* vpred2 = NULL;
    AbstractExpression* epred = NULL;
    if (subGraphVPredicate.size() != 0) {
    	curLabel = vlabelName;
    	input_vtable = oldGraphView->getVertexTableFromLabel(curLabel);
    	isV = true;
    	vpred = getPredicateFromWhere(subGraphVPredicate);
    }
    if (subGraphVPredicate2.size() != 0) {
    	// then the previous predicate cannot be null be default
    	isV2 = true;
    // 	vpred2 = getPredicateFromWhere(subGraphVPredicate2);
    }
    if (subGraphEPredicate.size() != 0) {
    	curLabel = elabelName;
    	input_etable = oldGraphView->getEdgeTableFromLabel(curLabel);
    	isE = true;
    	epred = getPredicateFromWhere(subGraphEPredicate);
    }		

    // int labelIdx = oldGraphView->getIndexFromVertexLabels(curLabel);

    stringstream output;
    output << "check:" << isV << ", " << isV2 << ", " << isE << ", " << useV << endl;;
    LogManager::GLog("GraphView", "full subGraph", 2052, output.str()); 

    if (useV) {
    	// cout << "GraphView:fillSubGraph:select bound vertices from v" << endl;
    	// selectOnlyBoundVerticesFromVertex(input_vtable, vpred, oldGraphView);
    	// cout << "GraphView:fillSubGraph:select free vertices from v" << endl;
    	// selectOnlyFreeVerticesFromVertex(input_etable, epred, oldGraphView);
    	cout << "GraphView: select bound and free vertices from v" << endl;
    	selectFreeBoundVerticesFromVertex(input_vtable, input_etable, vpred, epred, oldGraphView);
    	// cout << "GraphView: select bound intersects free vertices from v" << endl;
    	// selectFreeIntersectBoundVerticesFromVertex(input_vtable, input_etable, vpred, epred, oldGraphView);
    	// cout << "GraphView: selct bound edges from v " << endl;
    	// selectBoundEdgesFromVertex(input_etable, epred, oldGraphView);
    	// cout  << "GraphView: select free edges from v" << endl;
    	// selectFreeEdgesFromVertex(input_vtable, vpred, oldGraphView);
    	// cout << "GraphView: free union bound edges from v" << endl;
    	// selectFreeBoundEdgesFromVertex(input_vtable, input_etable, vpred, epred, oldGraphView);
    	// cout << "GraphView: free intersects bound edges from v" << endl;
    	// selectFreeIntersectBoundEdgesFromVertex(input_vtable, input_etable, vpred, epred, oldGraphView);
    }
    else {
    	// cout << "GraphView:fillSubGraph:select bound vertices from e" << endl;
    	// selectOnlyBoundVerticesFromEdge(input_vtable, vpred, oldGraphView);
    	// cout << "GraphView:fillSubGraph:select free vertices from e" << endl;
    	// selectOnlyFreeVerticesFromEdge(input_etable, epred, oldGraphView);
    	cout << "GraphView: select bound and free vertices from e" << endl;
    	selectFreeBoundVerticesFromEdge(input_vtable, input_etable, vpred, epred, oldGraphView);
    	// cout << "GraphView: select bound intersects free vertices from e" << endl;
    	// selectFreeIntersectBoundVerticesFromEdge(input_vtable, input_etable, vpred, epred, oldGraphView);
    	// cout << "GraphView: selct bound edges from e " << endl;
    	// selectBoundEdgesFromEdge(input_etable, epred, oldGraphView);
    	// cout  << "GraphView: select free edges from e" << endl;
    	// selectFreeEdgesFromEdge(input_vtable, vpred, oldGraphView);
    	// cout << "GraphView: free union bound edges from e" << endl;
    	// selectFreeBoundEdgesFromEdge(input_vtable, input_etable, vpred, epred, oldGraphView);
    	// cout << "GraphView: free intersects bound edges from e" << endl;
    	// selectFreeIntersectBoundEdgesFromEdge(input_vtable, input_etable, vpred, epred, oldGraphView);
    }
    // only vertex or edge predicate
   //  if (isV && !isE) {
   //  	// only vertex predicate
   //  	if (isV2) {
   //  		// if (graphPredicate.compare("V1<->V2 IN E") == 0) {
   //  		// 	// for now only support constraints on end nodes of an edge
   //  		// 	if (useV) {
   //  		// 		LogManager::GLog("GraphView", "fillSubGraph", 2059, "2 vertex pred, V1<->V2 IN E, traverse vertex");
   //  		// 		filterEndVertexPredicateFromVertex(input_vtable, vpred, vpred2, oldGraphView);
   //  		// 	}
   //  		// 	else {
   //  		// 		LogManager::GLog("GraphView", "fillSubGraph", 2063, "2 vertex pred, V1<->V2 IN E, traverse edge");
   //  		// 		filterEndVertexPredicateFromEdge(input_vtable, vpred, vpred2, oldGraphView);
   //  		// 	}
   //  		// }
   //  		if (graphPredicate.compare("V1<->V2 IN E 1") == 0) {
   //  			cout << "GraphView:fillSubGraph: #1" << endl;
   //  			filterEndVertexPredicateFromVertex1(input_vtable, vpred, vpred2, oldGraphView);
   //  		}
   //  		if (graphPredicate.compare("V1<->V2 IN E 2") == 0) {
   //  			cout << "GraphView:fillSubGraph: #2" << endl;
   //  			filterEndVertexPredicateFromVertex2(input_vtable, vpred, vpred2, oldGraphView);
   //  		}
   //  		if (graphPredicate.compare("V1<->V2 IN E 3") == 0) {
   //  			cout << "GraphView:fillSubGraph: #3" << endl;
   //  			filterEndVertexPredicateFromVertex(input_vtable, vpred, vpred2, oldGraphView);
   //  		}
   //  		if (graphPredicate.compare("V1<->V2 IN E 4") == 0) {
   //  			cout << "GraphView:fillSubGraph: #4" << endl;
   //  			filterEndVertexPredicateFromEdge(input_vtable, vpred, vpred2, oldGraphView);
   //  		}
   //  	}
   //  	else if (useV) {
    		
			// LogManager::GLog("GraphView", "fillSubGraph", 2143, "Only vertex predicates, VertexScan");
			// cout << "GraphView:fillSubGraph:Only vertex predicates, VertexScan" << endl;
   //  		selectOnlyBoundVerticesFromVertex(input_vtable, vpred, oldGraphView);
    		
   //  	}
   //  	else {
   //  		// LogManager::GLog("GraphView", "fillSubGraph", 2073, "1 vertex pred, traverse edge");
   //  		cout << "GraphView:fillSubGraph:Only vertex predicates, EdgeScan" << endl;
   //  		selectOnlyBoundVerticesFromEdge(input_vtable, vpred, oldGraphView);
    		
    		
   //  	}
   //  }
   //  else if (!isV && isE) {
   //  	// only edge predicate
   //  	if (useV) {
   //  		LogManager::GLog("GraphView", "fillSubGraph", 2080, "1 edge pred, traverse vertex");
   //  		cout << "GraphView:fillSubGraph:Only edge predicates, free vertex from v" << endl;	
   //  		// filterGraphEdgeFromEdgeTable(input_etable, labelIdx, epred, oldGraphView);
   //  		// filterGraphEdgeFromVertex(input_etable, epred, oldGraphView);
   //  		selectOnlyFreeVerticesFromVertex(input_etable, epred, oldGraphView);
   //  	}
   //  	else {
   //  		LogManager::GLog("GraphView", "fillSubGraph", 2084, "1 edge pred, traverse edge");
   //  		cout << "GraphView:fillSubGraph:Only edge predicates, free vertex from e" << endl;	
   //  		// filterGraphEdgeFromEdge(input_etable, epred, oldGraphView);
   //  		selectOnlyFreeVerticesFromEdge(input_etable, epred, oldGraphView);
   //  	}
   //  }
    // else if (isV && isE) {
    	// both vertex and edge predicates
    	// if (graphPredicate.length() == 0) {
    	// 	if (joinVEPredicate.compare("AND") == 0) {
	    // 		fillGraphByIntersection(vpred, epred, input_vtable, input_etable, useV, oldGraphView);
    	// 	}
	    // 	else if (joinVEPredicate.compare("OR1") == 0) {
	    // 		cout << "GraphView: Graph Union 1" << endl;
	    // 		fillGraphByUnion(vpred, epred, input_vtable, input_etable, useV, oldGraphView, 1);
	    // 	}
	    // 	else if (joinVEPredicate.compare("OR2") == 0) {
	    // 		cout << "GraphView: Graph Union 2" << endl;
	    // 		fillGraphByUnion(vpred, epred, input_vtable, input_etable, useV, oldGraphView, 2);
	    // 	}
	    // 	else if (joinVEPredicate.compare("OR3") == 0) {
	    // 		cout << "GraphView: Graph Union 3" << endl;
	    // 		fillGraphByUnion(vpred, epred, input_vtable, input_etable, useV, oldGraphView, 3);
	    // 	}
    	// }
	    // else if (graphPredicate.compare("V1<->V2=E") == 0) {
	    // 	if (useV) {
	    // 		LogManager::GLog("GraphView", "fillSubGraph", 2102, "vertex, edge pred, V1<->V2=E, traverse vertex");
	    // 		cout << "GraphView:fillSubGraph:V1<->V2=E, From Vertex" << endl;
	    // 		filterEndVertexEdgePredicateFromVertex(input_vtable, input_etable, vpred, vpred2, epred, oldGraphView);
	    // 	}
	    // 	else {
	    // 		LogManager::GLog("GraphView", "fillSubGraph", 2106, "vertex, edge pred, V1<->V2=E, traverse edge");
	    // 		cout << "GraphView:fillSubGraph:V1<->V2=E, From Edge" << endl;
	    // 		filterEndVertexEdgePredicateFromEdge(input_vtable, input_etable, vpred, vpred2, epred, oldGraphView);
	    // 	}
	    // }
    // }
    // we don't need a else stmt to catch remaining cases
	    
	cout << "Generate a subgraph of " << this->numOfVertexes() << " vertices and of " << this->numOfEdges() << " edges." << endl;
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
	Edge* currentEdge;
	for (std::map<unsigned,Vertex* >::iterator it= this->m_vertexes.begin(); it != this->m_vertexes.end(); ++it)
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
	output << "Edges" << endl;
	for (std::map<unsigned,Edge* >::iterator it= this->m_edges.begin(); it != this->m_edges.end(); ++it)
	{
		currentEdge = it->second;
		output << "\t\t\t" << currentEdge->toString() << endl;
	}
	output << "#############" << endl;
	return output.str();
}

GraphView::~GraphView(void)
{
}

}
