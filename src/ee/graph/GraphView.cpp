#include "GraphView.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/TupleSchemaBuilder.h"
#include "common/debuglog.h"
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
typedef pair<unsigned, unsigned> endNodesPair;
typedef tuple<unsigned, unsigned, unsigned> triple;

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

// TableTuple* GraphView::getVertexTuple(unsigned id) // LX FEAT2
// {
// 	Vertex* v = this->getVertex(id);
// 	Table* t = this->getVertexTableById(id);
// 	return new TableTuple(v->getTupleData(), t->schema());
// }

Table* GraphView::getVertexTableFromLabel(string vlabel)
{
	for (int i = 0; i < (this->m_vertexLabels).size(); i++){
		if (((this->m_vertexLabels[i]).compare(vlabel)) == 0)
			return this->m_vertexTables[i];
	}
	return NULL;
}

// Table* GraphView::getVertexTableById(unsigned id)
// {
// 	return this->m_idToVTableMap[id];
// }

int GraphView::getVertexLabelNum() {
	return this->m_vertexLabels.size();
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

// Table* GraphView::getEdgeTableById(unsigned id)
// {
// 	return this->m_idToETableMap[id];
// }

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

// TableTuple* GraphView::getEdgeTuple(unsigned id) // LX FEAT2
// {
// 	Edge* e = this->getEdge(id);
// 	Table* t = this->getEdgeTableById(id);
// 	return new TableTuple(e->getTupleData(), t->schema());
// }

int GraphView::getEdgeLabelNum() {
	return this->m_edgeLabels.size();
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
	Table *etab;
	int length;
	unsigned eid;
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
			e = v->getOutEdge(i);
			eid = v->getOutEdgeId(i);
			if (this->getEdgeLabelNum() == 1)
				etab = m_edgeTables[0];
			else
				etab = m_edgeTables[eid % 10];
			candVertexId = e->getEndVertex()->getId();

			if (spColumnIndexInEdgesTable >= 0)
			{
				// TODO: fix this later
				// edgeTuple = this->getEdgeTuple(e->getTupleData());
				// edgeTuple = this->getEdgeTuple(es);
				edgeTuple = new TableTuple(e->getTupleData(), etab->schema());
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

	
	// LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "graphView 10/3 xxxxxxxxx\n");
	bool vPropExists = (m_vPropColumnIndex >= 0);
	bool ePropExists = (m_ePropColumnIndex >= 0);

	// string id = ""; // LX FEAT2 string takes too much space, use int instead
	// LX this is a decimal number. The least significant position holds the index of label (at most 9 labels)
	unsigned id; // the maximum number is 4294967295
	int idCol;
	//fill the vertex collection
	// LX FEAT2
	int tupleval;
	for (int i = 0; i < this->m_vertexTables.size(); i++){
		Table* curTable = this->m_vertexTables[i];
		string curLabel = this->m_vertexLabels[i];

		TableIterator iter = curTable->iterator();
		schema = curTable->schema();
		TableTuple tuple(schema);
		Vertex* vertex = NULL;
		idCol = m_vertexIdColIdxList[curLabel];

		if (curTable->activeTupleCount() != 0)
		{
			while (iter.next(tuple))
			{
				if (tuple.isActive())
				{
					// id = ValuePeeker::peekInteger(tuple.getNValue(m_vertexIdColumnIndex));
					// id = curLabel + "." + to_string(ValuePeeker::peekInteger(tuple.getNValue(m_vertexIdColIdxList[curLabel])));
					tupleval = ValuePeeker::peekInteger(tuple.getNValue(idCol));
					// if (tupleval % 1000000 == 0)
					// 	cout << "." << endl;
					// if there is only one label, no transformation
					if (this->getVertexLabelNum() == 1)
						id = tupleval;
					else
						id = tupleval * 10 + i;
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
					// this->m_idToVTableMap[id] = curTable;
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
	int fromCol, toCol;
	//fill the edge collection
	for (int i = 0; i < this->m_edgeTables.size(); i++){
		Table* curTable = this->m_edgeTables[i];
		string curLabel = this->m_edgeLabels[i];
		string fromVertexLabel = this->m_startVLabels[i];
		string endVertexLabel = this->m_endVLabels[i];
		int fromIdx = getIndexFromVertexLabels(fromVertexLabel);
		int toIdx = getIndexFromVertexLabels(endVertexLabel);
		idCol = m_edgeIdColIdxList[curLabel];
		fromCol = m_edgeFromColIdxList[curLabel];
		toCol = m_edgeToColIdxList[curLabel];

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
					tupleval = ValuePeeker::peekInteger(edgeTuple.getNValue(idCol));
					// if (tupleval % 50000000 == 0)
					// 	cout << "*" << endl;
					// if there is only one edge label
					if (this->getEdgeLabelNum() == 1) {
						id = tupleval;
						from = ValuePeeker::peekInteger(edgeTuple.getNValue(fromCol));
						to = ValuePeeker::peekInteger(edgeTuple.getNValue(toCol));
					}
					else {
						id = tupleval * 10 + i;
						from = ValuePeeker::peekInteger(edgeTuple.getNValue(fromCol)) * 10 + fromIdx;
						to = ValuePeeker::peekInteger(edgeTuple.getNValue(toCol)) * 10 + toIdx;
					}
						
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
					// This is the first time edge appears
					// No need to check for edge duplicates
					vFrom->addOutEdge(edge);
					vTo->addInEdge(edge);

					if(!this->isDirected())
					{
						vTo->addOutEdge(edge);
						vFrom->addInEdge(edge);
					}

					this->addEdge(id, edge);
					// cout << "GraphView:1335:edge id is " << id << endl; 
					// this->m_idToETableMap[id] = curTable;
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

void GraphView::selectOnlyBoundVerticesFromVertex(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView, int lim) 
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);

	// predicate is about vertices and we want to start from vertices
	// get all the vertices from the old graph
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	// traverse all the vertices and get their tuple data
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 10)
			break;
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tuple = new TableTuple(curVertex->getTupleData(), input_table->schema());

		//check if the tuple satisfy the predicate
		if (postfilter.eval(tuple, NULL)) {
			createAndAddVertex(curVertexId, curVertex->getTupleData());
		}
		ct++;
	}

	char msg[512];
    snprintf(msg, sizeof(msg), "inputG_size = %d.", ct);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectOnlyBoundVerticesFromEdge(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView, int lim)
{
	// this function is very counter-intuitive
	// because of the isolated vertices

	// if we don't create vertices redundantly, then the total time is shortened
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);

    // predicate is about vertices and we want to start from edges
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	Edge *curEdge;
	unsigned from, to;
	Vertex* vFrom;
	Vertex* vTo;
	int create_v = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 10)
			break;
		// curEdgeId = it->first;
		curEdge = it->second;
		vFrom = curEdge->getStartVertex();
		vTo = curEdge->getEndVertex();
		// if (!this->hasVertex(vFrom->getId())) {
			TableTuple* fromtuple = new TableTuple(vFrom->getTupleData(), input_table->schema());
			if (postfilter.eval(fromtuple, NULL)) {
				from = curEdge->getStartVertexId();
				createAndAddVertex(from, vFrom->getTupleData());
				create_v++;
			}
		// }
		// if (!this->hasVertex(vTo->getId())) {
			TableTuple* totuple = new TableTuple(vTo->getTupleData(), input_table->schema());
			if (postfilter.eval(totuple, NULL)) {
				to = curEdge->getEndVertexId();
				createAndAddVertex(to, vTo->getTupleData());
				create_v++;
			}
		// }
		ct++;
	}

	// Some vertices may not be reachable from any vertices
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		curVertexId = it->first;
		curVertex = it->second;
		if (!this->hasVertex(curVertexId)) {
			TableTuple* tuple = new TableTuple(curVertex->getTupleData(), input_table->schema());
			if (postfilter.eval(tuple, NULL)) {
				createAndAddVertex(curVertexId, curVertex->getTupleData());
				create_v++;
			}
		}
	}

	char msg[512];
    snprintf(msg, sizeof(msg), "inputG_size = %d, create_v = %d.", ct, create_v);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectOnlyFreeVerticesFromVertex(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView, int lim) 
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);

	Vertex* curVertex;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	int create_v = 0;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 10)
			break;
		curVertex = it->second;
		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();

		unsigned from, to;
		bool needFrom = false;
		// iterate all related edges (all edges are visited twice)
		for (unsigned edgeId: edgeIds) {
			visited_fanout++;
			Edge *edge = oldGraphView->getEdge(edgeId);
			TableTuple* tuple = new TableTuple(edge->getTupleData(), input_table->schema());
			
			if (postfilter.eval(tuple, NULL)) {
				from = edge->getStartVertexId();
				to = edge->getEndVertexId();
				// createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				needFrom = true;
				createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
				create_v++;
			}
		}	
		if (needFrom) {	
			createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			create_v++;
		}
		ct++;
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "visited_fanout = %d, inputG_size = %d, create_v = %d.", visited_fanout, ct, create_v);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectOnlyFreeVerticesFromEdge(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView, int lim)
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
	int create_v = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 10)
			break;
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
			create_v = create_v + 2;
		}
		ct++;
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "inputG_size = %d, create_v = %d.", ct, create_v);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectFreeBoundVerticesFromVertex(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	int create_v = 0;
	int bound_v = 0;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 10)
			break;
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tupleV = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

		bool added = false;
		//check if the tuple satisfy the predicate
		if (postfilterV.eval(tupleV, NULL)) {
			// bound vertices
			added = true;
			bound_v++;
		}
		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
		unsigned to;
		
		for (unsigned edgeId: edgeIds) {
			visited_fanout++;
			Edge *edge = oldGraphView->getEdge(edgeId);
			TableTuple* tupleE = new TableTuple(edge->getTupleData(), input_etable->schema());
			if (postfilterE.eval(tupleE, NULL)) {
				// from = edge->getStartVertexId();
				to = edge->getEndVertexId();
				added = true;
				if (!this->hasVertex(to)) {
					createAndAddVertex(to, edge->getStartVertex()->getTupleData());
					create_v++;
				}
			}
		}
		if (added) {
			createAndAddVertex(curVertexId, curVertex->getTupleData());
			create_v++;
		}
		ct++;
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "visited_fanout = %d, inputG_size = %d, create_v = %d.", visited_fanout, ct, create_v);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectFreeBoundVerticesFromEdge(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Edge *curEdge;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	// int visited_fanout = 0;
	int create_v = 0;
	int bound_e = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 10)
			break;
		curEdge = it->second;
		TableTuple* tupleE = new TableTuple(curEdge->getTupleData(), input_etable->schema());

		Vertex* vFrom = curEdge->getStartVertex();	
		Vertex* vTo = curEdge->getEndVertex();
		
		//check if the tuple satisfy the predicate
		if (postfilterE.eval(tupleE, NULL)) {
			createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
			createAndAddVertex(vTo->getId(), vTo->getTupleData());
			create_v = create_v + 2;
			bound_e++;
		}
		else {
			// visited_fanout = visited_fanout + 1;
			TableTuple* tupleV1 = new TableTuple(vFrom->getTupleData(), input_vtable->schema());
			TableTuple* tupleV2 = new TableTuple(vTo->getTupleData(), input_vtable->schema());
			if (postfilterV.eval(tupleV1, NULL)) {
				createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
				create_v++;
			}
			if (postfilterV.eval(tupleV2, NULL)){
				createAndAddVertex(vTo->getId(), vTo->getTupleData());
				create_v++;
			}
		}
		ct++;
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
			// visited_fanout++;
			createAndAddVertex(curVertexId, curVertex->getTupleData());
			create_v++;
		}
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "inputG_size = %d, create_v = %d.", ct, create_v);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectFreeIntersectBoundVerticesFromVertex(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	int create_v = 0;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 10)
			break;
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
				create_v++;
				break;
			}
		}		
		ct++;	
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "visited_fanout = %d, inputG_size = %d, create_v = %d.", visited_fanout, ct, create_v);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectFreeIntersectBoundVerticesFromEdge(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Edge *curEdge;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	int bound_e = 0;
	int create_v = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 10)
			break;
		curEdge = it->second;
		TableTuple* tupleE = new TableTuple(curEdge->getTupleData(), input_etable->schema());

		//check if the tuple satisfy the predicate
		if (postfilterE.eval(tupleE, NULL)) {
			bound_e++;
			Vertex* vFrom = curEdge->getStartVertex();
			TableTuple* tupleV1 = new TableTuple(vFrom->getTupleData(), input_vtable->schema());
			if (postfilterV.eval(tupleV1, NULL)){
				createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
				create_v++;
			} 
		}
		ct++;
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "inputG_size = %d, create_v = %d.", ct, create_v);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectBoundEdgesFromVertex(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	// predicate is about edges and we want to start from vertices
	// get all the vertices from the old graph
	Vertex* curVertex;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0; // it is always the number of the total edges
	int create_v = 0;
	int create_e = 0;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	// traverse all the vertices
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 10)
			break;
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
				Edge *newEdge = createEdge(edgeId, edge->getTupleData(), from, to);
	    		
				// add these two vertices to new graph
				Vertex* vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
				Vertex* vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
				create_v = create_v + 2;
				create_e++;
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
			ct++;
		}				
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "visited_fanout = %d, inputG_size = %d, create_v = %d, create_e = %d.", visited_fanout, ct, create_v, create_e);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectBoundEdgesFromEdge(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	// predicate is about edges and we want to start from edges
	// get all the edges from the old graph
	Edge* curEdge;
	unsigned curEdgeId, from, to;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	int create_v = 0;
	int create_e = 0;
	// int adj_size = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 10)
			break;
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
			create_v = create_v + 2;
			create_e++;
			//update the endpoint vertexes in and out lists
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);
			// adj_size = adj_size + vFrom->fanOut() + vTo->fanIn();
			if(!this->isDirected())
			{
				vTo->addOutEdge(newEdge);
				vFrom->addInEdge(newEdge);
				// adj_size = adj_size + vFrom->fanIn() + vTo->fanOut();
			}
			this->addEdge(curEdgeId, newEdge);
		}
		ct++;
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "inputG_size = %d, create_v = %d, create_e = %d.", ct, create_v, create_e);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView:: selectFreeEdgesFromVertex(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView, int lim)
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
	int create_v = 0;
	int create_e = 0;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	// traverse all the vertices
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 10)
			break;
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
				create_v = create_v + 2;
				create_e++;
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
		ct++;		
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "visited_fanout = %d, inputG_size = %d, create_v = %d, create_e = %d.", visited_fanout, ct, create_v, create_e);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectFreeEdgesFromEdge(Table* input_table, AbstractExpression* predicate, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(predicate, limit, offset);
	// predicate is about edges and we want to start from edges
	// get all the edges from the old graph
	Edge* curEdge;
	unsigned curEdgeId, from, to;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	// int visited_fanout = 0;
	int create_v = 0;
	int create_e = 0;
	// int adj_size = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 10)
			break;
		curEdgeId = it->first;
		curEdge = it->second;
		Vertex* vFrom = curEdge->getStartVertex();
		Vertex* vTo = curEdge->getEndVertex();
		TableTuple* tuple1 = new TableTuple(vFrom->getTupleData(), input_table->schema());
		TableTuple* tuple2 = new TableTuple(vTo->getTupleData(), input_table->schema());

		//check if the tuple satisfy the predicate
		if (postfilter.eval(tuple1, NULL) && postfilter.eval(tuple2, NULL)) {
			// visited_fanout++;
			from = curEdge->getStartVertexId();
			to = curEdge->getEndVertexId();
			Edge *newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);
    		
			// add these two vertices to new graph
			Vertex* vNewFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			Vertex* vNewTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
			create_v = create_v + 2;
			create_e++;
			//update the endpoint vertexes in and out lists
			vNewFrom->addOutEdge(newEdge);
			vNewTo->addInEdge(newEdge);
			// adj_size = adj_size + vNewFrom->fanOut() + vNewTo->fanIn();
			if(!this->isDirected())
			{
				vNewTo->addOutEdge(newEdge);
				vNewFrom->addInEdge(newEdge);
				// adj_size = adj_size + vNewTo->fanOut() + vNewFrom->fanIn();
			}
			this->addEdge(curEdgeId, newEdge);
		}
		ct++;
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "inputG_size = %d, create_v = %d, create_e = %d.", ct, create_v, create_e);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}


void GraphView::selectFreeBoundEdgesFromVertex(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	int create_e = 0;
	int create_v = 0;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 10)
			break;
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tupleV = new TableTuple(curVertex->getTupleData(), input_vtable->schema());

		bool needAdd = false;
		//check if the tuple satisfy the predicate
		if (postfilterV.eval(tupleV, NULL)) {
			// bound vertices
			// createAndAddVertex(curVertexId, curVertex->getTupleData());
			// create_v++;
			needAdd = true;
		}
		// else {
			// free vertices
		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
		unsigned to;

		for (unsigned edgeId: edgeIds) {
			visited_fanout++;
			Edge *edge = oldGraphView->getEdge(edgeId);
			TableTuple* tupleE = new TableTuple(edge->getTupleData(), input_etable->schema());
			if (postfilterE.eval(tupleE, NULL)) {
				// from = edge->getStartVertexId();
				to = edge->getEndVertexId();
				needAdd = true;
				// createAndAddVertex(from, edge->getEndVertex()->getTupleData());
				createAndAddVertex(to, edge->getStartVertex()->getTupleData());
				create_v++;
			}
		}
		if (needAdd) {
			createAndAddVertex(curVertexId, curVertex->getTupleData());
			create_v++;
		}
		ct++;					
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
			create_e++;
			Vertex* vFrom = newEdge->getStartVertex();
			Vertex* vTo = newEdge->getEndVertex();
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);
			this->addEdge(curEdgeId, newEdge);
		}
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "visited_fanout = %d, inputG_size = %d, create_v = %d, create_e = %d.", visited_fanout, ct, create_v, create_e);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectFreeBoundEdgesFromEdge(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Edge *curEdge;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	int bound_e = 0;
	int create_v = 0;
	int create_e = 0;
	// int adj_size = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 10)
			break;
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
			bound_e++;
			create_v = create_v + 2;
		}
		else {
			// visited_fanout++;
			if (postfilterV.eval(tupleV1, NULL)) {
				createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
				create_v++;
			}
			if (postfilterV.eval(tupleV2, NULL)){
				createAndAddVertex(vTo->getId(), vTo->getTupleData());
				create_v++;
			}
		}
		ct++;
	}
	// Some vertices may not be reachable from any vertices
	// this traversal is not needed
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
			// visited_fanout++;
			createAndAddVertex(curVertexId, curVertex->getTupleData());
			create_v++;
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
			// visited_fanout++;
			Edge* newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);
			create_e++;
			Vertex* vFrom = newEdge->getStartVertex();
			Vertex* vTo = newEdge->getEndVertex();
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);
			// adj_size = adj_size + vFrom->fanOut() + vTo->fanIn();
			this->addEdge(curEdgeId, newEdge);
		}
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "inputG_size = %d, create_v = %d, create_e = %d.", ct, create_v, create_e);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectFreeIntersectBoundEdgesFromVertex(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int visited_fanout = 0;
	int create_v = 0;
	int create_e = 0;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 10)
			break;
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
				create_e++;
				create_v = create_v + 2;
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
		ct++;	
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "visited_fanout = %d, inputG_size = %d, create_v = %d, create_e = %d.", visited_fanout, ct, create_v, create_e);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::selectFreeIntersectBoundEdgesFromEdge(Table* input_vtable, Table* input_etable, AbstractExpression* vpred, AbstractExpression* epred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilterV(vpred, limit, offset);
	CountingPostfilter postfilterE(epred, limit, offset);

	Edge *curEdge;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	unsigned from, to;
	// int visited_fanout = 0;
	int create_v = 0;
	int create_e = 0;
	// int adj_size = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	// traverse all the edges and get their tuple data
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 10)
			break;
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
			// visited_fanout++;
			Edge *newEdge = createEdge(curEdge->getId(), curEdge->getTupleData(), from, to);
	    		
			Vertex* vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			Vertex* vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
			create_e++;
			create_v = create_v + 2;
			vFrom->addOutEdge(newEdge);
			vTo->addInEdge(newEdge);
			// adj_size = adj_size + vFrom->fanOut() + vTo->fanIn();
			if(!this->isDirected())
			{
				vTo->addOutEdge(newEdge);
				vFrom->addInEdge(newEdge);
			}
			this->addEdge(curEdge->getId(), newEdge);
		}
		ct++;
	}
	
	char msg[512];
    snprintf(msg, sizeof(msg), "inputG_size = %d, create_v = %d, create_e = %d.", ct, create_v, create_e);
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
}

void GraphView::checkVertexConstructTime(GraphView* oldGraphView, int vLimit)
{
	Vertex* curVertex;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();

	// traverse all the vertices and get their tuple data
	int counter = 0;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (counter < vLimit) {
			curVertexId = it->first;
			curVertex = it->second;
			createAndAddVertex(curVertexId, curVertex->getTupleData());
		}
		
		counter++;
	}
}

void GraphView::checkVertexAndEdgeConstructTime(GraphView* oldGraphView, int eLimit)
{
	unsigned curEdgeId, from, to;
	Edge* curEdge;
	int counter = 0;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (counter < eLimit) {
			curEdgeId = it->first;
			curEdge = it->second;
			// TableTuple* tuple = new TableTuple(curEdge->getTupleData(), input_table->schema());

			from = curEdge->getStartVertexId();
			to = curEdge->getEndVertexId();
			Edge *newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);
			
			// add these two vertices to new graph
			Vertex *vFrom, *vTo;
	  		if (this->hasVertex(from))
	  			vFrom = this->getVertex(from);
	  		else
				vFrom = createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());

			if (this->hasVertex(to))
	  			vTo = this->getVertex(to);
	  		else
				vTo = createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());

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
			
		counter++;
	}
}

void GraphView::checkEdgeConstructTime(GraphView* oldGraphView, int eLimit)
{
	unsigned curEdgeId, from, to;
	Edge* curEdge;
	int counter = 0;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (counter < eLimit) {
			curEdgeId = it->first;
			curEdge = it->second;
			
			from = curEdge->getStartVertexId();
			to = curEdge->getEndVertexId();
			Edge *newEdge = createEdge(curEdgeId, curEdge->getTupleData(), from, to);

			this->addEdge(curEdgeId, newEdge);
		}		
		counter++;
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

void GraphView::selectBoundVerticesFromVThenVC(Table* input_table, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	// first get all free edges with their bound vertices
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);

	Vertex* curVertex;
	unsigned curVertexId;
	unsigned from, to;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int fanOut = 0;
	int intermediateRes = 0;
	std::map<unsigned, Edge*> tempEdges;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	// traverse all the vertices
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 3)
			break;
		ct++;
		curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tupleV1 = new TableTuple(curVertex->getTupleData(), input_table->schema());
		if (!postfilter.eval(tupleV1, NULL)) 
			continue;

		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();

		
		for (unsigned edgeId: edgeIds) {
			fanOut++;
			Edge *edge = oldGraphView->getEdge(edgeId);
			from = edge->getStartVertexId();
			TableTuple* tupleV2;
			if (curVertexId == from)
				tupleV2 = new TableTuple(edge->getEndVertex()->getTupleData(), input_table->schema());
			else
				tupleV2 = new TableTuple(edge->getStartVertex()->getTupleData(), input_table->schema());
			if (postfilter.eval(tupleV2, NULL)) {
				tempEdges[edge->getId()] = edge;
			}
		}				
	}
	
	for(std::map<unsigned, Edge*>::iterator i=tempEdges.begin(); i!=tempEdges.end(); ++i){
		intermediateRes++;
		from = i->second->getStartVertexId();
		to = i->second->getEndVertexId();
		if ((!this->hasVertex(from)) && (!this->hasVertex(to))){
			createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
		}
	}
	cout << "post-filter-useV, fanOut = " << fanOut << ", intermediate result = " << intermediateRes << ", inputG_size = " << ct << endl;
}

void GraphView::selectVCThenBoundVerticesFromV(Table* input_vtable, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);

	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	std::map<unsigned, bool> visited;
	Vertex* curVertex;
	int fanOut = 0;
	int intermediateRes = 0;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
    for (std::map<unsigned, Vertex*>::iterator i=allVertices.begin(); i!=allVertices.end(); ++i) {
    	if (ct >= numV * lim / 3)
    		break;
    	ct++;
        unsigned name = i->first;
        curVertex = i->second;
        if (visited.count(name) > 0) {
            // this vertex is already added to the set
            continue;
        }

        std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
        for (unsigned edgeId: edgeIds) {
        	fanOut++;
			Edge *edge = oldGraphView->getEdge(edgeId);
            unsigned from = edge->getStartVertexId();
            unsigned to = edge->getEndVertexId();
            if ((visited.count(from) == 0) && (visited.count(to) == 0)) {
            	visited[from] = true;
            	visited[to] = true;
            	break;
			}
        }
    }
    unsigned vid;
    for (std::map<unsigned, bool>::iterator i=visited.begin(); i!=visited.end(); ++i) {
    	intermediateRes++;
    	vid = i->first;
    	TableTuple* tuple = new TableTuple(oldGraphView->getVertex(vid)->getTupleData(), input_vtable->schema());
    	if (postfilter.eval(tuple, NULL))
    		createAndAddVertex(vid, oldGraphView->getVertex(vid)->getTupleData());
    }
    cout << "pre-filter-useV, fanOut = " << fanOut << ", intermediate result = " << intermediateRes << ", inputG_size = " << ct << endl;
}

void GraphView::selectBoundVerticesFromEThenVC(Table* input_table, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);
	// predicate is about edges and we want to start from edges
	// get all the edges from the old graph
	Edge* curEdge;
	unsigned curEdgeId, from, to;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	std::map<unsigned, Edge*> tempEdges;
	int intermediateRes = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 3)
			break;
		ct++;

		curEdgeId = it->first;
		curEdge = it->second;
		Vertex* vFrom = curEdge->getStartVertex();
		Vertex* vTo = curEdge->getEndVertex();
		TableTuple* tuple1 = new TableTuple(vFrom->getTupleData(), input_table->schema());
		TableTuple* tuple2 = new TableTuple(vTo->getTupleData(), input_table->schema());

		//check if the tuple satisfy the predicate
		if (postfilter.eval(tuple1, NULL) && postfilter.eval(tuple2, NULL)) {
			tempEdges[curEdgeId] = curEdge;
		}
	}

	for(std::map<unsigned, Edge*>::iterator i=tempEdges.begin(); i!=tempEdges.end(); ++i){
		intermediateRes++;
		from = i->second->getStartVertexId();
		to = i->second->getEndVertexId();
		if ((!this->hasVertex(from)) && (!this->hasVertex(to))){
			createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
		}
	}
	cout << "post-filter-useE, intermediate result = " << intermediateRes << ", inputG_size = " << ct << endl;
}

void GraphView::selectVCThenBoundVerticesFromE(Table* input_vtable, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);

	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	std::map<unsigned, bool> visited;
	int intermediateRes = 0;
	Edge* e;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	for(std::map<unsigned, Edge*>::iterator i=allEdges.begin(); i!=allEdges.end(); ++i){
		if (ct >= numE * lim / 3)
			break;
		ct++;
		e = i->second;
		unsigned from = e->getStartVertexId();
        unsigned to = e->getEndVertexId();
        if ((visited.count(from) == 0) && (visited.count(to) == 0)) {
        	visited[from] = true;
        	visited[to] = true;
		}
	}
	unsigned vid;
    for (std::map<unsigned, bool>::iterator i=visited.begin(); i!=visited.end(); ++i) {
    	intermediateRes++;
    	vid = i->first;
    	TableTuple* tuple = new TableTuple(oldGraphView->getVertex(vid)->getTupleData(), input_vtable->schema());
    	if (postfilter.eval(tuple, NULL))
    		createAndAddVertex(vid, oldGraphView->getVertex(vid)->getTupleData());
    }
    cout << "pre-filter-useE, intermediate result = " << intermediateRes << ", inputG_size = " << ct << endl;
}

// void GraphView::preFilterWithPredFromV(Table* input_vtable, AbstractExpression* vpred, GraphView* oldGraphView)
// {
// 	// rewrite 2-approx. vertex cover to combine two steps into one
// 	int limit = CountingPostfilter::NO_LIMIT;
//     int offset = CountingPostfilter::NO_OFFSET;
//     CountingPostfilter postfilter(vpred, limit, offset);

// 	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
// 	Vertex* curVertex;
// 	std::map<unsigned, bool> visited;
// 	int fanOut = 0;
//     for (std::map<unsigned, Vertex*>::iterator i=allVertices.begin(); i!=allVertices.end(); ++i) {
//         unsigned name = i->first;
//         curVertex = i->second;
//         if (this->hasVertex(name)) {
//             continue;
//         }

//         std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
//         for (unsigned edgeId: edgeIds) {
//         	fanOut++;
// 			Edge *edge = oldGraphView->getEdge(edgeId);
//             unsigned from = edge->getStartVertexId();
//             unsigned to = edge->getEndVertexId();
//             bool fromAdded = (this->hasVertex(from)) || (visited.count(from) != 0);
//             bool toAdded = (this->hasVertex(to)) || (visited.count(to) != 0);
//             if (!fromAdded && !toAdded) {
//             	TableTuple* tuple1 = new TableTuple(oldGraphView->getVertex(from)->getTupleData(), input_vtable->schema());
//             	TableTuple* tuple2 = new TableTuple(oldGraphView->getVertex(to)->getTupleData(), input_vtable->schema());
//             	if (postfilter.eval(tuple1, NULL)) {
//             		createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
//             	}
//             	else {
//             		visited[from] = true;
//             	}

//             	if (postfilter.eval(tuple2, NULL)) {
//             		createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
//             	}
//             	else {
//             		visited[to] = true;
//             	}
//             	break;
// 			}
//         }
//     }
//     cout << "rewrite pre-filter-useV, fanOut = " << fanOut << endl;
// }

// void GraphView::preFilterWithPredFromE(Table* input_vtable, AbstractExpression* vpred, GraphView* oldGraphView)
// {
// 	int limit = CountingPostfilter::NO_LIMIT;
//     int offset = CountingPostfilter::NO_OFFSET;
//     CountingPostfilter postfilter(vpred, limit, offset);

// 	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
// 	std::map<unsigned, bool> visited;
// 	// int intermediateRes = 0;
// 	Edge* e;
// 	for(std::map<unsigned, Edge*>::iterator i=allEdges.begin(); i!=allEdges.end(); ++i){
// 		e = i->second;
// 		unsigned from = e->getStartVertexId();
//         unsigned to = e->getEndVertexId();
//         bool fromAdded = (this->hasVertex(from)) || (visited.count(from) != 0);
//         bool toAdded = (this->hasVertex(to)) || (visited.count(to) != 0);
//         if (!fromAdded && !toAdded) {
//         	TableTuple* tuple1 = new TableTuple(oldGraphView->getVertex(from)->getTupleData(), input_vtable->schema());
//         	TableTuple* tuple2 = new TableTuple(oldGraphView->getVertex(to)->getTupleData(), input_vtable->schema());
//         	if (postfilter.eval(tuple1, NULL)) {
//         		createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
//         	}
//         	else {
//         		visited[from] = true;
//         	}

//         	if (postfilter.eval(tuple2, NULL)) {
//         		createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
//         	}
//         	else {
//         		visited[to] = true;
//         	}
// 		}
// 	}
// 	// cout << "rewrite pre-filter-useE, fanOut = " << fanOut << endl;
// }

void GraphView::preFilterToPostFilterFromV(Table* input_table, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);

	Vertex* curVertex;
	Vertex* vTo;
	// unsigned curVertexId;
	unsigned from, to;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	std::map<unsigned, bool> visited;
	int fanOut = 0;
	int intermediateRes = 0;
	int ub = (oldGraphView->numOfEdges()) / 10;
	int lastdigit = 0;
	// std::map<unsigned, Edge*> tempEdges;
	bool tempEdges[ub];
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 3)
			break;
		ct++;
		// curVertexId = it->first;
		curVertex = it->second;
		TableTuple* tupleV1 = new TableTuple(curVertex->getTupleData(), input_table->schema());
		// if (visited.count(curVertexId) != 0)
		// 	continue;

		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
		
		for (unsigned edgeId: edgeIds) {
			fanOut++;
			Edge *edge = oldGraphView->getEdge(edgeId);
			vTo = edge->getEndVertex();
			TableTuple* tupleV2 = new TableTuple(vTo->getTupleData(), input_table->schema());
			if (postfilter.eval(tupleV1, NULL) || postfilter.eval(tupleV2, NULL)) {
				tempEdges[(edge->getId()) / 10] = true;
				lastdigit = (edge->getId()) % 10;
			}
			// if (postfilter.eval(tupleV1, NULL))
			// 	tempEdges[edge->getId()] = edge;
			// else {
			// 	visited[curVertexId] = true;
			// 	vTo = edge->getEndVertex();
			// 	TableTuple* tupleV2 = new TableTuple(vTo->getTupleData(), input_table->schema());
			// 	if (postfilter.eval(tupleV2, NULL) ) {
			// 		tempEdges[edge->getId()] = edge;
			// 	}
			// 	else {
			// 		visited[vTo->getId()] = true;
			// 	}
			// 	break;
			// }
				
		}				
	}
	
	for(int i = 0; i < ub; i++){
		intermediateRes++;
		if (!tempEdges[i]) 
			continue;
		Edge* edge = oldGraphView->getEdge(i * 10 + lastdigit);
		from = edge->getStartVertexId();
		to = edge->getEndVertexId();
		if ((!this->hasVertex(from)) && (!this->hasVertex(to))){
			TableTuple* tupleV1 = new TableTuple(oldGraphView->getVertex(from)->getTupleData(), input_table->schema());
			TableTuple* tupleV2 = new TableTuple(oldGraphView->getVertex(to)->getTupleData(), input_table->schema());
			if (postfilter.eval(tupleV1, NULL))
				createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			if (postfilter.eval(tupleV2, NULL))
				createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
		}
	}
	cout << "pre to post-filter-useV, fanOut = " << fanOut << ", intermediate result = " << intermediateRes << ", inputG_size = " << ct << endl;
}

void GraphView::preFilterToPostFilterFromV2(Table* input_table, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);

	Vertex* curVertex;
	Vertex* vTo;
	// unsigned curVertexId;
	unsigned from, to;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	Edge *edge;
	int ub = (oldGraphView->numOfEdges()) / 10;
	int lastdigit = 0;
	bool coveredEdges[ub];
	int fanOut = 0;
	int intermediateRes = 0;
	std::map<unsigned, Edge*> tempEdges;
	int ct = 0;
	int numV = oldGraphView->numOfVertexes();
	TableTuple* tupleV1;
	TableTuple* tupleV2;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 3)
			break;
		ct++;
		// curVertexId = it->first;
		curVertex = it->second;
		tupleV1 = new TableTuple(curVertex->getTupleData(), input_table->schema());
		if (!postfilter.eval(tupleV1, NULL))
			continue;

		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
		
		for (unsigned edgeId: edgeIds) {
			fanOut++;
			edge = oldGraphView->getEdge(edgeId);
			vTo = edge->getEndVertex();
			tupleV2 = new TableTuple(vTo->getTupleData(), input_table->schema());
			if (postfilter.eval(tupleV2, NULL)){
				tempEdges[edge->getId()] = edge;
				coveredEdges[(edge->getId()) / 10] = true;
				lastdigit = (edge->getId()) % 10;
			}	
		}				
	}
	
	for(std::map<unsigned, Edge*>::iterator i=tempEdges.begin(); i!=tempEdges.end(); ++i){
		intermediateRes++;
		from = i->second->getStartVertexId();
		to = i->second->getEndVertexId();
		if ((!this->hasVertex(from)) && (!this->hasVertex(to))){
			createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
		}
	}

	for (int i = 0; i < ub; i++) {
		if (!coveredEdges[i]) {
			edge = oldGraphView->getEdge(i * 10 + lastdigit);
			from = edge->getStartVertexId();
			to = edge->getEndVertexId();
			tupleV1 = new TableTuple(edge->getStartVertex()->getTupleData(), input_table->schema());
			tupleV2 = new TableTuple(edge->getEndVertex()->getTupleData(), input_table->schema());
			if (postfilter.eval(tupleV1, NULL))
				createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			else if (postfilter.eval(tupleV2, NULL))
				createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
		}
	}
	cout << "2pre to post-filter-useV, fanOut = " << fanOut << ", intermediate result = " << intermediateRes << ", inputG_size = " << ct << endl;
}

void GraphView::preFilterToPostFilterFromE(Table* input_table, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);

	Edge* curEdge;
	unsigned curEdgeId, from, to;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	std::map<unsigned, Edge*> tempEdges;
	std::map<unsigned, bool> visited;
	int intermediateRes = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 3)
			break;
		ct++;
		curEdgeId = it->first;
		curEdge = it->second;
		Vertex* vFrom = curEdge->getStartVertex();
		Vertex* vTo = curEdge->getEndVertex();
		TableTuple* tuple1 = new TableTuple(vFrom->getTupleData(), input_table->schema());
		TableTuple* tuple2 = new TableTuple(vTo->getTupleData(), input_table->schema());
		if ((postfilter.eval(tuple1, NULL)) || (!postfilter.eval(tuple2, NULL))) {
			tempEdges[curEdgeId] = curEdge;
		}
		// if ((visited.count(from) != 0) || (visited.count(to) != 0))
		// 	continue;

		// if ((!postfilter.eval(tuple1, NULL)) && (!postfilter.eval(tuple2, NULL))) {
		// 	visited[vFrom->getId()] = true;
		// 	visited[vTo->getId()] = true;
		// }
		// else {
		// 	tempEdges[curEdgeId] = curEdge;
		// }
	}

	for(std::map<unsigned, Edge*>::iterator i=tempEdges.begin(); i!=tempEdges.end(); ++i){
		intermediateRes++;
		from = i->second->getStartVertexId();
		to = i->second->getEndVertexId();
		if ((!this->hasVertex(from)) && (!this->hasVertex(to))){
			TableTuple* tupleV1 = new TableTuple(oldGraphView->getVertex(from)->getTupleData(), input_table->schema());
			TableTuple* tupleV2 = new TableTuple(oldGraphView->getVertex(to)->getTupleData(), input_table->schema());
			if (postfilter.eval(tupleV1, NULL))
				createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			if (postfilter.eval(tupleV2, NULL))
				createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
		}
	}
	cout << "pre to post-filter-useE, intermediate result = " << intermediateRes << ", inputG_size = " << ct << endl;
}

void GraphView::preFilterToPostFilterFromE2(Table* input_table, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);
	
	Edge* curEdge;
	unsigned curEdgeId, from, to;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	std::map<unsigned, Edge*> tempEdges;
	int ub = (oldGraphView->numOfEdges()) / 10 ;
	int lastdigit = 0;
	bool coveredEdges[ub];
	int intermediateRes = 0;
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	TableTuple* tupleV1;
	TableTuple* tupleV2;
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 3)
			break;
		ct++;

		curEdgeId = it->first;
		curEdge = it->second;
		Vertex* vFrom = curEdge->getStartVertex();
		Vertex* vTo = curEdge->getEndVertex();
		tupleV1 = new TableTuple(vFrom->getTupleData(), input_table->schema());
		tupleV2 = new TableTuple(vTo->getTupleData(), input_table->schema());

		//check if the tuple satisfy the predicate
		if (postfilter.eval(tupleV1, NULL) && postfilter.eval(tupleV2, NULL)) {
			tempEdges[curEdgeId] = curEdge;
			coveredEdges[(curEdge->getId()) / 10] = true;
			lastdigit = (curEdge->getId()) % 10;
		}
	}

	for(std::map<unsigned, Edge*>::iterator i=tempEdges.begin(); i!=tempEdges.end(); ++i){
		intermediateRes++;
		from = i->second->getStartVertexId();
		to = i->second->getEndVertexId();
		if ((!this->hasVertex(from)) && (!this->hasVertex(to))){
			createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
		}
	}
	
	for (int i = 0; i < ub; i++) {
		if (!coveredEdges[i]) {
			curEdge = oldGraphView->getEdge(i * 10 + lastdigit);
			from = curEdge->getStartVertexId();
			to = curEdge->getEndVertexId();
			tupleV1 = new TableTuple(curEdge->getStartVertex()->getTupleData(), input_table->schema());
			tupleV2 = new TableTuple(curEdge->getEndVertex()->getTupleData(), input_table->schema());
			if (postfilter.eval(tupleV1, NULL))
				createAndAddVertex(from, oldGraphView->getVertex(from)->getTupleData());
			else if (postfilter.eval(tupleV2, NULL))
				createAndAddVertex(to, oldGraphView->getVertex(to)->getTupleData());
		}
	}
	cout << "2pre to post-filter-useE, intermediate result = " << intermediateRes << ", inputG_size = " << ct << endl;
}

void GraphView::postFilterToPreFilterFromV(Table* input_table, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);
	
	Vertex* curVertex, *vTo;
	unsigned curVertexId;
	std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
	int ct = 0;
	int fanOut = 0;
	int numV = oldGraphView->numOfVertexes();
	TableTuple* tupleV1;
	TableTuple* tupleV2;
	Edge* edge;
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it )
	{
		if (ct >= numV * lim / 3)
			break;
		ct++;
		curVertexId = it->first;
		curVertex = it->second;
		tupleV1 = new TableTuple(curVertex->getTupleData(), input_table->schema());
		if ((!postfilter.eval(tupleV1, NULL)) || (this->hasVertex(curVertexId)))
			continue;

		std::vector<unsigned> edgeIds = curVertex->getAllOutEdges();
		
		for (unsigned edgeId: edgeIds) {
			fanOut++;
			edge = oldGraphView->getEdge(edgeId);
			vTo = edge->getEndVertex();
			tupleV2 = new TableTuple(vTo->getTupleData(), input_table->schema());
			if (!postfilter.eval(tupleV2, NULL)){
				continue;
			}	
			if ((!this->hasVertex(curVertexId)) && (!this->hasVertex(vTo->getId()))) {
				createAndAddVertex(curVertexId, curVertex->getTupleData());
				createAndAddVertex(vTo->getId(), vTo->getTupleData());
			}
		}				
	}
	cout << "post to prefilter useV, rewrite, fanOut = " << fanOut << ", inputG_size = " << ct << endl;
}

void GraphView::postFilterToPreFilterFromE(Table* input_table, AbstractExpression* vpred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(vpred, limit, offset);
	
	Edge* curEdge;
	// unsigned curEdgeId;
	std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
	int ct = 0;
	int numE = oldGraphView->numOfEdges();
	Vertex *vFrom, *vTo;
	TableTuple* tupleV1;
	TableTuple* tupleV2;
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it )
	{
		if (ct >= numE * lim / 3)
			break;
		ct++;
		// curEdgeId = it->first;
		curEdge = it->second;
		vFrom = curEdge->getStartVertex();
		vTo = curEdge->getEndVertex();
		tupleV1 = new TableTuple(vFrom->getTupleData(), input_table->schema());
		tupleV2 = new TableTuple(vTo->getTupleData(), input_table->schema());

		if (postfilter.eval(tupleV1, NULL) && postfilter.eval(tupleV2, NULL)) {
			if ((!this->hasVertex(vFrom->getId())) && (!this->hasVertex(vTo->getId()))) {
				createAndAddVertex(vFrom->getId(), vFrom->getTupleData());
				createAndAddVertex(vTo->getId(), vTo->getTupleData());
			}
		}			
	}
	cout << "post to prefilter useE, rewrite, inputG_size = " << ct << endl;
}

// triangle counting
std::vector<triple> GraphView::triangleCounting(std::map<unsigned, Edge*> allEdges, std::map<unsigned, Vertex*>allVertices, GraphView *oldGraphView)
{
	std::vector<triple> res;

	// From edges, construct a new map: (fromV, toV) -> E
	unsigned from, to;
	Edge* edge;
	endNodesPair t, t1, t2;
	std::map<endNodesPair, unsigned> vertex2edge;
	for (std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it ) {
		edge = it->second;
		from = edge->getStartVertexId();
		to = edge->getEndVertexId();
		t = make_pair(from, to);
		vertex2edge[t] = edge->getId();
	}

	// cout << "done building endNodesPair to edge mapping" << endl;

	// iterate over all the vertices
	Vertex* vertex;
	int vDeg, uDeg, wDeg;
	unsigned vid;
	int ct = 0;
	// int numV = allVertices.size();
	for (std::map<unsigned, Vertex*>::iterator it = allVertices.begin(); it != allVertices.end(); ++it ) {
		if (ct >= 2500) {
			cout << "reaches time limit, break now" << endl;
			break;
		}
		ct++;
		// check the degree of the current vertex
		vid = it->first;
		vertex = it->second;
		vDeg = vertex->fanOut(); // undirected graph fanin=fanout

		// check two out end nodes and their degree
		std::vector<unsigned> edgeIds = vertex->getAllOutEdges();
		// Vertex *u, *w;
		Edge *e1, *e2;
		unsigned uid, wid;
		for (int j = 0; j < vDeg; j++) {
			e1 = oldGraphView->getEdge(edgeIds[j]); 
			uid = e1->getStartVertexId()==vid ? e1->getEndVertexId() : e1->getStartVertexId();
			uDeg = oldGraphView->getVertex(uid)->fanOut();
			if (vDeg > uDeg || (vDeg==uDeg && vid > uid))
				continue;
			for (int k = j+1; k < vDeg; k++) {
				e2 = oldGraphView->getEdge(edgeIds[k]);
				wid = e2->getStartVertexId()==vertex->getId() ? e2->getEndVertexId() : e2->getStartVertexId();
				wDeg = oldGraphView->getVertex(wid)->fanOut();
				if (vDeg > wDeg || (vDeg==wDeg && vid > wid))
					continue;
				t1 = make_pair(uid, wid);
				t2 = make_pair(wid, uid);
				if (vertex2edge.count(t1) != 0) {
					triple triTup = make_tuple(e1->getId(), e2->getId(), vertex2edge[t1]);
					res.push_back(triTup);
				}
				if ( vertex2edge.count(t2) != 0) {
					triple triTup = make_tuple(e1->getId(), e2->getId(), vertex2edge[t2]);
					res.push_back(triTup);
				}
			}
		}
	}
	return res;
}

void GraphView::triangleCountingFirst(Table* input_table, AbstractExpression* pred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(pred, limit, offset);

    std::map<unsigned, Vertex*> allVertices = oldGraphView->getVertexMap();
    std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();
    std::vector<triple> triangles = triangleCounting(allEdges, allVertices, oldGraphView);
    
    unsigned eid1, eid2, eid3;
    Edge *e1, *e2, *e3;
    for (triple tri: triangles) {
    	eid1 = get<0>(tri);
    	eid2 = get<1>(tri);
    	eid3 = get<2>(tri);
    	e1 = oldGraphView->getEdge(eid1);
    	e2 = oldGraphView->getEdge(eid2);
    	e3 = oldGraphView->getEdge(eid3);
    	TableTuple* tuple1 = new TableTuple(e1->getTupleData(), input_table->schema());
    	if (!postfilter.eval(tuple1, NULL))
    		continue;
    	TableTuple* tuple2 = new TableTuple(e2->getTupleData(), input_table->schema());
    	if (!postfilter.eval(tuple2, NULL))
    		continue;
    	TableTuple* tuple3 = new TableTuple(e3->getTupleData(), input_table->schema());
    	if (!postfilter.eval(tuple3, NULL))
    		continue;
    	addTriangleToGraph(e1, e2, e3);
    }
}

void GraphView::triangleCountingLast(Table* input_table, AbstractExpression* pred, GraphView* oldGraphView, int lim)
{
	int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    CountingPostfilter postfilter(pred, limit, offset);

    std::map<unsigned, Edge*> chosenEdges;
    std::map<unsigned, Vertex*> chosenVertices;
    std::map<unsigned, Edge*> allEdges = oldGraphView->getEdgeMap();

    Edge *edge;
    for(std::map<unsigned, Edge*>::iterator it = allEdges.begin(); it != allEdges.end(); ++it ) {
    	edge = it->second;
    	TableTuple* tuple = new TableTuple(edge->getTupleData(), input_table->schema());
    	if (!postfilter.eval(tuple, NULL))
    		continue;
    	chosenEdges[edge->getId()] = edge;
    	chosenVertices[edge->getStartVertexId()] = edge->getStartVertex();
    	chosenVertices[edge->getEndVertexId()] = edge->getEndVertex();
    }

    unsigned eid1, eid2, eid3;
    Edge *e1, *e2, *e3;
    std::vector<triple> triangles = triangleCounting(chosenEdges, chosenVertices, oldGraphView);
    for (triple tri: triangles) {
    	eid1 = get<0>(tri);
    	eid2 = get<1>(tri);
    	eid3 = get<2>(tri);
    	e1 = oldGraphView->getEdge(eid1);
    	e2 = oldGraphView->getEdge(eid2);
    	e3 = oldGraphView->getEdge(eid3);
    	addTriangleToGraph(e1, e2, e3);
    }
}

void GraphView::addTriangleToGraph(Edge *e1, Edge *e2, Edge *e3) 
{
	createAndAddVertex(e1->getStartVertexId(), e1->getStartVertex()->getTupleData());
	createAndAddVertex(e1->getEndVertexId(), e1->getEndVertex()->getTupleData());
	createAndAddVertex(e2->getStartVertexId(), e2->getStartVertex()->getTupleData());
	createAndAddVertex(e2->getEndVertexId(), e2->getEndVertex()->getTupleData());

	Edge *newEdge;
	Vertex *vFrom, *vTo;
	unsigned eid1 = e1->getId();
	unsigned eid2 = e2->getId();
	unsigned eid3 = e3->getId();
	// by default both old and new graphs are undirected
	newEdge = createEdge(eid1, e1->getTupleData(), e1->getStartVertexId(), e1->getEndVertexId());
	vFrom = newEdge->getStartVertex();
	vTo = newEdge->getEndVertex();
	vFrom->addOutEdge(newEdge);
	vTo->addInEdge(newEdge);
	vTo->addOutEdge(newEdge);
	vFrom->addInEdge(newEdge);
	this->addEdge(eid1, newEdge);

	newEdge = createEdge(eid2, e2->getTupleData(), e2->getStartVertexId(), e2->getEndVertexId());
	vFrom = newEdge->getStartVertex();
	vTo = newEdge->getEndVertex();
	vFrom->addOutEdge(newEdge);
	vTo->addInEdge(newEdge);
	vTo->addOutEdge(newEdge);
	vFrom->addInEdge(newEdge);
	this->addEdge(eid2, newEdge);

	newEdge = createEdge(eid3, e3->getTupleData(), e3->getStartVertexId(), e3->getEndVertexId());
	vFrom = newEdge->getStartVertex();
	vTo = newEdge->getEndVertex();
	vFrom->addOutEdge(newEdge);
	vTo->addInEdge(newEdge);
	vTo->addOutEdge(newEdge);
	vFrom->addInEdge(newEdge);
	this->addEdge(eid3, newEdge);
}

void GraphView::fillSubGraphFromRelationalTables(string filterHint, bool postfilter, const string& subGraphVPredicate, const string& subGraphEPredicate, int inputGraphSize, std::string joinVEPredicate, GraphView* oldGraphView, std::string vlabelName, std::string elabelName, bool useV)
{
	// stringstream output;
	// output << graphPredicate << ", " << joinVEPredicate << ", " << useV << endl;
	// LogManager::GLog("GraphView", "full subgraph", 2016, output.str());
	this->m_vertexes.clear();
	this->m_edges.clear();

	// checkVertexAndEdgeConstructTime(oldGraphView, hasLimit);

	// LX FEAT4
	// start from the oldGraphView, check vertex by vertex, or edge by edge
	// filter by the subgraphPredicate

	// first check the predicate is about vertex or edge
    std::string curLabel = "";
    Table* input_vtable = NULL;
    Table* input_etable = NULL;
    // bool isV = false;
    // bool isV2 = false;
    // bool isE = false;
    AbstractExpression* vpred = NULL;
    // AbstractExpression* vpred2 = NULL;
    AbstractExpression* epred = NULL;
    if (subGraphVPredicate.size() != 0) {
    	curLabel = vlabelName;
    	input_vtable = oldGraphView->getVertexTableFromLabel(curLabel);
    	// isV = true;
    	vpred = getPredicateFromWhere(subGraphVPredicate);
    }
    // if (subGraphVPredicate2.size() != 0) {
    	// then the previous predicate cannot be null be default
    	// isV2 = true;
    // 	vpred2 = getPredicateFromWhere(subGraphVPredicate2);
    // }
    if (subGraphEPredicate.size() != 0) {
    	curLabel = elabelName;
    	input_etable = oldGraphView->getEdgeTableFromLabel(curLabel);
    	// isE = true;
    	epred = getPredicateFromWhere(subGraphEPredicate);
    }		

    
	int limit = inputGraphSize / 10;
	int thisquery = inputGraphSize % 10;
	if (limit == 0)
		limit = 10;

    // add query rewrite for hint()
    if (filterHint != "") {
    	// vertexcover returns the vertex set 
    	if (filterHint.compare("VERTEXCOVER") == 0) {
    		// first vertex cover then select subgraph
    		if (postfilter) {
    			if (useV) {
    				if (thisquery == 1) {
    					LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: post-filter-useV");
	    				selectBoundVerticesFromVThenVC(input_vtable, vpred, oldGraphView, limit);
    				}
	    			if (thisquery == 2) {
	    				LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: rewrite post-filter-useV");
	    				postFilterToPreFilterFromV(input_vtable, vpred, oldGraphView, limit);
	    			}
    			}
    			else {
    				if (thisquery == 1) {
    					LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: post-filter-useE");
	    				selectBoundVerticesFromEThenVC(input_vtable, vpred, oldGraphView, limit);
    				}
	    			if (thisquery == 2) {
	    				LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: rewrite post-filter-useE");
	    				postFilterToPreFilterFromE(input_vtable, vpred, oldGraphView, limit);
	    			}
    			}
    		}
    		else {
    			if (useV) {
    				if (thisquery == 1) {
    					LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: pre-filter-useV");
	    				selectVCThenBoundVerticesFromV(input_vtable, vpred, oldGraphView, limit);
    				}
	    			if (thisquery == 2) {
    					LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: pre-filter-useV rewrite");
	    				preFilterToPostFilterFromV(input_vtable, vpred, oldGraphView, limit);
    				}
    			}
    			else {
    				if (thisquery == 1) {
    					LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: pre-filter-useE");
	    				selectVCThenBoundVerticesFromE(input_vtable, vpred, oldGraphView, limit);
    				}
	    			if (thisquery == 2) {
    					LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: pre-filter-useE rewrite");
	    				preFilterToPostFilterFromE(input_vtable, vpred, oldGraphView, limit);
    				}	
    			}
    		}
    	}
    	
    	if (filterHint.compare("TRIANGLE") == 0) {
    		if (useV) {
    			LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: triangleCounting first");
    			triangleCountingFirst(input_etable, epred, oldGraphView, limit);
    		}
    		else {
    			LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: triangleCounting last");
    			triangleCountingLast(input_etable, epred, oldGraphView, limit);
    		}
    	}
    }
    else {
	    if (useV) {
	    	if (thisquery == 1) {
	    		LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView:fillSubGraph:select bound vertices from v");
		    	selectOnlyBoundVerticesFromVertex(input_vtable, vpred, oldGraphView, limit);
	    	}
		    if (thisquery == 2) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView:fillSubGraph:select free vertices from v");
		    	selectOnlyFreeVerticesFromVertex(input_etable, epred, oldGraphView, limit);
		    }	
		   	if (thisquery == 3) {
		   		LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: select bound and free vertices from v");
		    	selectFreeBoundVerticesFromVertex(input_vtable, input_etable, vpred, epred, oldGraphView, limit);
		   	}
		    if (thisquery == 4) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: select bound intersects free vertices from v");
		    	selectFreeIntersectBoundVerticesFromVertex(input_vtable, input_etable, vpred, epred, oldGraphView, limit);
		    }
		    if (thisquery == 5) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: selct bound edges from v ");
		    	selectBoundEdgesFromVertex(input_etable, epred, oldGraphView, limit);
		    }
		    if (thisquery == 6) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: select free edges from v");
		    	selectFreeEdgesFromVertex(input_vtable, vpred, oldGraphView, limit);
		    }
		    if (thisquery == 7) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: free union bound edges from v");
		    	selectFreeBoundEdgesFromVertex(input_vtable, input_etable, vpred, epred, oldGraphView, limit);
		    }
		    if (thisquery == 8) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: free intersects bound edges from v");
		    	selectFreeIntersectBoundEdgesFromVertex(input_vtable, input_etable, vpred, epred, oldGraphView, limit);
		    }
	    }
	    else {
	    	if (thisquery == 1) {
	    		LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView:fillSubGraph:select bound vertices from e");

		    	selectOnlyBoundVerticesFromEdge(input_vtable, vpred, oldGraphView, limit);
	    	}
		    if (thisquery == 2) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView:fillSubGraph:select free vertices from e");
		    	selectOnlyFreeVerticesFromEdge(input_etable, epred, oldGraphView, limit);
		    }	
		   	if (thisquery == 3) {
		   		LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: select bound and free vertices from e");
		    	selectFreeBoundVerticesFromEdge(input_vtable, input_etable, vpred, epred, oldGraphView, limit);
		   	}
		    if (thisquery == 4) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: select bound intersects free vertices from e");
		    	selectFreeIntersectBoundVerticesFromEdge(input_vtable, input_etable, vpred, epred, oldGraphView, limit);
		    }
		    if (thisquery == 5) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: selct bound edges from e ");
		    	selectBoundEdgesFromEdge(input_etable, epred, oldGraphView, limit);
		    }
		    if (thisquery == 6) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR,  "GraphView: select free edges from e");
		    	selectFreeEdgesFromEdge(input_vtable, vpred, oldGraphView, limit);
		    }
		    if (thisquery == 7) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: free union bound edges from e");
		    	selectFreeBoundEdgesFromEdge(input_vtable, input_etable, vpred, epred, oldGraphView, limit);
		    }
		    if (thisquery == 8) {
		    	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, "GraphView: free intersects bound edges from e");
	    		selectFreeIntersectBoundEdgesFromEdge(input_vtable, input_etable, vpred, epred, oldGraphView, limit);
		    }
	    }
    }
	    
	char msg[512];
    snprintf(msg, sizeof(msg), "Subgraph with %d vertex %d edges", this->numOfVertexes(), this->numOfEdges());
    msg[sizeof msg - 1] = '\0';
	LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, msg);
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
