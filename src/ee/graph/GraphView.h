#ifndef GRAPHVIEW_H
#define GRAPHVIEW_H

#include <map>
#include <string>
#include <ctime>
#include <sys/time.h>
//#include <chrono>
#include "storage/table.h"
#include "storage/temptable.h"
#include "graph/GraphTypes.h"

#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/ValueFactory.hpp"

using namespace std;

namespace voltdb {

//#include "vertex.h"
class Vertex;
class Edge;
class PathIterator;
class CountingPostfilter; // LX FEAT4

class GraphView
{
	friend class PathIterator;
	friend class TableIterator;
	friend class GraphViewFactory;
	friend class Vertex;
	friend class Edge;

public:
	//PQEntryWithLength.first is the cost, PQEntryWithLength.second.first is the vertexId, PQEntryWithLength.second.second is the path length
	typedef pair<double, pair<unsigned, int> > PQEntryWithLength; // LX FEAT2
	typedef pair<int, unsigned> PQEntry; // LX FEAT2
	~GraphView(void);

	/*
	 * Table lifespan can be managed by a reference count. The
	 * reference is trivial to maintain since it is only accessed by
	 * the execution engine thread. Snapshot, Export and the
	 * corresponding CatalogDelegate may be reference count
	 * holders. The table is deleted when the refcount falls to
	 * zero. This allows longer running processes to complete
	 * gracefully after a table has been removed from the catalog.
	 */
	void incrementRefcount() {
		m_refcount += 1;
	}

	void decrementRefcount() {
		m_refcount -= 1;
		if (m_refcount == 0) {
			delete this;
		}
	}

	string name();
	string debug();
	bool isDirected();

	// get all vertex related
	int numOfVertexes();
	Vertex* getVertex(unsigned id); // LX FEAT2
	bool hasVertex(unsigned id);
	TableTuple* getVertexTuple(unsigned id); // LX FEAT2
	Table* getVertexTableFromLabel(string vlabel); // LX FEAT2
	Table* getVertexTableById(unsigned id); // LX FEAT2
	std::vector<Table*> getVertexTables();
	std::vector<std::string> getVertexLabels();
	Table* getVertexTableByIndex(int idx); 
	std::string getVertexLabelByIndex(int idx); 
	TupleSchema* getVertexSchema();
	int getVertexIdColumnIndex();
	int getColumnIdInVertexTable(int vertexAttributeId);
	string getVertexAttributeName(int vertexAttributeId);
	std::map<unsigned, Vertex*> getVertexMap(); // LX FEAT7
	int getIndexFromVertexLabels(string label);

	// get all edge related
	int numOfEdges();
	Edge* getEdge(unsigned id); // LX FEAT2
	bool hasEdge(unsigned id);
	TableTuple* getEdgeTuple(unsigned id);  // LX FEAT2
	TableTuple* getEdgeTuple(char* data);
	Table* getEdgeTableFromLabel(string elabel); // LX FEAT3
	Table* getEdgeTableById(unsigned id); // LX FEAT3
	std::vector<Table*> getEdgeTables();
	std::vector<std::string> getEdgeLabels();
	std::vector<std::string> getStartVertexLabels();
	std::vector<std::string> getEndVertexLabels();
	Table* getEdgeTableByIndex(int idx);
	std::string getEdgeLabelByIndex(int idx) ;
	TupleSchema* getEdgeSchema();
	int getEdgeIdColumnIndex();
	int getEdgeFromColumnIndex();
	int getEdgeToColumnIndex();
	int getColumnIdInEdgeTable(int edgeAttributeId);
	string getEdgeAttributeName(int edgeAttributeId);
	std::map<unsigned, Edge*> getEdgeMap(); // LX FEAT7
	int getIndexFromEdgeLabels(string label);

	// get path related
	Table* getPathTable();
	Table* getGraphTable(); // LX FEAT4
	TupleSchema* getPathSchema();
	TupleSchema* getGraphSchema(); // LX FEAT4
	string getPathsTableName() {return m_pathTableName; }
	string getGraphTableName() {return m_graphTableName; }// LX FEAT4
	//path related members
	//Notice that VoltDB allows one operation or query / one thread per time
	//Hence, we assume that a single path traversal query is active at any point in time
	PathIterator* iteratorDeletingAsWeGo(GraphOperationType opType); // LX
	PathIterator* iteratorDeletingAsWeGo(); // LX
	void expandCurrentPathOperation();
	void processTupleInsertInGraphView(TableTuple& target, std::string tableName); // LX FEAT6

	// set vertex
	void addVertex(unsigned id, Vertex* vertex); // LX FEAT2
	void setVertexSchema(TupleSchema* s);

	// set edge
	void addEdge(unsigned id, Edge* edge); // LX FEAT2
	void setEdgeSchema(TupleSchema* s);

	// set path
	void setPathSchema(TupleSchema* s);
	void setGraphSchema(TupleSchema* s); // LX FEAT4

	// traverse graph
	float shortestPath(int source, int destination, int costColumnId);
	//Queries
	void BFS_Reachability_ByDepth(unsigned startVertexId, int depth);
	void BFS_Reachability_ByDestination(unsigned startVertexId, unsigned endVertex);
	void BFS_Reachability_ByDepth_eSelectivity(unsigned startVertexId, int depth, int eSelectivity);
	void SP_TopK(unsigned src, unsigned dest, int k);
	void SP_EdgeSelectivity(unsigned src, unsigned dest, int edgeSelectivity);
	void SP_ToAllVertexes_EdgeSelectivity(unsigned src, int edgeSelectivity);
	unsigned fromVertexId, toVertexId; // LX FEAT2
	int queryType, pathLength, topK, vSelectivity, eSelectivity, spColumnIndexInEdgesTable;
	//Topology query, i.e., connected sub-graph of
	//to select all vertexes, set vSelectivty to 100, same for the edges
	void SubGraphLoop(int length, int vSelectivity, int eSelectivity);
	// void SubGraphLoopFromStartVertex(int startVertexId, int length, int vSelectivity, int eSelectivity); //14
	// void SubGraphLoop(int startVertexId, int length); //startVertexId of -1 means to try all the vertexes as the start of the loop
	void SubGraphLoopFromStartVertex(unsigned startVertexId, int length, int vSelectivity, int eSelectivity); // LX FEAT2
	void SubGraphLoop(unsigned startVertexId, int length); // LX FEAT2

protected:
	void fillGraphFromRelationalTables();
	void fillSubGraphFromRelationalTables(const string& subGraphVPredicate, const string& subGraphEPredicate, GraphView* oldGraphName, std::string vlabelName, std::string elabelName, bool isV);
	void constructPathSchema(); //constucts m_pathColumnNames and m_pathSchema
	void constructPathTempTable();
	void constructGraphSchema(); // LX FEAT4
	void constructGraphTempTable(); // LX FEAT4

	// vertex
	std::vector<std::vector<Vertex*>> m_subgraphVertexList; // LX FEAT4
	std::map<unsigned, Vertex* > m_vertexes; // LX FEAT2
	std::vector<Table*> m_vertexTables; // LX FEAT2
	std::vector<string> m_vertexLabels; // LX FEAT2
	std::map<unsigned, Table*> m_idToVTableMap; // LX FEAT2
	TupleSchema* m_vertexSchema; //will contain fanIn and fanOut as additional attributes
	std::vector<string> m_vertexColumnNames;
	std::vector<int> m_columnIDsInVertexTable;
	int m_vertexIdColumnIndex;
	std::map<std::string, int> m_vertexIdColIdxList;// LX FEAT2: store the id col index for each label

	// edge
	std::vector<std::vector<Edge*>> m_subgraphEdgeList; // LX FEAT4
	std::map<unsigned, Edge* > m_edges; // LX FEAT2
	std::vector<Table*> m_edgeTables; // LX FEAT3
	std::vector<string> m_edgeLabels; // LX FEAT3
	std::vector<string> m_startVLabels; // LX FEAT3
	std::vector<string> m_endVLabels; // LX FEAT3
	std::map<unsigned, Table*> m_idToETableMap; // LX FEAT3
	TupleSchema* m_edgeSchema; //will contain startVertexId and endVertexId as additional attributes
	std::vector<string> m_edgeColumnNames;
	std::vector<int> m_columnIDsInEdgeTable;
	int m_edgeIdColumnIndex;
	int m_edgeFromColumnIndex;
	int m_edgeToColumnIndex;
	std::map<std::string, int> m_edgeIdColIdxList;// LX FEAT3
	std::map<std::string, int> m_edgeFromColIdxList;// LX FEAT3
	std::map<std::string, int> m_edgeToColIdxList;// LX FEAT3

	// path
	std::vector<string> m_subgraphList; // LX FEAT4
	TempTable* m_pathTable;
	TableIterator* m_pathTableIterator;
	PathIterator* m_pathIterator;
	TempTable* m_graphTable;
	TableIterator* m_graphTableIterator;
	PathIterator* m_graphIterator;
	TupleSchema* m_pathSchema; //will contain startVertexId, endVertexId, and cost for now
	TupleSchema* m_graphSchema; // LX FEAT4
	std::vector<string> m_pathColumnNames;
	std::vector<string> m_graphColumnNames; // LX FEAT4
	int m_vPropColumnIndex, m_ePropColumnIndex;
	string m_pathTableName = "PATHS_TEMP_TABLE";
	string m_graphTableName = "SELECT_GRAPH_TEMP_TABLE"; // LX FEAT4
	GraphOperationType currentPathOperationType;
	//TODO: this should be removed
	int dummyPathExapansionState = 0;

	// graph traversal
	//bool traverseBFS = false;
	bool executeTraversal = false;

	// identity information
	CatalogId m_databaseId;
	string m_name;
	bool m_isDirected;
	//SHA-1 of signature string
	char m_signature[20];
	//Mohamed: I think all the below ma not be needed as we will just reference the underlying tables

	/*TableTuple m_tempVertexTuple;
	TableTuple m_tempEdgeTuple;
	boost::scoped_array<char> m_tempVertexTupleMemory;
	boost::scoped_array<char> m_tempEdgeTupleMemory;

	TupleSchema* m_vertexSchema;
	TupleSchema* m_edgeSchema;

	// schema as array of string names
	std::vector<string> m_vertexColumnNames;
	std::vector<string> m_edgeColumnNames;
	char *m_vertexColumnHeaderData;
	char *m_edgeColumnHeaderData;
	int32_t m_vertexColumnHeaderSize;
	int32_t m_edgeColumnHeaderSize;
	*/

	

	GraphView(void);

private:
    int32_t m_refcount;
    ThreadLocalPool m_tlPool;
    int m_compactionThreshold;
};

}

#endif
