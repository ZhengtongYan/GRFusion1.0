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

class GraphView
{
	friend class PathIterator;
	friend class TableIterator;
	friend class GraphViewFactory;
	friend class Vertex;
	friend class Edge;

public:
	//PQEntryWithLength.first is the cost, PQEntryWithLength.second.first is the vertexId, PQEntryWithLength.second.second is the path length
	typedef pair<double, pair<string, int> > PQEntryWithLength; // LX FEAT2
	typedef pair<int, string> PQEntry; // LX FEAT2
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


	float shortestPath(int source, int destination, int costColumnId);
	// Vertex* getVertex(int id);
	Vertex* getVertex(string id); // LX FEAT2
	// TableTuple* getVertexTuple(int id);
	TableTuple* getVertexTuple(string id); // LX FEAT2
	// Edge* getEdge(int id);
	Edge* getEdge(string id); // LX FEAT2
	// TableTuple* getEdgeTuple(int id);
	TableTuple* getEdgeTuple(string id);  // LX FEAT2
	TableTuple* getEdgeTuple(char* data);
	// void addVertex(int id, Vertex* vertex);
	// void addEdge(int id, Edge* edge);
	void addVertex(string id, Vertex* vertex); // LX FEAT2
	void addEdge(string id, Edge* edge); // LX FEAT2
	int numOfVertexes();
	int numOfEdges();
	string name();
	string debug();
	bool isDirected();
	// Table* getVertexTable(); // TODO: remove after fixing vertexscan
	Table* getVertexTableFromLabel(string vlabel); // LX FEAT2
	Table* getVertexTableById(string id); // LX FEAT2
	Table* getEdgeTableFromLabel(string elabel); // LX FEAT3
	Table* getEdgeTableById(string id); // LX FEAT3

	void addToSubgraphList(string graphname); // LX FEAT4
	void addToSubgraphVertex(std::vector<Vertex*> subgraphVertex); // LX FEAT4
	void addToSubgraphEdge(std::vector<Edge*> subgraphEdge); // LX FEAT4
	
	// Table* getEdgeTable();
	Table* getPathTable();
	Table* getGraphTable(); // LX FEAT4
	TupleSchema* getVertexSchema();
	TupleSchema* getEdgeSchema();
	TupleSchema* getPathSchema();
	TupleSchema* getGraphSchema(); // LX FEAT4
	string getPathsTableName() {return m_pathTableName; }
	string getGraphTableName() {return m_graphTableName; }// LX FEAT4
	void setVertexSchema(TupleSchema* s);
	void setEdgeSchema(TupleSchema* s);
	void setPathSchema(TupleSchema* s);
	void setGraphSchema(TupleSchema* s); // LX FEAT4

	int getVertexIdColumnIndex();
	int getEdgeIdColumnIndex();
	int getEdgeFromColumnIndex();
	int getEdgeToColumnIndex();
	int getColumnIdInVertexTable(int vertexAttributeId);
	int getColumnIdInEdgeTable(int edgeAttributeId);
	string getVertexAttributeName(int vertexAttributeId);
	string getEdgeAttributeName(int edgeAttributeId);

	//path related members
	//Notice that VoltDB allows one operation or query / one thread per time
	//Hence, we assume that a single path traversal query is active at any point in time
	PathIterator* iteratorDeletingAsWeGo(GraphOperationType opType); // LX
	PathIterator* iteratorDeletingAsWeGo(); // LX

	void expandCurrentPathOperation();

	//Queries
	// void BFS_Reachability_ByDepth(int startVertexId, int depth);
	// void BFS_Reachability_ByDestination(int startVertexId, int endVertex);
	// void BFS_Reachability_ByDepth_eSelectivity(int startVertexId, int depth, int eSelectivity);
	// void SP_TopK(int src, int dest, int k);
	// void SP_EdgeSelectivity(int src, int dest, int edgeSelectivity);
	// void SP_ToAllVertexes_EdgeSelectivity(int src, int edgeSelectivity);
	void BFS_Reachability_ByDepth(string startVertexId, int depth); // LX FEAT2
	void BFS_Reachability_ByDestination(string startVertexId, string endVertex); // LX FEAT2
	void BFS_Reachability_ByDepth_eSelectivity(string startVertexId, int depth, int eSelectivity); // LX FEAT2
	void SP_TopK(string src, string dest, int k); // LX FEAT2
	void SP_EdgeSelectivity(string src, string dest, int edgeSelectivity); // LX FEAT2
	void SP_ToAllVertexes_EdgeSelectivity(string src, int edgeSelectivity); // LX FEAT2
	string fromVertexId, toVertexId; // LX FEAT2
	int queryType, pathLength, topK, vSelectivity, eSelectivity, spColumnIndexInEdgesTable;

	//Topology query, i.e., connected sub-graph of

	//to select all vertexes, set vSelectivty to 100, same for the edges
	void SubGraphLoop(int length, int vSelectivity, int eSelectivity);
	// void SubGraphLoopFromStartVertex(int startVertexId, int length, int vSelectivity, int eSelectivity); //14
	// void SubGraphLoop(int startVertexId, int length); //startVertexId of -1 means to try all the vertexes as the start of the loop
	void SubGraphLoopFromStartVertex(string startVertexId, int length, int vSelectivity, int eSelectivity); // LX FEAT2
	void SubGraphLoop(string startVertexId, int length); // LX FEAT2


protected:
	void fillGraphFromRelationalTables();
	void constructPathSchema(); //constucts m_pathColumnNames and m_pathSchema
	void constructPathTempTable();
	void constructGraphSchema(); // LX FEAT4
	void constructGraphTempTable(); // LX FEAT4

	std::vector<std::vector<Vertex*>> m_subgraphVertexList; // LX FEAT4
	std::vector<std::vector<Edge*>> m_subgraphEdgeList; // LX FEAT4
	std::vector<string> m_subgraphList; // LX FEAT4

	// std::map<int, Vertex* > m_vertexes;
	// std::map<int, Edge* > m_edges;
	std::map<string, Vertex* > m_vertexes; // LX FEAT2
	std::map<string, Edge* > m_edges; // LX FEAT2
	// Table* m_vertexTable;
	std::vector<Table*> m_vertexTables; // LX FEAT2
	std::vector<string> m_vertexLabels; // LX FEAT2
	std::map<string, Table*> m_idToVTableMap; // LX FEAT2
	// Table* m_edgeTable;
	std::vector<Table*> m_edgeTables; // LX FEAT3
	std::vector<string> m_edgeLabels; // LX FEAT3
	std::vector<string> m_startVLabels; // LX FEAT3
	std::vector<string> m_endVLabels; // LX FEAT3
	std::map<string, Table*> m_idToETableMap; // LX FEAT3

	TempTable* m_pathTable;
	TableIterator* m_pathTableIterator;
	PathIterator* m_pathIterator;
	TempTable* m_graphTable;
	TableIterator* m_graphTableIterator;
	PathIterator* m_graphIterator;
	TupleSchema* m_vertexSchema; //will contain fanIn and fanOut as additional attributes
	TupleSchema* m_edgeSchema; //will contain startVertexId and endVertexId as additional attributes
	TupleSchema* m_pathSchema; //will contain startVertexId, endVertexId, and cost for now
	TupleSchema* m_graphSchema; // LX FEAT4
	// schema as array of string names
	std::vector<string> m_vertexColumnNames;
	std::vector<string> m_edgeColumnNames;
	std::vector<string> m_pathColumnNames;
	std::vector<string> m_graphColumnNames; // LX FEAT4
	std::vector<int> m_columnIDsInVertexTable;
	std::vector<int> m_columnIDsInEdgeTable;
	int m_vertexIdColumnIndex;
	std::map<string, int> m_vertexIdColIdxList;// LX FEAT2: store the id col index for each label
	

	int m_edgeIdColumnIndex;
	int m_edgeFromColumnIndex;
	int m_edgeToColumnIndex;
	std::map<string, int> m_edgeIdColIdxList;// LX FEAT3
	std::map<string, int> m_edgeFromColIdxList;// LX FEAT3
	std::map<string, int> m_edgeToColIdxList;// LX FEAT3

	int m_vPropColumnIndex, m_ePropColumnIndex;
	string m_pathTableName = "PATHS_TEMP_TABLE";
	string m_graphTableName = "SELECT_GRAPH_TEMP_TABLE"; // LX FEAT4
	GraphOperationType currentPathOperationType;
	//TODO: this should be removed
	int dummyPathExapansionState = 0;
	//bool traverseBFS = false;
	bool executeTraversal = false;
	// identity information
	CatalogId m_databaseId;
	string m_name;
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

	bool m_isDirected;

	GraphView(void);

private:
    int32_t m_refcount;
    ThreadLocalPool m_tlPool;
    int m_compactionThreshold;
};

}

#endif
