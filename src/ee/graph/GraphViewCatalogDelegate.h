/*
 * GraphViewCatalogDelegate.h
 *
 *  Created on: Nov 23, 2016
 *      Author: msaberab
 */

#ifndef SRC_EE_GRAPH_GRAPHVIEWCATALOGDELEGATE_H_
#define SRC_EE_GRAPH_GRAPHVIEWCATALOGDELEGATE_H_

#include "catalog/graphview.h"
#include "catalog/table.h"
#include "common/tabletuple.h"

#include <set>

namespace catalog {
class Database;
}

namespace voltdb {
class Table;
class Pool;
class ExecutorContext;
class TupleSchema;
class GraphView;

class GraphViewCatalogDelegate {
public:
	GraphViewCatalogDelegate(const std::string& signature, int32_t compactionThreshold)
	        : m_graphView(NULL)
	        , m_signature(signature)
	        , m_compactionThreshold(compactionThreshold)
	    {}

	    ~GraphViewCatalogDelegate();

	    void deleteCommand();

	    // void init(catalog::Database const &catalogDatabase, catalog::GraphView const &catalogGraphView, Table* vTable, Table* eTable, Table* pTable); 
	    void init(catalog::Database const &catalogDatabase, catalog::GraphView const &catalogGraphView, vector<std::string> vLabels, vector<Table*> vTables, vector<std::string> eLabels, vector<Table*> eTables, vector<std::string> startVLabels, vector<std::string> endVLabels, Table* pTable); // LX FEAT2
	    void initSubgraph(catalog::Database const &catalogDatabase, catalog::GraphView const &catalogGraphView, vector<std::string> vLabels, vector<Table*> vTables, vector<std::string> eLabels, vector<Table*> eTables, vector<std::string> startVLabels, vector<std::string> endVLabels, Table* pTable, const string& subGraphVPredicate, const string& subGraphEPredicate, GraphView* oldGraphName, std::string vlabelName, std::string elabelName, bool isV); // LX FEAT4





	    void processGraphViewChange(catalog::Database const &catalogDatabase, catalog::GraphView const &catalogGraphView, vector<std::string> vLabels, vector<Table*> vTables, vector<std::string> eLabels, vector<Table*> eTables, vector<std::string> startVLabels, vector<std::string> endVLabels, Table* pTable);

	    void processSchemaChanges(catalog::Database const &catalogDatabase,
	                             catalog::GraphView const &catalogGraphView,
	                             std::map<std::string, GraphViewCatalogDelegate*> const &graphViewsByName);

	    static TupleSchema *createOutputVertexTupleSchema(catalog::Database const &catalogDatabase,
	                                          catalog::GraphView const &catalogGraphView);

	    static TupleSchema *createOutputEdgeTupleSchema(catalog::Database const &catalogDatabase,
	    									  catalog::GraphView const &catalogGraphView);


	    /**
	     * Sets each field in the tuple to the default value for the
	     * table.  Schema is assumed to be the same as the target table.
	     * 1. This method will skip over the fields whose indices appear in
	     *    parameter fieldsExplicitlySet.
	     * 2. If any timestamp columns with default of NOW are found,
	     *    their indices will be appended to nowFields.  It's up to the
	     *    caller to set these to the appropriate time.
	     */
	    void initVertexTupleWithDefaultValues(Pool* pool,
	    								catalog::GraphView const *catalogGraphView,
	                                    const std::set<int>& fieldsExplicitlySet,
	                                    TableTuple& tbTuple,
	                                    std::vector<int>& nowFields);

	    void initEdgeTupleWithDefaultValues(Pool* pool,
	    	                                    catalog::GraphView const *catalogGraphView,
	    	                                    const std::set<int>& fieldsExplicitlySet,
	    	                                    TableTuple& tbTuple,
	    	                                    std::vector<int>& nowFields);

	    GraphView *getGraphView() const ;


	    void setGraphView(GraphView * gv) {
	        m_graphView = gv;
	    }


	    const std::string& signature() { return m_signature; }

	    const char* signatureHash() { return m_signatureHash; }


	  private:
	    // GraphView *constructGraphViewFromCatalog(catalog::Database const &catalogDatabase,
	    //                                  catalog::GraphView const &catalogGraphView,
	    //                                  Table* vTable, Table* eTable, Table* pTable);
	    void preprocessGraphView(catalog::GraphView const &catalogGraphView, vector<string>* columnNamesVertex, vector<int>* columnIdsInVertexTable, vector<string>* columnNamesEdge, vector<int>* columnIdsInEdgeTable);

	    GraphView *constructSubGraphViewFromCatalog(catalog::Database const &catalogDatabase, catalog::GraphView const &catalogGraphView, vector<std::string> vLabels, vector<Table*> vTables, vector<std::string> eLabels, vector<Table*> eTables, vector<std::string> startVLabels, vector<std::string> endVLabels, Table* pTable, const string& subGraphVPredicate, const string& subGraphEPredicate, GraphView* oldGraphName, std::string vlabelName, std::string elabelName, bool isV);

	  	GraphView *constructGraphViewFromCatalog(catalog::Database const &catalogDatabase,
	                                     catalog::GraphView const &catalogGraphView, vector<std::string> vLabels,
	                                     vector<Table*> vTables, vector<std::string> eLabels,
	                                     vector<Table*> eTables, vector<std::string> startVLabels, 
	                                     vector<std::string> endVLabels, Table* pTable); // LX FEAT2

	    voltdb::GraphView *m_graphView;
	    const std::string m_signature;
	    const int32_t m_compactionThreshold;
	    char m_signatureHash[20];
};

}
#endif /* SRC_EE_GRAPH_GRAPHVIEWCATALOGDELEGATE_H_ */
