/*
 * GraphVEScanExecutor.cpp
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#include "GraphVEScanExecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "storage/temptable.h"
#include "logging/LogManager.h"
#include "graph/GraphView.h"
#include "graph/Vertex.h"
#include "plannodes/GraphVEScanNode.h"

#include "common/FatalException.hpp"
#include "executors/aggregateexecutor.h"
#include "executors/executorutil.h"
#include "execution/ProgressMonitorProxy.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/limitnode.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/ValueFactory.hpp"

#include "executors/insertexecutor.h"
#include "plannodes/insertnode.h"


namespace voltdb {

bool GraphVEScanExecutor::p_init(AbstractPlanNode *abstractNode, const ExecutorVector& executorVector)
{
	VOLT_TRACE("init GraphVEScan Executor");
    std::cout << "GraphVEScan:44" << endl;
	GraphVEScanPlanNode* node = dynamic_cast<GraphVEScanPlanNode*>(abstractNode);
	// vassert(node);
	bool isSubquery = node->isSubQuery();
	// vassert(isSubquery || node->getTargetGraphView());
	// vassert((! isSubquery) || (node->getChildren().size() == 1));
    // vassert(!node->isSubquery() || (node->getChildren().size() == 1));
	graphView = node->getTargetGraphView();
    m_vLabel = "BALL"; // hard-coded for now

	std::cout << graphView->name() << ", " << m_vLabel << endl;
    // m_aggExec = voltdb::getInlineAggregateExecutor(node);
    // m_insertExec = voltdb::getInlineInsertExecutor(node);

	if (node->getPredicate() != NULL || node->getInlinePlanNodes().size() > 0) {
		// Create output table based on output schema from the plan
        std::cout << graphView->getVertexTableFromLabel(m_vLabel)->name() << endl;
		const std::string& temp_name = isSubquery ?
				node->getChildren()[0]->getOutputTable()->name():
                graphView->getVertexTableFromLabel(m_vLabel)->name(); // LX FEAT2
				// graphView->getVertexTable()->name();
		// setTempOutputTable(limits, temp_name);
        setTempOutputTable(executorVector, temp_name);
		// LogManager::GLog("GraphVEScanExecutor", "p_init", 70,
		// 		"after calling setTempOutputTable with temp table = " + temp_name);
	}
	//
	// Otherwise create a new temp table that mirrors the
	// output schema specified in the plan (which should mirror
	// the output schema for any inlined projection)
	//
	else {
		Table* temp_t = isSubquery ?
				 node->getChildren()[0]->getOutputTable() :
                 graphView->getVertexTableFromLabel(m_vLabel); // LX FEAT2
				 // graphView->getVertexTable();
		node->setOutputTable(temp_t);
		LogManager::GLog("GraphVEScanExecutor", "p_init", 83,
						"after calling setOutputTable with temp table name = " + temp_t->name());
	}

	//node->setOutputTable(node->getTargetGraphView()->getVertexTable());
	// Inline aggregation can be serial, partial or hash
	// m_aggExec = voltdb::getInlineAggregateExecutor(node);
    LogManager::GLog("GraphVEScanExecutor", "p_init", 87, "done init");
    return true;
}

bool GraphVEScanExecutor::p_execute(const NValueArray &params) {
    GraphVEScanPlanNode* node = dynamic_cast<GraphVEScanPlanNode*>(m_abstractNode);
    vassert(node);
    LogManager::GLog("GraphVEScanExecutor", "p_execute", 96, "begin execute");

    // Short-circuit an empty scan
    if (node->isEmptyScan()) {
        cout << "GraphVEScanExecutor:106" << endl;
        VOLT_DEBUG ("Empty GraphVE Scan :\n %s", output_table->debug().c_str());
        return true;
    }

    GraphView* graphView = node->getTargetGraphView();
    // std::string vertexLabel = "BALL";
    std::string vertexLabel = node->getChosenVertexLabel();
    Table* inputVertexTable = graphView->getVertexTableFromLabel(vertexLabel);
    std::string edgeLabel = ""; //node->getChosenVertexLabel();
    Table* inputEdgeTable = graphView->getVertexTableFromLabel(edgeLabel);

    cout << "GraphVEScanExecutor:122:" << vertexLabel << ", " << inputVertexTable->name() << endl;

    std::vector<Vertex*> vertexlist;
    std::vector<Edge*> edgelist;
    // vassert(inputVertexTable);
    int vertexId = -1, fanIn = -1, fanOut = -1;

    // VOLT_TRACE("Sequential Scanning vertexes in :\n %s",
    //            inputVertexTable->debug().c_str());
    // VOLT_DEBUG("Sequential Scanning vertexes table : %s which has %d active, %d"
    //            " allocated",
    //            inputVertexTable->name().c_str(),
    //            (int)inputVertexTable->activeTupleCount(),
    //            (int)inputVertexTable->allocatedTupleCount());

    // int num_of_columns = -1;
    ProjectionPlanNode* projectionNode = dynamic_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PlanNodeType::Projection));
    // if (projectionNode != NULL) {
        // cout << "GraphVEScanExecutor:150" << endl;
        // num_of_columns = 1;
        // num_of_columns = static_cast<int> (projectionNode->getOutputColumnExpressions().size());
        // LogManager::GLog("GraphVEScanExecutor", "p_execute", 151, projectionNode->getOutputColumnNames()[0] + projectionNode->getOutputColumnNames()[1]);
    // }
    // LogManager::GLog("GraphVEScanExecutor", "p_execute:140", num_of_columns, "num_of_columns"  );


    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PlanNodeType::Limit));

    if (node->getPredicate() != NULL || projectionNode != NULL ||
        limit_node != NULL || m_aggExec != NULL)
    {
        TableTuple tuple(inputVertexTable->schema());
        TableTuple tuple1(inputEdgeTable->schema());
        TableIterator iterator = inputVertexTable->iteratorDeletingAsWeGo();
        AbstractExpression *predicate = node->getPredicate();

        if (predicate)
        {
            VOLT_TRACE("SCAN PREDICATE :\n%s\n", predicate->debug(true).c_str());
        }

        int limit = CountingPostfilter::NO_LIMIT;
        int offset = CountingPostfilter::NO_OFFSET;
        LogManager::GLog("GraphVEScanExecutor", "p_execute:189", offset, "offset");
        if (limit_node) {
            std::tie(limit, offset) = limit_node->getLimitAndOffset(params);
            // limit_node->getLimitAndOffsetByReference(params, limit, offset);
        }
        // Initialize the postfilter
        CountingPostfilter postfilter(m_tmpOutputTable, predicate, limit, offset);

        ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
        TableTuple temp_tuple;
        vassert(m_tmpOutputTable);
        if (m_aggExec != NULL){//} || m_insertExec != NULL) {
            LogManager::GLog("GraphVEScanExecutor", "p_execute", 206, "1");
            const TupleSchema * inputSchema = inputVertexTable->schema();
            // if (projectionNode != NULL) {
            //     inputSchema = projectionNode->getOutputTable()->schema();
            // }
            
            temp_tuple = m_aggExec->p_execute_init(params, &pmp, inputSchema, m_tmpOutputTable, &postfilter);

        } else {
            LogManager::GLog("GraphVEScanExecutor", "p_execute", 206, "2");
            temp_tuple = m_tmpOutputTable->tempTuple();
        }
        
        while (postfilter.isUnderLimit() && iterator.next(tuple))
        {
#if   defined(VOLT_TRACE_ENABLED)
            int tuple_ctr = 0;
#endif
            VOLT_TRACE("INPUT TUPLE: %s, %d/%d\n",
                       tuple.debug(inputVertexTable->name()).c_str(), tuple_ctr,
                       (int)inputVertexTable->activeTupleCount());
            pmp.countdownProgress();
            // LogManager::GLog("GraphVEScanExecutor", "p_execute", 230, tuple.debug(inputVertexTable->name()).c_str());
            cout << "GraphVEScanExecutor:233:" << tuple.debug(inputVertexTable->name()).c_str() << endl;
            //
            // For each tuple we need to evaluate it against our predicate and limit/offset
            //
            if (postfilter.eval(&tuple, NULL))
            {

                // if (projectionNode != NULL)
                // {
                    VOLT_TRACE("inline projection...");
                    //get the vertex id
                    vertexId = ValuePeeker::peekInteger(tuple.getNValue(0));
                    vertexlist.push_back(graphView->getVertex(vertexLabel + "." + to_string(vertexId)));
                    cout << "GraphVEScanExecutor:250:" << vertexId << endl;
                    // fanOut = graphView->getVertex(vertexId)->fanOut();
                    // fanIn = graphView->getVertex(vertexId)->fanIn();
                    cout << "GraphVEScanExecutor:251:vertexID:" << vertexId << ": " << fanOut << ", " << fanIn << endl;
                    // for (int ctr = 0; ctr < num_of_columns ; ctr++) {
                    // 	//msaber: todo, need to check the projection operator construction
                    // 	//and modify it to allow selecting graph vertex attributes
                        
                    //     // LX: add workaround to solve inconsistent column index issue: given a ctr index, find the column name
                    //     string t_coln = projectionNode->getOutputColumnNames()[ctr];
                    //     cout << "GraphVEScanExecutor:257:" << t_coln << endl;
                    //     if (t_coln.compare(vertexLabel + ".FANIN") == 0){
                    //         temp_tuple.setNValue(ctr, ValueFactory::getIntegerValue(fanIn));
                    //         continue;
                    //     }
                    //     else if (t_coln.compare(vertexLabel + ".FANOUT") == 0){
                    //         temp_tuple.setNValue(ctr, ValueFactory::getIntegerValue(fanOut));
                    //         continue;
                    //     }
                        
                    //     // int newctr = graphView->getColumnIdInVertexTable(tbl_ctr);
                    //     // NValue value = projectionNode->getOutputColumnExpressions()[newctr]->eval(&tuple, NULL);
                    //     NValue value = projectionNode->getOutputColumnExpressions()[ctr]->eval(&tuple, NULL);
                    //     temp_tuple.setNValue(ctr, value);
                    // }
                    // outputTuple(temp_tuple);
                // }
                // else
                // {
                //     outputTuple( tuple);
                // }
                pmp.countdownProgress();
            }
        }
        graphView->addToSubgraphVertex(vertexlist);
        if (m_aggExec != NULL) {
            m_aggExec->p_execute_finish();
        }

    }
    VOLT_TRACE("\n%s\n", node->getOutputTable()->debug().c_str());
    VOLT_DEBUG("Finished Seq scanning");

    return true;
}

/**
 * Set up a multi-column temp output table for those executors that require one.
 * Called from p_init.

void GraphVEScanExecutor::setTempOutputTable(TempTableLimits* limits, const string tempTableName) {

	LogManager::GLog("GraphVEScanExecutor", "setTempOutputTable", 255,
							"setTempOutputTable in GraphVEScanExecutor called with tempTableName = " + tempTableName);

    assert(limits);
    TupleSchema* schema = m_abstractNode->generateTupleSchema();
    int column_count = schema->columnCount();
    std::vector<std::string> column_names(column_count);
    assert(column_count >= 1);
    const std::vector<SchemaColumn*>& outputSchema = m_abstractNode->getOutputSchema();

    for (int ctr = 0; ctr < column_count; ctr++) {
        column_names[ctr] = outputSchema[ctr]->getColumnName();
    }

    m_tmpOutputTable = TableFactory::getTempTable(m_abstractNode->databaseId(), tempTableName, schema, column_names, limits);
    m_abstractNode->setOutputTable(m_tmpOutputTable);
}
 */

void GraphVEScanExecutor::outputTuple(TableTuple& tuple)
{
    if (m_aggExec != NULL) {
        m_aggExec->p_execute_tuple(tuple);
        return;
    }
    // else if (m_insertExec != NULL) {
    //     m_insertExec->p_execute_tuple(tuple);
    //     return;
    // }
    //
    // Insert the tuple into our output table
    //
    vassert(m_tmpOutputTable);
    m_tmpOutputTable->insertTempTuple(tuple);
}


GraphVEScanExecutor::~GraphVEScanExecutor() {
	// TODO Auto-generated destructor stub
}

}
