/*
 * VertexScanExecutor.cpp
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#include "VertexScanExecutor.h"

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
#include "plannodes/VertexScanNode.h"

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

bool VertexScanExecutor::p_init(AbstractPlanNode *abstractNode, const ExecutorVector& executorVector)
{
	VOLT_TRACE("init VertexScan Executor");

	VertexScanPlanNode* node = dynamic_cast<VertexScanPlanNode*>(abstractNode);
	vassert(node);
	bool isSubquery = node->isSubQuery();
	vassert(isSubquery || node->getTargetGraphView());
	vassert((! isSubquery) || (node->getChildren().size() == 1));
    // vassert(!node->isSubquery() || (node->getChildren().size() == 1));
	graphView = node->getTargetGraphView();
    m_vLabel = node->getVertexLabel(); // LX FEAT2

	//
	// OPTIMIZATION: If there is no predicate for this SeqScan,
	// then we want to just set our OutputTable pointer to be the
	// pointer of our TargetTable. This prevents us from just
	// reading through the entire TargetTable and copying all of
	// the tuples. We are guarenteed that no Executor will ever
	// modify an input table, so this operation is safe
	//
    m_aggExec = voltdb::getInlineAggregateExecutor(node);
    // m_insertExec = voltdb::getInlineInsertExecutor(node);

	if (node->getPredicate() != NULL || node->getInlinePlanNodes().size() > 0) {
		// Create output table based on output schema from the plan
		const std::string& temp_name = (node->isSubQuery()) ?
				node->getChildren()[0]->getOutputTable()->name():
                graphView->getVertexTableFromLabel(m_vLabel)->name(); // LX FEAT2
				// graphView->getVertexTable()->name();
		// setTempOutputTable(limits, temp_name);
        setTempOutputTable(executorVector, temp_name);
		LogManager::GLog("VertexScanExecutor", "p_init", 70,
				"after calling setTempOutputTable with temp table = " + temp_name);
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
		LogManager::GLog("VertexScanExecutor", "p_init", 83,
						"after calling setOutputTable with temp table name = " + temp_t->name());
	}

	//node->setOutputTable(node->getTargetGraphView()->getVertexTable());
	// Inline aggregation can be serial, partial or hash
	// m_aggExec = voltdb::getInlineAggregateExecutor(node);

    return true;
}

bool VertexScanExecutor::p_execute(const NValueArray &params) {
    VertexScanPlanNode* node = dynamic_cast<VertexScanPlanNode*>(m_abstractNode);
    vassert(node);
    LogManager::GLog("VertexScanExecutor", "p_execute", 96,
                "begin execute");

    // Short-circuit an empty scan
    if (node->isEmptyScan()) {
        cout << "VertexScanExecutor:106" << endl;
        VOLT_DEBUG ("Empty Vertex Scan :\n %s", output_table->debug().c_str());
        return true;
    }

    /*
    Table* input_table = (node->isSubQuery()) ?
            node->getChildren()[0]->getOutputTable():
            node->getTargetTable();
	*/
    GraphView* graphView = node->getTargetGraphView();
    // Table* input_table = graphView->getVertexTable();

    // LX FEAT2
    std::string vertexLabel = node->getVertexLabel();
    Table* input_table = graphView->getVertexTableFromLabel(vertexLabel);
    cout << "VertexScanExecutor:122:" << vertexLabel << ", " << input_table->name() << endl;

    vassert(input_table);
    int vertexId = -1, fanIn = -1, fanOut = -1;

    //* for debug */std::cout << "SeqScanExecutor: node id " << node->getPlanNodeId() <<
    //* for debug */    " input table " << (void*)input_table <<
    //* for debug */    " has " << input_table->activeTupleCount() << " tuples " << std::endl;
    VOLT_TRACE("Sequential Scanning vertexes in :\n %s",
               input_table->debug().c_str());
    VOLT_DEBUG("Sequential Scanning vertexes table : %s which has %d active, %d"
               " allocated",
               input_table->name().c_str(),
               (int)input_table->activeTupleCount(),
               (int)input_table->allocatedTupleCount());

    //
    // OPTIMIZATION: NESTED PROJECTION
    //
    // Since we have the input params, we need to call substitute to
    // change any nodes in our expression tree to be ready for the
    // projection operations in execute
    //

    int num_of_columns = -1;

    ProjectionPlanNode* projectionNode = dynamic_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PlanNodeType::Projection));
    if (projectionNode != NULL) {
        cout << "VertexScanExecutor:150" << endl;
        // num_of_columns = 1;
        num_of_columns = static_cast<int> (projectionNode->getOutputColumnExpressions().size());
        // LogManager::GLog("VertexScanExecutor", "p_execute", 151, projectionNode->getOutputColumnNames()[0] + projectionNode->getOutputColumnNames()[1]);
    }
    LogManager::GLog("VertexScanExecutor", "p_execute:140", num_of_columns, "num_of_columns"  );
    //
    // OPTIMIZATION: NESTED LIMIT
    // How nice! We can also cut off our scanning with a nested limit!
    //

    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PlanNodeType::Limit));

    //
    // OPTIMIZATION:
    //
    // If there is no predicate and no Projection for this SeqScan,
    // then we have already set the node's OutputTable to just point
    // at the TargetTable. Therefore, there is nothing we more we need
    // to do here
    //

    if (node->getPredicate() != NULL || projectionNode != NULL ||
        limit_node != NULL || m_aggExec != NULL)
    {
        //
        // Just walk through the table using our iterator and apply
        // the predicate to each tuple. For each tuple that satisfies
        // our expression, we'll insert them into the output table.
        //
        TableTuple tuple(input_table->schema());
        TableIterator iterator = input_table->iteratorDeletingAsWeGo();
        AbstractExpression *predicate = node->getPredicate();

        if (predicate)
        {
            VOLT_TRACE("SCAN PREDICATE :\n%s\n", predicate->debug(true).c_str());
        }

        int limit = CountingPostfilter::NO_LIMIT;
        int offset = CountingPostfilter::NO_OFFSET;
        LogManager::GLog("VertexScanExecutor", "p_execute:189", offset, "offset");
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
            LogManager::GLog("VertexScanExecutor", "p_execute", 206, "1");
            const TupleSchema * inputSchema = input_table->schema();
            if (projectionNode != NULL) {
                inputSchema = projectionNode->getOutputTable()->schema();
            }
            
            // if (m_aggExec != NULL) {
            temp_tuple = m_aggExec->p_execute_init(params, &pmp, inputSchema, m_tmpOutputTable, &postfilter);
            // }
            // else{
                // if (!m_insertExec->p_execute_init(inputSchema, m_tmpOutputTable, temp_tuple)) {
                //     return true;
                // }
                // vassert(projectionNode != NULL ? (temp_tuple.getSchema()->columnCount() == projectionNode->getOutputColumnExpressions().size()) : true);
            // }
        } else {
            LogManager::GLog("VertexScanExecutor", "p_execute", 206, "2");
            temp_tuple = m_tmpOutputTable->tempTuple();
        }
        
        while (postfilter.isUnderLimit() && iterator.next(tuple))
        {
#if   defined(VOLT_TRACE_ENABLED)
            int tuple_ctr = 0;
#endif
            VOLT_TRACE("INPUT TUPLE: %s, %d/%d\n",
                       tuple.debug(input_table->name()).c_str(), tuple_ctr,
                       (int)input_table->activeTupleCount());
            pmp.countdownProgress();
            // LogManager::GLog("VertexScanExecutor", "p_execute", 230, tuple.debug(input_table->name()).c_str());
            cout << "VertexScanExecutor:233:" << tuple.debug(input_table->name()).c_str() << endl;
            //
            // For each tuple we need to evaluate it against our predicate and limit/offset
            //
            if (postfilter.eval(&tuple, NULL))
            {
                //
                // Nested Projection
                // Project (or replace) values from input tuple
                //
                if (projectionNode != NULL)
                {
                    // LogManager::GLog("VertexScanExecutor", "p_execute", 243, "243");

                    VOLT_TRACE("inline projection...");
                    //get the vertex id
                    vertexId = ValuePeeker::peekInteger(tuple.getNValue(0));
                    cout << "VertexScanExecutor:250:" << vertexId << endl;
                    fanOut = graphView->getVertex(vertexLabel + "." + to_string(vertexId))->fanOut();
                    fanIn = graphView->getVertex(vertexLabel + "." + to_string(vertexId))->fanIn();
                    cout << "VertexScanExecutor:251:vertexID:" << vertexId << ": " << fanOut << ", " << fanIn << endl;
                    for (int ctr = 0; ctr < num_of_columns ; ctr++) {
                    	//msaber: todo, need to check the projection operator construction
                    	//and modify it to allow selecting graph vertex attributes
                        
                        // LX: add workaround to solve inconsistent column index issue: given a ctr index, find the column name
                        string t_coln = projectionNode->getOutputColumnNames()[ctr];
                        cout << "VertexScanExecutor:257:" << t_coln << endl;
                        // cout << "VertexScanExecutor:259:" << projectionNode->getOutputColumnExpressions()[ctr]->debug(true).c_str() << endl;
                        if (t_coln.compare(vertexLabel + ".FANIN") == 0){
                            temp_tuple.setNValue(ctr, ValueFactory::getIntegerValue(fanIn));
                            continue;
                        }
                        else if (t_coln.compare(vertexLabel + ".FANOUT") == 0){
                            temp_tuple.setNValue(ctr, ValueFactory::getIntegerValue(fanOut));
                            continue;
                        }

                        // int tbl_ctr;
                        // for (tbl_ctr = 0; tbl_ctr < num_of_columns - 2; tbl_ctr++){
                        //     string c_coln = graphView->getVertexAttributeName(tbl_ctr);

                        //     if (t_coln.compare(c_coln) == 0)
                        //         break;
                        // }
                        
                        // int newctr = graphView->getColumnIdInVertexTable(tbl_ctr);
                        // NValue value = projectionNode->getOutputColumnExpressions()[newctr]->eval(&tuple, NULL);
                        NValue value = projectionNode->getOutputColumnExpressions()[ctr]->eval(&tuple, NULL);
                        
                        // LogManager::GLog("VertexScanExecutor", "p_execute:254", 254, projectionNode->getOutputColumnExpressions()[ctr]->debug(true).c_str());
                        temp_tuple.setNValue(ctr, value);
                    }
                    outputTuple(temp_tuple);
                }
                else
                {
                    outputTuple( tuple);
                }
                pmp.countdownProgress();
            }
        }

        if (m_aggExec != NULL) {
            m_aggExec->p_execute_finish();
        }
        // else if (m_insertExec != NULL) {
        //     m_insertExec->p_execute_finish();
        // }
    }
    //* for debug */std::cout << "SeqScanExecutor: node id " << node->getPlanNodeId() <<
    //* for debug */    " output table " << (void*)output_table <<
    //* for debug */    " put " << output_table->activeTupleCount() << " tuples " << std::endl;
    VOLT_TRACE("\n%s\n", node->getOutputTable()->debug().c_str());
    VOLT_DEBUG("Finished Seq scanning");

    return true;
}

/**
 * Set up a multi-column temp output table for those executors that require one.
 * Called from p_init.

void VertexScanExecutor::setTempOutputTable(TempTableLimits* limits, const string tempTableName) {

	LogManager::GLog("VertexScanExecutor", "setTempOutputTable", 255,
							"setTempOutputTable in VertexScanExecutor called with tempTableName = " + tempTableName);

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

void VertexScanExecutor::outputTuple(TableTuple& tuple)
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


VertexScanExecutor::~VertexScanExecutor() {
	// TODO Auto-generated destructor stub
}

}
