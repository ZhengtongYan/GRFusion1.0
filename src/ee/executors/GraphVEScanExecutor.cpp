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

#include <set> 

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

    // m_aggExec = voltdb::getInlineAggregateExecutor(node);
    // m_insertExec = voltdb::getInlineInsertExecutor(node);

	if (node->getPredicate() != NULL || node->getInlinePlanNodes().size() > 0) {
		// Create output table based on output schema from the plan
		const std::string& temp_name = isSubquery ?
				node->getChildren()[0]->getOutputTable()->name():
                graphView->getGraphTableName();
		// setTempOutputTable(limits, temp_name);
        setTempOutputTable(executorVector, temp_name);
		LogManager::GLog("GraphVEScanExecutor", "p_init", 70, "after calling setTempOutputTable with temp table = " + temp_name);
	}
	//
	// Otherwise create a new temp table that mirrors the
	// output schema specified in the plan (which should mirror
	// the output schema for any inlined projection)
	//
	else {
		Table* temp_t = isSubquery ?
				node->getChildren()[0]->getOutputTable() :
                 graphView->getGraphTable();
				 // graphView->getVertexTable();
		node->setOutputTable(temp_t);
		LogManager::GLog("GraphVEScanExecutor", "p_init", 83, "after calling setOutputTable with temp table name = " + temp_t->name());
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

    std::string edgeLabel = node->getChosenEdgeLabel();
    Table* inputEdgeTable = graphView->getEdgeTableFromLabel(edgeLabel);

    // cout << "GraphVEScanExecutor:122:" << vertexLabel << ", " << inputVertexTable->name() << endl;

    ProjectionPlanNode* projectionNode = dynamic_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PlanNodeType::Projection));
    
    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PlanNodeType::Limit));

    if (node->getPredicate() != NULL || projectionNode != NULL ||
        limit_node != NULL || m_aggExec != NULL)
    {
        AbstractExpression *predicate = node->getPredicate();

        if (predicate)
        {
            VOLT_TRACE("SCAN PREDICATE :\n%s\n", predicate->debug(true).c_str());
        }

        // std::vector<Vertex*> vertexlist;
        // std::vector<Edge*> edgelist;
        checkTupleForPredicate(predicate, inputVertexTable, vertexLabel, inputEdgeTable, edgeLabel, limit_node, projectionNode, graphView, params);

        // graphView->addToSubgraphVertex(vertexlist);
        // graphView->addToSubgraphEdge(edgelist);
        if (m_aggExec != NULL) {
            m_aggExec->p_execute_finish();
        }

    }
    VOLT_TRACE("\n%s\n", node->getOutputTable()->debug().c_str());
    VOLT_DEBUG("Finished Seq scanning");

    return true;
}

/*
    Returns the selected vertices with its adjacency list and the selected edges
 */
void GraphVEScanExecutor::checkTupleForPredicate(AbstractExpression* predicate, Table* inputVertexTable, std::string vlabel, Table* inputEdgeTable, std::string elabel, LimitPlanNode* limit_node, ProjectionPlanNode* projectionNode, GraphView* graphView, const NValueArray &params) {
    
    if (((!predicate->getLeftExp()->getGraphObject().empty()) && 
            (predicate->getLeftExp()->getGraphObject()).compare("VERTEXES") == 0) || 
                ((!predicate->getRightExp()->getGraphObject().empty()) && 
                     (predicate->getRightExp()->getGraphObject()).compare("VERTEXES") == 0) ) {
        filterFromGraph(predicate, inputVertexTable, vlabel, limit_node, projectionNode, graphView, "VERTEX", params);
        return;
    }
    else if (((!predicate->getLeftExp()->getGraphObject().empty()) && 
            (predicate->getLeftExp()->getGraphObject()).compare("EDGES") == 0) || 
                ((!predicate->getRightExp()->getGraphObject().empty()) && 
                     (predicate->getRightExp()->getGraphObject()).compare("EDGES") == 0)){
        filterFromGraph(predicate, inputEdgeTable, elabel, limit_node, projectionNode, graphView, "EDGE", params);
        return ;
    }

    if (predicate->getLeftExp()->getGraphObject().empty() && predicate->getRightExp()->getGraphObject().empty()) {
        checkTupleForPredicate(predicate->getLeftExp(), inputVertexTable, vlabel, inputEdgeTable, elabel, limit_node, projectionNode, graphView, params);
        checkTupleForPredicate(predicate->getRightExp(), inputVertexTable, vlabel, inputEdgeTable, elabel, limit_node, projectionNode, graphView, params);
    }

    // ExpressionType etype = predicate->getExpressionType();
    // switch (etype) {
    //     case:

    //     default:
    // }
    
    return ;
}

void GraphVEScanExecutor::filterFromGraph(AbstractExpression* predicate, Table* inputTable, std::string label, LimitPlanNode* limit_node, ProjectionPlanNode* projectionNode, GraphView* graphView, std::string obj, const NValueArray &params) {

    TableTuple tuple(inputTable->schema());
    TableIterator iterator = inputTable->iteratorDeletingAsWeGo();
    // AbstractExpression *predicate = node->getPredicate();
    int num_of_columns = -1;
    if (projectionNode != NULL) {
        num_of_columns = static_cast<int> (projectionNode->getOutputColumnExpressions().size());
    }
    cout << "GraphVEScanExecutor:177:" << num_of_columns << endl;
   
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    LogManager::GLog("GraphVEScanExecutor", "p_execute:189", offset, "offset");
    if (limit_node) {
        std::tie(limit, offset) = limit_node->getLimitAndOffset(params);
    }
    // Initialize the postfilter
    CountingPostfilter postfilter(m_tmpOutputTable, predicate, limit, offset);

    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
    TableTuple temp_tuple;
    vassert(m_tmpOutputTable);
    if (m_aggExec != NULL){
        LogManager::GLog("GraphVEScanExecutor", "p_execute", 206, "1");
        const TupleSchema * inputSchema = inputTable->schema();
        if (projectionNode != NULL) {
            inputSchema = projectionNode->getOutputTable()->schema();
        }
        
        temp_tuple = m_aggExec->p_execute_init(params, &pmp, inputSchema, m_tmpOutputTable, &postfilter);

    } else {
        LogManager::GLog("GraphVEScanExecutor", "p_execute", 206, "2");
        temp_tuple = m_tmpOutputTable->tempTuple();
    }
    
    int id;
    std::set<std::string> edgelist;

    while (postfilter.isUnderLimit() && iterator.next(tuple))
    {
#if   defined(VOLT_TRACE_ENABLED)
        int tuple_ctr = 0;
#endif
        VOLT_TRACE("INPUT TUPLE: %s, %d/%d\n",
                   tuple.debug(inputTable->name()).c_str(), tuple_ctr,
                   (int)inputTable->activeTupleCount());
        pmp.countdownProgress();
        // LogManager::GLog("GraphVEScanExecutor", "p_execute", 230, tuple.debug(inputVertexTable->name()).c_str());
        cout << "GraphVEScanExecutor:233:" << tuple.debug(inputTable->name()).c_str() << endl;

        if (postfilter.eval(&tuple, NULL))
        {
            VOLT_TRACE("inline projection...");
            //get the vertex id
            id = ValuePeeker::peekInteger(tuple.getNValue(0));
            if (obj.compare("VERTEX") == 0) {
                Vertex * v = graphView->getVertex(label + "." + to_string(id));
                // vertexlist.insert(label + "." + to_string(id));
                // subVertex->push_back(v);
                // add all its inEdge and outEdge list to subEdge
                for (int i = 0; i < v->fanOut(); i++) {
                    // subEdge->push_back(v->getOutEdge(i));
                    edgelist.insert(v->getOutEdge(i)->getId());
                }
                for (int j = 0; j < v->fanIn(); j++) {
                    // subEdge->push_back(v->getInEdge(j));
                    edgelist.insert(v->getInEdge(j)->getId());
                }

            }
            else if (obj.compare("EDGE") == 0) {
                Edge * e = graphView->getEdge(label + "." + to_string(id));
                edgelist.insert(e->getId());
                // subEdge->push_back(e);
                // add its startVertex and endVertex to subVertex
                // subVertex->push_back(e->getStartVertex());
                // subVertex->push_back(e->getEndVertex());
                
            }
            
            pmp.countdownProgress();
        }
    }
    Edge * e;
    for (std::set<std::string>::iterator it = edgelist.begin(); it!=edgelist.end(); ++it) {
        e = graphView->getEdge(*it);
        temp_tuple.setNValue(0, ValueFactory::getStringValue(e->getId()));
        temp_tuple.setNValue(1, ValueFactory::getStringValue(e->getStartVertex()->getId()));
        temp_tuple.setNValue(2, ValueFactory::getStringValue(e->getEndVertex()->getId()));
        outputTuple(temp_tuple);
    }

}


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
