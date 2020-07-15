/*
 * GraphVEScanExecutor.h
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#ifndef SRC_EE_EXECUTORS_GraphVESCANEXECUTOR_H_
#define SRC_EE_EXECUTORS_GraphVESCANEXECUTOR_H_

#include <vector>
#include "boost/shared_array.hpp"
#include "common/common.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "executors/abstractexecutor.h"
#include "execution/VoltDBEngine.h"
#include "graph/Vertex.h"
#include "graph/Edge.h"
#include "plannodes/limitnode.h"

namespace voltdb {

class AbstractExpression;
class TempTable;
class TempTableLimits;
class Table;
class AggregateExecutorBase;
class GraphView;
class CountingPostfilter; // modified LX

class GraphVEScanExecutor : public AbstractExecutor {

public:
	GraphVEScanExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
		: AbstractExecutor(engine, abstract_node)
		  , m_aggExec(NULL)
	{
         //output_table = NULL;
         LogManager::GLog("GraphVEScanExecutor", "Constructor", 32, abstract_node->debug());
    }
        ~GraphVEScanExecutor();

    // modified by LX
    // using AbstractExecutor::p_init;
    // virtual protected bool p_init(AbstractPlanNode*, TempTableLimits* limits);
    protected:
        bool p_init(AbstractPlanNode*, const ExecutorVector& executorVector); // LX
        bool p_execute(const NValueArray &params);

        //void setTempOutputTable(TempTableLimits* limits, const string tempTableName);

    private:
        void outputTuple(TableTuple& tuple);
        void filterFromGraph(AbstractExpression* predicate, std::vector<Vertex*> subVertex, std::vector<Edge*> subEdge, Table* inputTable, std::string label, LimitPlanNode* limit_node, GraphView* graphView, std::string obj,const NValueArray &params);
        void checkTupleForPredicate(AbstractExpression* predicate, std::vector<Vertex*> subVertex, std::vector<Edge*> subEdge, Table* inputVertexTable, std::string vlable, Table* inputEdgeTable, std::string elabel, LimitPlanNode* limit_node, GraphView* graphView, const NValueArray &params);
        AggregateExecutorBase* m_aggExec;
        GraphView* graphView;
        std::string m_vLabel;


};

}
#endif /* SRC_EE_EXECUTORS_GraphVESCANEXECUTOR_H_ */
