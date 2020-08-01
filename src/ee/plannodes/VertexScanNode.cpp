/*
 * VertexScan.cpp
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

//#include "storage/table.h"
#include "graph/GraphView.h"
#include "VertexScanNode.h"
#include "execution/VoltDBEngine.h"
#include "graph/GraphViewCatalogDelegate.h"

using namespace std;

namespace voltdb
{

VertexScanPlanNode::VertexScanPlanNode() {
	// TODO Auto-generated constructor stub

}

VertexScanPlanNode::~VertexScanPlanNode() {
	// TODO Auto-generated destructor stub
}

PlanNodeType VertexScanPlanNode::getPlanNodeType() const { return PlanNodeType::VertexScan; }

std::string VertexScanPlanNode::debugInfo(const string& spacer) const
{
    std::ostringstream buffer;
    buffer << "VerexScan PlanNode";
    return buffer.str();
}

GraphView* VertexScanPlanNode::getTargetGraphView() const
{
	if (m_gcd == NULL)
	{
		return NULL;
	}
	return m_gcd->getGraphView();
}

// LX FEAT2
std::string VertexScanPlanNode::getVertexLabel() const
{
	return m_vertexLabel;
}

// LX FEAT7
bool VertexScanPlanNode::checkHasGraphHint() const
{
	return m_hasHint;
}

// LX FEAT7
std::string VertexScanPlanNode::getGraphHint() const
{
	return m_hint;
}

void VertexScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
	m_target_graph_name = obj.valueForKey("TARGET_GRAPH_NAME").asStr();

	m_vertexLabel = obj.valueForKey("VERTEX_LABEL").asStr(); // LX FEAT2

	m_isEmptyScan = obj.hasNonNullKey("PREDICATE_FALSE");

	// Set the predicate (if any) only if it's not a trivial FALSE expression
	if (!m_isEmptyScan)
	{
		m_predicate.reset(loadExpressionFromJSONObject("PREDICATE", obj));
	}

	// LX FEAT7
	m_hasHint = obj.hasNonNullKey("HINT");
	// cout << "VertexScanPlanNode::80:" << m_hasHint << endl;
	if (m_hasHint) {
		m_hint = obj.valueForKey("HINT").asStr();
		// cout << "VertexScanPlanNode:82:" << m_hint << endl;
	}

	m_isSubQuery = obj.hasNonNullKey("SUBQUERY_INDICATOR");

	if (m_isSubQuery) {
		m_gcd = NULL;
	} else
	{
		VoltDBEngine* engine = ExecutorContext::getEngine();
	    m_gcd = engine->getGraphViewDelegate(m_target_graph_name);
	    if ( ! m_gcd) {
	    	VOLT_ERROR("Failed to retrieve target graph view from execution engine for PlanNode '%s'",
	        debug().c_str());
	            //TODO: throw something
	    }
	    else
	    {
	    	LogManager::GLog("VertexScanPlanNode", "loadFromJSONObject", 73, "Target graph view name = " + m_gcd->getGraphView()->name());
	    }
	}
}

}
