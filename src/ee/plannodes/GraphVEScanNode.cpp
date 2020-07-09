//#include "storage/table.h"
#include "graph/GraphView.h"
#include "GraphVEScanNode.h"
#include "execution/VoltDBEngine.h"
#include "graph/GraphViewCatalogDelegate.h"

using namespace std;

namespace voltdb
{

GraphVEScanPlanNode::GraphVEScanPlanNode() {
	// TODO Auto-generated constructor stub

}

GraphVEScanPlanNode::~GraphVEScanPlanNode() {
	// TODO Auto-generated destructor stub
}

PlanNodeType GraphVEScanPlanNode::getPlanNodeType() const { return PlanNodeType::GraphVEScan; }

std::string GraphVEScanPlanNode::debugInfo(const string& spacer) const
{
    std::ostringstream buffer;
    buffer << "VerexScan PlanNode";
    return buffer.str();
}

GraphView* GraphVEScanPlanNode::getTargetGraphView() const
{
	if (m_gcd == NULL)
	{
		return NULL;
	}
	return m_gcd->getGraphView();
}

std::string GraphVEScanPlanNode::getChosenVertexLabel() const
{
	return m_chosenVertexLabel;
}

void GraphVEScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
	std::cout << "GraphVEScan:46" << endl;
	m_target_graph_name = obj.valueForKey("TARGET_GRAPH_NAME").asStr();

	m_hasVertexLabel = obj.hasNonNullKey("CHOSEN_VERTEX_LABEL"); 
	if (m_hasVertexLabel)
		m_chosenVertexLabel = obj.valueForKey("CHOSEN_VERTEX_LABEL").asStr();
	else {
		// m_chosenVertexLabel needs to include all vertex labels in this graph view
		// TODO: fix later
	}

	m_subGraphName = obj.valueForKey("SUB_GRAPH_NAME").asStr();
	// m_subGraphVertex = obj.valueForKey("SUB_GRAPH_VERTEX").asBool();
	// m_subGraphEdge = obj.valueForKey("SUB_GRAPH_EDGE").asBool();
	m_vertexTabAlias = obj.valueForKey("VERTEX_TABLE_ALIAS").asStr();
	m_edgeTabAlias = obj.valueForKey("EDGE_TABLE_ALIAS").asStr();

	m_isEmptyScan = obj.hasNonNullKey("PREDICATE_FALSE");

	// Set the predicate (if any) only if it's not a trivial FALSE expression
	if (!m_isEmptyScan)
	{
		m_predicate.reset(loadExpressionFromJSONObject("PREDICATE", obj));
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
	    	LogManager::GLog("GraphVEScanPlanNode", "loadFromJSONObject", 79, "Target graph view name = " + m_gcd->getGraphView()->name());
	    }
	}
}

}
