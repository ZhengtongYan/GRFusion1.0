/*
 * VertexScan.h
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#ifndef SRC_EE_PLANNODES_VERTEXSCANNODE_H_
#define SRC_EE_PLANNODES_VERTEXSCANNODE_H_

#include "plannodes/abstractplannode.h"
#include "abstractscannode.h"
#include "expressions/abstractexpression.h"

namespace voltdb
{

class GraphViewCatalogDelegate;
class GraphView;

class VertexScanPlanNode : public AbstractPlanNode {
public:
	VertexScanPlanNode();
	virtual ~VertexScanPlanNode();

	PlanNodeType getPlanNodeType() const;
	std::string debugInfo(const std::string& spacer) const;

	GraphView* getTargetGraphView() const;
	std::string getVertexLabel() const; // LX FEAT2
	void setTargetGraphViewDelegate(GraphViewCatalogDelegate* gcd) { m_gcd = gcd; } // DEPRECATED?

	std::string getTargetGraphViewName() const { return m_target_graph_name; } // DEPRECATED?
	AbstractExpression* getPredicate() const { return m_predicate.get(); }

	bool isSubQuery() const { return m_isSubQuery; }

	bool isEmptyScan() const { return m_isEmptyScan; }

protected:
    void loadFromJSONObject(PlannerDomValue obj);


    std::string m_vertexLabel; // LX FEAT2
    std::string m_target_graph_name;
	GraphViewCatalogDelegate* m_gcd;
	//
	// This is the predicate used to filter out tuples during the scan
	//
	boost::scoped_ptr<AbstractExpression> m_predicate;
	// True if this scan represents a sub query
	bool m_isSubQuery;
	// True if this scan has a predicate that always evaluates to FALSE
	bool m_isEmptyScan;
};

}

#endif /* SRC_EE_PLANNODES_VERTEXSCANNODE_H_ */
