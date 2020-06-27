package org.hsqldb_voltpatches;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;

public class GraphView implements SchemaObject {

    public static final GraphView[] emptyArray = new GraphView[]{};
    
	protected HsqlName GraphName;
    public Database database;
    protected boolean isDirected;
    protected int type;
	
    // String VSubQuery;
    HashMappedList VSubQueryList;// map label to vquery index LX FEAT2
    String ESubQuery;
    // HsqlName VTableName;
    HashMappedList VTableNameList; // map label idx to vtable name LX FEAT2
    HsqlName ETableName;

    String statement;

    ArrayList<String> VLabelList;// LX FEAT2
        
    private Integer EDGE = 1;
    private Integer VERTEX = 2;
    private Integer PATH = 3;

    private ArrayList AllPropList; // array of properties
    private ArrayList<HsqlName> AllVTableList;// LX FEAT2
    private ArrayList<String> AllVSubQueryList;// LX FEAT2

    private HashMappedList idxToIdx0; // LX FEAT2
    private HashMappedList idxToPropTypeList;// map global index to proptype (edge/vertex/path)LX FEAT2
    private HashMappedList idxToLabelIdxList; // map global index to label index LX FEAT2
    
    private HashMappedList VertexPropList; // maps vertex: name - id 
    // private ArrayList VertexPropList; // LX FEAT2 store all the vertex props
    private int vertexPropCount;
    
    private HashMappedList EdgePropList; // maps edge: name  - id
    // private ArrayList EdgePropList; // LX FEAT2
    private int edgePropCount;
    
    private HashMappedList PathPropList; // maps path: name   - id 
    // private ArrayList PathPropList; // LX FEAT2
    private int pathPropCount;
    
    private final long DefPrecision = 10;

    /*
        index is the global index, index0 is the local index
    */
        
    

    String Hint;
	
    public GraphView(Database database, HsqlName name, int type) {
    	this.database = database;
    	GraphName = name;
    	
    	this.type = type;
    	if (type == TableBase.DIRECTED_GRAPH) isDirected = true;
    	else isDirected = false;

        VTableNameList = new HashMappedList();//LX FEAT2
        VSubQueryList = new HashMappedList();//LX FEAT2
        VLabelList = new ArrayList<String>(); // LX FEAT2
    	
    	AllPropList     = new ArrayList();
        AllVTableList   = new ArrayList<HsqlName>();// LX FEAT2
        AllVSubQueryList = new ArrayList<String>();// LX FEAT2
        idxToIdx0           = new HashMappedList(); // LX FEAT2
    	idxToPropTypeList    = new HashMappedList();
        idxToLabelIdxList    = new HashMappedList();//LX FEAT2
    	VertexPropList  = new HashMappedList();
    	vertexPropCount = 0;
    	EdgePropList    = new HashMappedList();
    	edgePropCount = 0;
    	PathPropList    = new HashMappedList();
    	pathPropCount = 0;
    	
    }
    
    /*
	 * Adds default edge Vertex properties
	 * Called after adding all other columns from tables 
	 * in order to have column indices from source select statement matched indices of not defailt properties 
	 */
    public void addDefVertexProps(HsqlName schema, boolean isDelimitedIdentifier, String label) {
    	
    	// VERTEX Def Prop
    	HsqlName Name = database.nameManager.newColumnHsqlName(schema, label + ".FANOUT", isDelimitedIdentifier);
    	ColumnSchema fanOut = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addVertexPropNoCheck(fanOut, label);  
        
        Name = database.nameManager.newColumnHsqlName(schema, label + ".FANIN", isDelimitedIdentifier);
    	ColumnSchema fanIn = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addVertexPropNoCheck(fanIn, label);
    	
    }
    
    /*
	 * Adds default Path properties
	 * Called after adding all other columns from tables 
	 * in order to have column indices from source select statement matched indices of not defailt properties 
	 */
    public void addDefPathProps(HsqlName schema, boolean isDelimitedIdentifier) {
    	
    	HsqlName Name;
    	// PATHS Def Prop
    	Name = database.nameManager.newColumnHsqlName(schema, "STARTVERTEXID", isDelimitedIdentifier);
    	ColumnSchema col = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(col);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "ENDVERTEXID", isDelimitedIdentifier);
    	col = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(col);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "LENGTH", isDelimitedIdentifier);
    	ColumnSchema pathLength = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(pathLength);    	
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "COST", isDelimitedIdentifier);
    	ColumnSchema pathCost = new ColumnSchema(Name, new NumberType(Types.SQL_DOUBLE, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(pathCost);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "PATH", isDelimitedIdentifier);
    	col = new ColumnSchema(Name, new CharacterType(Types.SQL_VARCHAR, 1024), false, false, null);
    	addPathPropNoCheck(col);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "PROP1", isDelimitedIdentifier);
    	col = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(col);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "PROP2", isDelimitedIdentifier);
    	col = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(col);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "PROP3", isDelimitedIdentifier);
    	col = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(col);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "PROP4", isDelimitedIdentifier);
    	col = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(col);
    	
    	Name = database.nameManager.newColumnHsqlName(schema, "PROP5", isDelimitedIdentifier);
    	col = new ColumnSchema(Name, new NumberType(Types.SQL_INTEGER, DefPrecision, 0), false, false, null);
    	addPathPropNoCheck(col);
    	
    }
    
	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return type;
	}

	@Override
	public HsqlName getName() {
		// TODO Auto-generated method stub
		return GraphName;
	}

	@Override
	public HsqlName getSchemaName() {
		// TODO Auto-generated method stub
		return GraphName.schema;
	}

	@Override
	public HsqlName getCatalogName() {
		// TODO Auto-generated method stub
		return database.getCatalogName();
	}

	@Override
	public Grantee getOwner() {
		// TODO Auto-generated method stub
		return GraphName.schema.owner;
	}

	@Override
	public OrderedHashSet getReferences() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OrderedHashSet getComponents() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void compile(Session session) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getSQL() {
		// TODO Auto-generated method stub
		return statement;
	}


	public void setSQL(String sqlString) {
		statement = sqlString;
		
	}
	
    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */


    VoltXMLElement voltGetGraphXML(Session session)
            throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        VoltXMLElement graphxml = new VoltXMLElement("graph");
        Map<String, String> autoGenNameMap = new HashMap<String, String>();

        // add graph metadata
        String graphName = getName().name;
        graphxml.attributes.put("name", graphName);
        
        // graphxml.attributes.put("Vtable", VTableName.name);
        graphxml.attributes.put("Etable", ETableName.name);
        
        // graphxml.attributes.put("Vquery", VSubQuery);
        graphxml.attributes.put("Equery", ESubQuery);

        graphxml.attributes.put("isdirected", String.valueOf(isDirected));
        
        graphxml.attributes.put("DDL", statement);
        
        // read all the vertex properties
        //VoltXMLElement vertexes = new VoltXMLElement("vertexes");
        //vertexes.attributes.put("name", "vertexes");
        
        // LX FEAT2        
        for (int i = 0; i < VLabelList.size(); i++){
            VoltXMLElement vertex = new VoltXMLElement("vertex");
            String curLabel = VLabelList.get(i);
            vertex.attributes.put("label", curLabel);
            vertex.attributes.put("Vtable", AllVTableList.get((int)VTableNameList.get(curLabel)).name);
            vertex.attributes.put("Vquery", AllVSubQueryList.get((int)VSubQueryList.get(curLabel)));
            
            // int countCurLabelProp = 0;
            for (int j = 0; j < getAllPropCount(); j++){
                if (idxToPropTypeList.get(j) == VERTEX ){
                    if ((int)idxToLabelIdxList.get(j) == VLabelList.indexOf(curLabel)){
                        ColumnSchema property = getVertexProp(j);
                        VoltXMLElement propChild = property.voltGetColumnXML(session);
                        propChild.attributes.put("index", Integer.toString(j));
                        // Index Vertex props from 0 ... 
                        // propChild.attributes.put("index0", Integer.toString(VertexPropList.getIndex(property.getNameString())));
                        propChild.attributes.put("index0", Integer.toString((int)idxToIdx0.get(j)));
                        // countCurLabelProp++;
                        vertex.children.add(propChild);
                        assert(propChild != null);
                    }
                }
            }
            graphxml.children.add(vertex);
        }
  
        VoltXMLElement path = new VoltXMLElement("path");
        for (int i = 0; i < getAllPropCount(); i++) {
        	if (idxToPropTypeList.get(i) == PATH) {
        		ColumnSchema property = getPathProp(i);
        		VoltXMLElement propChild = property.voltGetColumnXML(session);
        		
        		propChild.attributes.put("index", Integer.toString(i));
        		// Index Path props from 0 ... 
        		propChild.attributes.put("index0", Integer.toString(PathPropList.getIndex(property.getNameString())));
                // propChild.attributes.put("index0", Integer.toString(idxToIdx0.get(i))); // LX FEAT2
        		
        		path.children.add(propChild);
        		assert(propChild != null);
        	}
        }

        graphxml.children.add(path);

        VoltXMLElement edge = new VoltXMLElement("edge");
        for (int i = 0; i < getAllPropCount(); i++) {
        	if (idxToPropTypeList.get(i) == EDGE) {
        		ColumnSchema property = getEdgeProp(i);
        		VoltXMLElement propChild = property.voltGetColumnXML(session);
        		
        		propChild.attributes.put("index", Integer.toString(i));
        		// Index Edge props from 0 ... 
        		propChild.attributes.put("index0", Integer.toString(EdgePropList.getIndex(property.getNameString())));
                // propChild.attributes.put("index0", Integer.toString(idxToIdx0.get(i))); // LX FEAT2
        		
        		edge.children.add(propChild);
        		assert(propChild != null);
        	}
        }
        graphxml.children.add(edge);
        /*
        HsqlName[] EdgeProperties = getEdgeProperties();
        for (HsqlName prop : EdgeProperties) {
        	VoltXMLElement property = new VoltXMLElement("property");
            property.attributes.put("name", prop.statementName);
            edge.children.add(property);
        }
        */
        //edges.children.add(edge);
        
        return graphxml;
    }

    /*
	private HsqlName[] getEdgeProperties() {
		// TODO Auto-generated method stub
		return EdgeProperties;
	}

	private HsqlName[] getVertexProperties() {
		// TODO Auto-generated method stub
		return VertexProperties;
	}
    */
    
    // LX FEAT2
    public ArrayList<String> getVertexLabelList() {
        return VLabelList;
    }

    // LX FEAT2
    public String getVertexLabelByIndex(int idx) {
        return VLabelList.get(idx);
    }

    // LX FEAT2
    public int getVLabelIdxByIndex(int idx) {
        return (int)idxToLabelIdxList.get(idx);
    }

    // LX FEAT2
    public void addVertexLabel(String newLabel) {
        // should check the label to be unique
        for (int i = 0; i < VLabelList.size(); i++){
            if (VLabelList.get(i).equals(newLabel)){
                // TODO: throw some exception
                throw new RuntimeException("Two vertex labels are the same");
            }
        }
        VLabelList.add(newLabel);
        return;
    }

    // LX FEAT2
    public void addVTableName(HsqlName vname, String label){
        AllVTableList.add(vname);
        int idx = AllVTableList.indexOf(vname);
        VTableNameList.add(label, idx);
        return;
    }

    // LX FEAT2
    public void addVSubQuery(String vquery, String label){
        AllVSubQueryList.add(vquery);
        int idx = AllVSubQueryList.indexOf(vquery);
        VSubQueryList.add(label, idx);
        return;
    }

    public int getPropIndex0(int i) {
        // System.out.println("GraphView:388:" + i + "," + idxToIdx0.size());
        // return (int)idxToIdx0.get(i); 
        
    	if (idxToPropTypeList.get(i) == VERTEX) {
    		// return VertexPropList.getIndex(getVertexProp(i).getNameString());
            return (int)idxToIdx0.get(i); // LX FEAT2
    	}
    	else if (idxToPropTypeList.get(i) == EDGE) {
    		return EdgePropList.getIndex(getEdgeProp(i).getNameString());
    	}
    	else
    		return PathPropList.getIndex(getPathProp(i).getNameString());
    }
    
	public ColumnSchema getVertexProp(int i) {
		return (ColumnSchema) AllPropList.get(i);
		// return (ColumnSchema) VertexPropList.get(idxToIdx0.get(i)); // LX FEAT2
	}

    /**
     *  Returns the count of all visible vertex properties.
     */
    //public int getVertexPropCount() {
    //    return vertexPropCount;
    //}
    
    void renameVertexProp(ColumnSchema property, HsqlName newName) {

        String oldname = property.getName().name;
        int    i       = getVertexPropIndex(oldname);

        ((ColumnSchema)AllPropList.get(i)).getName().rename(newName);
    }
    
    /**
     *  Returns the index in vertex prop list of given column name or throws if not found
     */
    public int getVertexPropIndex(String name) {

        int i = findVertexProp(name);

        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        return i;
    }
    
    /**
     *  Returns the index of given column name or -1 if not found.
     */
    public int findVertexProp(String name) { // format: label.colName LX FEAT2
        // System.out.println("GraphView:437:" + name);
        int index = (Integer)VertexPropList.get(name);
        // System.out.println("GraphView:439:" + index + ", " + idxToIdx0.size() + ", " + getAllPropCount() + ", " + VertexPropList.size());
        return index; 
    }
    
    public void addVertexPropNoCheck(ColumnSchema property, String label) {
        AllPropList.add(property);
        int idx = AllPropList.indexOf(property);
        int vlabidx = VLabelList.indexOf(label);
    	VertexPropList.add(property.getName().name, idx);
    	idxToPropTypeList.add(idx, VERTEX);
        
        int idx0 = 0;
        for (int i = 0 ; i < getAllPropCount(); i++){
            if (idxToPropTypeList.get(i) == VERTEX)
                if ((int)idxToLabelIdxList.get(i) == vlabidx)
                    idx0++;
        }
        // System.out.println("GraphView:456:" + idx);
        idxToIdx0.add(idx, idx0);
        idxToLabelIdxList.add(idx, vlabidx);
        vertexPropCount++;
    }
    // EDGES
	public ColumnSchema getEdgeProp(int i) {
		return (ColumnSchema) AllPropList.get(i);//EdgePropList.get(i);
        // return (ColumnSchema) EdgePropList.get(idxToIdx0.get(i)); // LX FEAT2
	}

    // PATHS
	public ColumnSchema getPathProp(int i) {
		return (ColumnSchema) AllPropList.get(i);//PathPropList.get(i);
        // return (ColumnSchema) PathPropList.get(idxToIdx0.get(i)); // LX FEAT2
	}
	
    // All comment by LX FEAT2
	// public ColumnSchema getAllProp(int i) {
		//System.out.println(i);
		// return (ColumnSchema) AllPropList.get(i);//PathPropList.get(i);
	// }
	
    /**
     *  Returns the count of all visible vertex properties.
     */
    //public int getEdgePropCount() {
    //    return edgePropCount;
    //}
    
    void renameEdgeProp(ColumnSchema property, HsqlName newName) {

        String oldname = property.getName().name;
        int    i       = getEdgePropIndex(oldname);

        ((ColumnSchema)AllPropList.get(i)).getName().rename(newName);
    }
    
    /**
     *  Returns the index of given edge property name or throws if not found
     */
    public int getEdgePropIndex(String name) {

        int i = findEdgeProp(name);
        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }
        return i;
    }
    
    /**
     *  Returns the index of given edge property name or -1 if not found.
     */
    public int findEdgeProp(String name) {
        int index = (Integer)EdgePropList.get(name);
        return index;
    }
    
    public void addEdgePropNoCheck(ColumnSchema property) {
        // LX FEAT2
        // EdgePropList.add(property);
        // idxToPropTypeList.add(EDGE);
        // idxToLabelIdxList.add(""); // TODO: add edge label later
        // idxToIdx0.add(edgePropCount);
        // edgePropCount++;
        AllPropList.add(property);
        int idx = AllPropList.indexOf(property);
    	EdgePropList.add(property.getName().name, idx);
    	idxToPropTypeList.add(idx, EDGE);
        idxToLabelIdxList.add(idx, ""); // TODO: fix the edge label later
        edgePropCount++;

        idxToIdx0.add(idx, -1); // TODO: fix this issue later
    }
    // Path
    /**
     *  Returns the index of given column name or throws if not found
     */
    public int getPathPropIndex(String name) {

        int i = findPathProp(name);

        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        return i;
    }
    
    /**
     *  Returns the count of all visible vertex properties.
     */
    //public int getPathPropCount() {
    //    return pathPropCount;
    //}
    
    /**
     *  Returns the index of given column name or -1 if not found.
     */
    public int findPathProp(String name) {
        int index = (Integer)PathPropList.get(name);
        return index;
    }
    
    public void addPathPropNoCheck(ColumnSchema property) {
        // LX FEAT2
        // EdgePropList.add(property);
        // idxToPropTypeList.add(EDGE);
        // idxToLabelIdxList.add(""); // empty string because path doesn't have labels
        // idxToIdx0.add(pathPropCount);
        // pathPropCount++;
        AllPropList.add(property);
        int idx = AllPropList.indexOf(property);
    	PathPropList.add(property.getName().name, idx);
    	idxToPropTypeList.add(idx, PATH);
        idxToLabelIdxList.add(idx, ""); // path doesn't have a label
        pathPropCount++;
        idxToIdx0.add(idx, -1); // TODO: fix this issue later
    }
    
    /**
     *  Returns the count of all visible properties.
     */
    public int getAllPropCount() {
        return pathPropCount+edgePropCount+vertexPropCount;
    }
    
    public boolean isVertex(int id) {
    	if (idxToPropTypeList.get(id) == VERTEX)
    		return true;
    	return false;
    }
    
    public boolean isEdge(int id) {
    	if (idxToPropTypeList.get(id) == EDGE)
    		return true;
    	return false;
    }
    
    public boolean isPath(int id) {
    	if (idxToPropTypeList.get(id) == PATH)
    		return true;
    	return false;
    }
}
