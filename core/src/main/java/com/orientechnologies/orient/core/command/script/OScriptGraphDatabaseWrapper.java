/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.command.script;

import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.OAbstractPropertyGraph;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Database wrapper class to use from scripts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OScriptGraphDatabaseWrapper extends OScriptDocumentDatabaseWrapper {
  public OScriptGraphDatabaseWrapper(final OGraphDatabase iDatabase) {
    super(iDatabase);
  }

  public OScriptGraphDatabaseWrapper(final ODatabaseDocumentTx iDatabase) {
    super(new OGraphDatabase((ODatabaseRecordTx) iDatabase.getUnderlying()));
  }

  public OScriptGraphDatabaseWrapper(final ODatabaseRecordTx iDatabase) {
    super(iDatabase);
  }

  public OScriptGraphDatabaseWrapper(final String iURL) {
    super(iURL);
  }

  public long countVertexes() {
    return ((OAbstractPropertyGraph) database).countVertexes();
  }

  public long countEdges() {
    return ((OAbstractPropertyGraph) database).countEdges();
  }

  public Iterable<ODocument> browseVertices() {
    return ((OAbstractPropertyGraph) database).browseVertices();
  }

  public Iterable<ODocument> browseVertices(boolean iPolymorphic) {
    return ((OAbstractPropertyGraph) database).browseVertices(iPolymorphic);
  }

  public Iterable<ODocument> browseEdges() {
    return ((OAbstractPropertyGraph) database).browseEdges();
  }

  public Iterable<ODocument> browseEdges(boolean iPolymorphic) {
    return ((OAbstractPropertyGraph) database).browseEdges(iPolymorphic);
  }

  public Iterable<ODocument> browseElements(String iClass, boolean iPolymorphic) {
    return ((OAbstractPropertyGraph) database).browseElements(iClass, iPolymorphic);
  }

  public ODocument createVertex() {
    return ((OAbstractPropertyGraph) database).createVertex();
  }

  public ODocument createVertex(String iClassName) {
    return ((OAbstractPropertyGraph) database).createVertex(iClassName);
  }

  public ODocument createEdge(ORID iSourceVertexRid, ORID iDestVertexRid) {
    return ((OAbstractPropertyGraph) database).createEdge(iSourceVertexRid, iDestVertexRid);
  }

  public ODocument createEdge(ORID iSourceVertexRid, ORID iDestVertexRid, String iClassName) {
    return ((OAbstractPropertyGraph) database).createEdge(iSourceVertexRid, iDestVertexRid, iClassName);
  }

  public void removeVertex(ODocument iVertex) {
    ((OAbstractPropertyGraph) database).removeVertex(iVertex);
  }

  public void removeEdge(ODocument iEdge) {
    ((OAbstractPropertyGraph) database).removeEdge(iEdge);
  }

  public ODocument createEdge(ODocument iSourceVertex, ODocument iDestVertex) {
    return ((OAbstractPropertyGraph) database).createEdge(iSourceVertex, iDestVertex);
  }

  public ODocument createEdge(ODocument iOutVertex, ODocument iInVertex, String iClassName) {
    return ((OAbstractPropertyGraph) database).createEdge(iOutVertex, iInVertex, iClassName);
  }

  public Set<OIdentifiable> getEdgesBetweenVertexes(ODocument iVertex1, ODocument iVertex2) {
    return ((OAbstractPropertyGraph) database).getEdgesBetweenVertexes(iVertex1, iVertex2);
  }

  public Set<OIdentifiable> getEdgesBetweenVertexes(ODocument iVertex1, ODocument iVertex2, String[] iLabels) {
    return ((OAbstractPropertyGraph) database).getEdgesBetweenVertexes(iVertex1, iVertex2, iLabels);
  }

  public Set<OIdentifiable> getEdgesBetweenVertexes(ODocument iVertex1, ODocument iVertex2, String[] iLabels, String[] iClassNames) {
    return ((OAbstractPropertyGraph) database).getEdgesBetweenVertexes(iVertex1, iVertex2, iLabels, iClassNames);
  }

  public Set<OIdentifiable> getOutEdges(OIdentifiable iVertex) {
    return ((OAbstractPropertyGraph) database).getOutEdges(iVertex);
  }

  public Set<OIdentifiable> getOutEdges(OIdentifiable iVertex, String iLabel) {
    return ((OAbstractPropertyGraph) database).getOutEdges(iVertex, iLabel);
  }

  public Set<OIdentifiable> getOutEdgesHavingProperties(OIdentifiable iVertex, Map<String, Object> iProperties) {
    return ((OAbstractPropertyGraph) database).getOutEdgesHavingProperties(iVertex, iProperties);
  }

  public Set<OIdentifiable> getOutEdgesHavingProperties(OIdentifiable iVertex, Iterable<String> iProperties) {
    return ((OAbstractPropertyGraph) database).getOutEdgesHavingProperties(iVertex, iProperties);
  }

  public Set<OIdentifiable> getInEdges(OIdentifiable iVertex) {
    return ((OAbstractPropertyGraph) database).getInEdges(iVertex);
  }

  public Set<OIdentifiable> getInEdges(OIdentifiable iVertex, String iLabel) {
    return ((OAbstractPropertyGraph) database).getInEdges(iVertex, iLabel);
  }

  public Set<OIdentifiable> getInEdgesHavingProperties(OIdentifiable iVertex, Iterable<String> iProperties) {
    return ((OAbstractPropertyGraph) database).getInEdgesHavingProperties(iVertex, iProperties);
  }

  public Set<OIdentifiable> getInEdgesHavingProperties(ODocument iVertex, Map<String, Object> iProperties) {
    return ((OAbstractPropertyGraph) database).getInEdgesHavingProperties(iVertex, iProperties);
  }

  public ODocument getInVertex(OIdentifiable iEdge) {
    return ((OAbstractPropertyGraph) database).getInVertex(iEdge);
  }

  public ODocument getOutVertex(OIdentifiable iEdge) {
    return ((OAbstractPropertyGraph) database).getOutVertex(iEdge);
  }

  public ODocument getRoot(String iName) {
    return ((OAbstractPropertyGraph) database).getRoot(iName);
  }

  public ODocument getRoot(String iName, String iFetchPlan) {
    return ((OAbstractPropertyGraph) database).getRoot(iName, iFetchPlan);
  }

  public OAbstractPropertyGraph setRoot(String iName, ODocument iNode) {
    return ((OAbstractPropertyGraph) database).setRoot(iName, iNode);
  }

  public OClass createVertexType(String iClassName) {
    return ((OAbstractPropertyGraph) database).createVertexType(iClassName);
  }

  public OClass createVertexType(String iClassName, String iSuperClassName) {
    return ((OAbstractPropertyGraph) database).createVertexType(iClassName, iSuperClassName);
  }

  public OClass createVertexType(String iClassName, OClass iSuperClass) {
    return ((OAbstractPropertyGraph) database).createVertexType(iClassName, iSuperClass);
  }

  public OClass getVertexType(String iClassName) {
    return ((OAbstractPropertyGraph) database).getVertexType(iClassName);
  }

  public OClass createEdgeType(String iClassName) {
    return ((OAbstractPropertyGraph) database).createEdgeType(iClassName);
  }

  public OClass createEdgeType(String iClassName, String iSuperClassName) {
    return ((OAbstractPropertyGraph) database).createEdgeType(iClassName, iSuperClassName);
  }

  public OClass createEdgeType(String iClassName, OClass iSuperClass) {
    return ((OAbstractPropertyGraph) database).createEdgeType(iClassName, iSuperClass);
  }

  public OClass getEdgeType(String iClassName) {
    return ((OAbstractPropertyGraph) database).getEdgeType(iClassName);
  }

  public boolean isSafeMode() {
    return ((OAbstractPropertyGraph) database).isSafeMode();
  }

  public void setSafeMode(boolean safeMode) {
    ((OAbstractPropertyGraph) database).setSafeMode(safeMode);
  }

  public OClass getVertexBaseClass() {
    return ((OAbstractPropertyGraph) database).getVertexBaseClass();
  }

  public OClass getEdgeBaseClass() {
    return ((OAbstractPropertyGraph) database).getEdgeBaseClass();
  }

  public Set<OIdentifiable> filterEdgesByProperties(OMVRBTreeRIDSet iEdges, Iterable<String> iPropertyNames) {
    return ((OAbstractPropertyGraph) database).filterEdgesByProperties(iEdges, iPropertyNames);
  }

  public Set<OIdentifiable> filterEdgesByProperties(OMVRBTreeRIDSet iEdges, Map<String, Object> iProperties) {
    return ((OAbstractPropertyGraph) database).filterEdgesByProperties(iEdges, iProperties);
  }

  public boolean isUseCustomTypes() {
    return ((OAbstractPropertyGraph) database).isUseCustomTypes();
  }

  public void setUseCustomTypes(boolean useCustomTypes) {
    ((OAbstractPropertyGraph) database).setUseCustomTypes(useCustomTypes);
  }

  public boolean isVertex(ODocument iRecord) {
    return ((OAbstractPropertyGraph) database).isVertex(iRecord);
  }

  public boolean isEdge(ODocument iRecord) {
    return ((OAbstractPropertyGraph) database).isEdge(iRecord);
  }
}
