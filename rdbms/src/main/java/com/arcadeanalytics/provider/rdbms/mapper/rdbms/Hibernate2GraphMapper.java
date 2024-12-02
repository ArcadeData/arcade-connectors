package com.arcadeanalytics.provider.rdbms.mapper.rdbms;

/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2021 ArcadeData
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.rdbms.context.Statistics;
import com.arcadeanalytics.provider.rdbms.dbengine.DBQueryEngine;
import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderRuntimeException;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Attribute;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.HierarchicalBag;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Extends ER2GraphMapper thus manages the source DB schema and the destination graph model with
 * their correspondences. Unlike the superclass, this class builds the source DB schema starting
 * from Hibernate's XML migrationConfigDoc file.
 *
 * @author Gabriele Ponzi
 */
public class Hibernate2GraphMapper extends ER2GraphMapper {

  private static final Logger log = LoggerFactory.getLogger(Hibernate2GraphMapper.class);
  private final String xmlPath;

  public Hibernate2GraphMapper(
      DataSourceInfo dataSource,
      String xmlPath,
      List<String> includedTables,
      List<String> excludedTables,
      DBQueryEngine queryEngine,
      DBMSDataTypeHandler dataTypeHandler,
      String executionStrategy,
      NameResolver nameResolver,
      Statistics statistics) {
    super(
        dataSource,
        includedTables,
        excludedTables,
        queryEngine,
        dataTypeHandler,
        executionStrategy,
        nameResolver,
        statistics);
    this.xmlPath = xmlPath;
  }

  @Override
  public void buildSourceDatabaseSchema() {
    try {
      /*
       * Building Info from DB Schema
       */

      super.buildSourceDatabaseSchema();

      /*
       * XML Checking and Inheritance
       */

      // XML parsing and DOM building

      File xmlFile = new File(this.xmlPath);
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document dom = dBuilder.parse(xmlFile);

      NodeList entities = dom.getElementsByTagName("class");
      Element currentEntityElement;
      Entity currentEntity = null;

      for (int i = 0; i < entities.getLength(); i++) {
        currentEntityElement = (Element) entities.item(i);

        if (currentEntityElement.hasAttribute("table"))
          currentEntity =
              super.dataBaseSchema.getEntityByNameIgnoreCase(
                  currentEntityElement.getAttribute("table"));
        else {
          log.error(
              "XML Format error: problem in class definition, table attribute missing in the class"
                  + " node.\n");
          throw new RDBMSProviderRuntimeException();
        }

        // inheritance
        if (currentEntity != null)
          this.detectInheritanceAndUpdateSchema(currentEntity, currentEntityElement);
      }

      // sorting tables for inheritance level and then for name
      Collections.sort(super.dataBaseSchema.getEntities());
    } catch (Exception e) {
      throw new RDBMSProviderRuntimeException(e);
    }
  }

  private void detectInheritanceAndUpdateSchema(Entity parentEntity, Element parentEntityElement) {
    NodeList subclassElements = parentEntityElement.getElementsByTagName("subclass");
    NodeList joinedSubclassElements = parentEntityElement.getElementsByTagName("joined-subclass");
    NodeList unionSubclassElements = parentEntityElement.getElementsByTagName("union-subclass");
    Element discriminatorElement =
        (Element) parentEntityElement.getElementsByTagName("discriminator").item(0);

    HierarchicalBag hierarchicalBag = new HierarchicalBag();
    String rootDiscriminatorValue = null;

    // TABLE PER CLASS Hierarchy or Table per Subclass Inheritance
    if (subclassElements.getLength() > 0) {
      if (parentEntityElement.hasAttribute("discriminator-value"))
        rootDiscriminatorValue = parentEntityElement.getAttribute("discriminator-value");
      this.performSubclassTagInheritance(
          hierarchicalBag,
          parentEntity,
          subclassElements,
          discriminatorElement,
          rootDiscriminatorValue);
    }

    // TABLE PER SUBCLASS Inheritance
    if (joinedSubclassElements.getLength() > 0) {
      // initializing the hierarchical bag
      hierarchicalBag.setInheritancePattern("table-per-type");
      super.dataBaseSchema.getHierarchicalBags().add(hierarchicalBag);
      if (hierarchicalBag.getDepth2entities().get(parentEntity.getInheritanceLevel()) == null) {
        Set<Entity> tmp = new LinkedHashSet<Entity>();
        tmp.add(parentEntity);
        hierarchicalBag.getDepth2entities().put(parentEntity.getInheritanceLevel(), tmp);
        parentEntity.setHierarchicalBag(hierarchicalBag);
      }
      if (discriminatorElement != null) {
        hierarchicalBag.setDiscriminatorColumn(discriminatorElement.getAttribute("column"));
      }

      this.performJoinedSubclassTagInheritance(
          hierarchicalBag, parentEntity, joinedSubclassElements);
    }

    // TABLE PER CONCRETE CLASS Inheritance
    if (unionSubclassElements.getLength() > 0) {
      // initializing the hierarchical bag
      hierarchicalBag.setInheritancePattern("table-per-concrete-type");
      super.dataBaseSchema.getHierarchicalBags().add(hierarchicalBag);
      if (hierarchicalBag.getDepth2entities().get(parentEntity.getInheritanceLevel()) == null) {
        Set<Entity> tmp = new LinkedHashSet<Entity>();
        tmp.add(parentEntity);
        hierarchicalBag.getDepth2entities().put(parentEntity.getInheritanceLevel(), tmp);
        parentEntity.setHierarchicalBag(hierarchicalBag);
      }
      if (discriminatorElement != null) {
        hierarchicalBag.setDiscriminatorColumn(discriminatorElement.getAttribute("column"));
      }

      this.performUnionSubclassTagInheritance(hierarchicalBag, parentEntity, unionSubclassElements);
    }
  }

  // Table per Class Hierarchy or Table per Subclass Inheritance
  private void performSubclassTagInheritance(
      HierarchicalBag hierarchicalBag,
      Entity parentEntity,
      NodeList subclassElements,
      Element discriminatorElement,
      String rootDiscriminatorValue) {
    NodeList joinElements;
    Element currentEntityElement;
    String currentEntityElementName = null;
    Entity currentChildEntity;

    // distinguishing between "Table Per Class Hierarchy" and "Table Per Subclass" inheritance
    currentEntityElement = (Element) subclassElements.item(0);
    joinElements = currentEntityElement.getElementsByTagName("join");

    // Table Per Subclass inheritance when join elements are present
    if (joinElements.getLength() > 0) {
      // initializing the hierarchical bag
      hierarchicalBag.setInheritancePattern("table-per-type");
      super.dataBaseSchema.getHierarchicalBags().add(hierarchicalBag);
      if (hierarchicalBag.getDepth2entities().get(parentEntity.getInheritanceLevel()) == null) {
        Set<Entity> tmp = new LinkedHashSet<Entity>();
        tmp.add(parentEntity);
        hierarchicalBag.getDepth2entities().put(parentEntity.getInheritanceLevel(), tmp);
        parentEntity.setHierarchicalBag(hierarchicalBag);
      }
      if (discriminatorElement != null) {
        hierarchicalBag.setDiscriminatorColumn(discriminatorElement.getAttribute("column"));
      }

      for (int j = 0; j < subclassElements.getLength(); j++) {
        currentEntityElement = (Element) subclassElements.item(j);
        joinElements = currentEntityElement.getElementsByTagName("join");
        performJoinedSubclassTagInheritance(hierarchicalBag, parentEntity, joinElements);
      }
    }
    // Table per Class Hierarchy
    else {
      // initializing the hierarchical bag
      hierarchicalBag.setInheritancePattern("table-per-hierarchy");
      super.dataBaseSchema.getHierarchicalBags().add(hierarchicalBag);
      if (hierarchicalBag.getDepth2entities().get(parentEntity.getInheritanceLevel()) == null) {
        Set<Entity> tmp = new LinkedHashSet<Entity>();
        tmp.add(parentEntity);
        hierarchicalBag.getDepth2entities().put(parentEntity.getInheritanceLevel(), tmp);
        parentEntity.setHierarchicalBag(hierarchicalBag);
      }
      if (discriminatorElement != null) {
        hierarchicalBag.setDiscriminatorColumn(discriminatorElement.getAttribute("column"));
      }
      hierarchicalBag
          .getEntityName2discriminatorValue()
          .put(parentEntity.getName(), rootDiscriminatorValue);

      for (int i = 0; i < subclassElements.getLength(); i++) {
        currentEntityElement = (Element) subclassElements.item(i);

        if (currentEntityElement.hasAttribute("name"))
          currentEntityElementName = currentEntityElement.getAttribute("name");
        else {
          log.error(
              "XML Format error: problem in subclass definition, table attribute missing in the"
                  + " joined-subclass nodes.\n");
          throw new RDBMSProviderRuntimeException();
        }
        currentChildEntity = new Entity(currentEntityElementName, null, dataSource);

        // entity's attributes setting
        String discriminatorColumnName = discriminatorElement.getAttribute("column");
        parentEntity.removeAttributeByNameIgnoreCase(discriminatorColumnName);
        parentEntity.renumberAttributesOrdinalPositions();

        // primary key setting
        currentChildEntity.setPrimaryKey(parentEntity.getPrimaryKey());

        NodeList propertiesElements = currentEntityElement.getElementsByTagName("property");
        Element currentPropertyElement;
        Attribute currentChildAttribute;
        Attribute currentParentCorrespondingAttribute;

        for (int j = 0; j < propertiesElements.getLength(); j++) {
          currentPropertyElement = (Element) propertiesElements.item(j);
          currentParentCorrespondingAttribute =
              parentEntity.getAttributeByNameIgnoreCase(
                  currentPropertyElement.getAttribute("column"));

          // building child's attribute and removing the corresponding attribute from the parent
          // entity
          currentChildAttribute =
              new Attribute(
                  currentParentCorrespondingAttribute.getName(),
                  j + 1,
                  currentParentCorrespondingAttribute.getDataType(),
                  currentChildEntity);
          currentChildEntity.addAttribute(currentChildAttribute);
          parentEntity.getAttributes().remove(currentParentCorrespondingAttribute);
        }

        parentEntity.renumberAttributesOrdinalPositions();

        super.dataBaseSchema.getEntities().add(currentChildEntity);
        currentChildEntity.setParentEntity(parentEntity);
        currentChildEntity.setInheritanceLevel(parentEntity.getInheritanceLevel() + 1);

        // updating hierarchical bag
        if (hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel())
            == null) {
          Set<Entity> tmp = new LinkedHashSet<Entity>();
          tmp.add(currentChildEntity);
          hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
        } else {
          Set<Entity> tmp =
              hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel());
          tmp.add(currentChildEntity);
          hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
        }
        currentChildEntity.setHierarchicalBag(hierarchicalBag);
        hierarchicalBag
            .getEntityName2discriminatorValue()
            .put(
                currentChildEntity.getName(),
                currentEntityElement.getAttribute("discriminator-value"));
      }
    }
  }

  // Table per Subclass Inheritance
  private void performJoinedSubclassTagInheritance(
      HierarchicalBag hierarchicalBag, Entity parentEntity, NodeList joinedSubclassElements) {
    Element currentChildElement;
    Entity currentChildEntity;
    String currentChildEntityName = null;

    for (int i = 0; i < joinedSubclassElements.getLength(); i++) {
      currentChildElement = (Element) joinedSubclassElements.item(i);
      if (currentChildElement.hasAttribute("table"))
        currentChildEntityName = currentChildElement.getAttribute("table");
      else {
        log.error(
            "XML Format error: problem in subclass definition, table attribute missing in the"
                + " joined-subclass nodes.\n");
        throw new RDBMSProviderRuntimeException();
      }
      currentChildEntity = super.dataBaseSchema.getEntityByNameIgnoreCase(currentChildEntityName);
      currentChildEntity.setParentEntity(parentEntity);
      currentChildEntity.setInheritanceLevel(parentEntity.getInheritanceLevel() + 1);

      // removing attributes belonging to the primary key
      Attribute currentAttribute;
      Iterator<Attribute> it = currentChildEntity.getAttributes().iterator();
      while (it.hasNext()) {
        currentAttribute = it.next();
        if (currentChildEntity.getPrimaryKey().getInvolvedAttributes().contains(currentAttribute)) {
          it.remove();
        }
      }
      currentChildEntity.renumberAttributesOrdinalPositions();

      // updating hierarchical bag
      if (hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel())
          == null) {
        Set<Entity> tmp = new LinkedHashSet<Entity>();
        tmp.add(currentChildEntity);
        hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
      } else {
        Set<Entity> tmp =
            hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel());
        tmp.add(currentChildEntity);
        hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
      }
      currentChildEntity.setHierarchicalBag(hierarchicalBag);

      // recursive call on the node
      this.detectInheritanceAndUpdateSchema(currentChildEntity, currentChildElement);
    }
  }

  // Table per Concrete Class
  void performUnionSubclassTagInheritance(
      HierarchicalBag hierarchicalBag, Entity parentEntity, NodeList unionSubclassElements) {
    Element currentChildElement;
    Entity currentChildEntity;
    String currentChildEntityName = null;

    for (int i = 0; i < unionSubclassElements.getLength(); i++) {
      currentChildElement = (Element) unionSubclassElements.item(i);

      if (currentChildElement.hasAttribute("table"))
        currentChildEntityName = currentChildElement.getAttribute("table");
      else {
        log.error(
            "XML Format error: problem in subclass definition, table attribute missing in the"
                + " joined-subclass nodes.\n");
        throw new RDBMSProviderRuntimeException();
      }

      currentChildEntity = super.dataBaseSchema.getEntityByNameIgnoreCase(currentChildEntityName);
      currentChildEntity.setParentEntity(parentEntity);
      currentChildEntity.setInheritanceLevel(parentEntity.getInheritanceLevel() + 1);

      // removing attributes belonging to the primary key
      Attribute currentAttribute;
      Iterator<Attribute> it = currentChildEntity.getAttributes().iterator();
      while (it.hasNext()) {
        currentAttribute = it.next();
        if (currentChildEntity.getPrimaryKey().getInvolvedAttributes().contains(currentAttribute)) {
          it.remove();
        }
      }
      currentChildEntity.renumberAttributesOrdinalPositions();

      // updating hierarchical bag
      if (hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel())
          == null) {
        Set<Entity> tmp = new LinkedHashSet<Entity>();
        tmp.add(currentChildEntity);
        hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
      } else {
        Set<Entity> tmp =
            hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel());
        tmp.add(currentChildEntity);
        hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
      }
      currentChildEntity.setHierarchicalBag(hierarchicalBag);

      // recursive call on the node
      this.detectInheritanceAndUpdateSchema(currentChildEntity, currentChildElement);

      // removing inherited attributes
      it = currentChildEntity.getAttributes().iterator();
      while (it.hasNext()) {
        currentAttribute = it.next();
        if (parentEntity.getAttributes().contains(currentAttribute)) {
          it.remove();
          currentChildEntity.getInheritedAttributes().add(currentAttribute);
        }
      }
      currentChildEntity.renumberAttributesOrdinalPositions();
    }
  }
}
