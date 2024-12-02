/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.arcadeanalytics.provider.rdbms.hibernate;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.rdbms.context.Statistics;
import com.arcadeanalytics.provider.rdbms.dbengine.DBQueryEngine;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.ER2GraphMapper;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.Hibernate2GraphMapper;
import com.arcadeanalytics.provider.rdbms.model.dbschema.CanonicalRelationship;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.HierarchicalBag;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.VertexType;
import com.arcadeanalytics.provider.rdbms.nameresolver.JavaConventionNameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.HSQLDBDataTypeHandler;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Gabriele Ponzi
 */
class HibernateMapperTest {

  private static final String XML_TABLE_PER_CLASS =
      "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerClassHierarchyInheritanceTest.xml";
  private static final String XML_TABLE_PER_SUBCLASS1 =
      "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerSubclassInheritanceTest1.xml";
  private static final String XML_TABLE_PER_SUBCLASS2 =
      "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerSubclassInheritanceTest2.xml";
  private static final String XML_TABLE_PER_CONCRETE_CLASS =
      "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerConcreteClassInheritanceTest.xml";
  private ER2GraphMapper mapper;
  private DBQueryEngine dbQueryEngine;
  private String driver = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private DataSourceInfo dataSource;
  private String executionStrategy;
  private JavaConventionNameResolver nameResolver;
  private HSQLDBDataTypeHandler dataTypeHandler;
  private Statistics statistics;

  @BeforeEach
  void init() {
    this.dataSource =
        new DataSourceInfo(
            1L,
            "RDBMS_HSQL",
            "testDataSource",
            "desc",
            "mem",
            1234,
            "mydb",
            username,
            password,
            false,
            "{}",
            false,
            false,
            "",
            22,
            "",
            false);

    dbQueryEngine = new DBQueryEngine(dataSource, 300);
    executionStrategy = "not_specified";
    nameResolver = new JavaConventionNameResolver();
    dataTypeHandler = new HSQLDBDataTypeHandler();
    statistics = new Statistics();
  }

  /*
   * Table per Class Hierarchy Inheritance (<subclass> tag)
   *  table ( http://www.javatpoint.com/hibernate-table-per-hierarchy-example-using-xml-file )
   */

  @Test
  void TablePerClassHierarchyInheritance() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residence =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, TYPE varchar(256), NAME"
              + " varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), PAY_PER_HOUR"
              + " decimal(10,2), CONTRACT_DURATION varchar(256), RESIDENCE varchar(256),primary key"
              + " (id), foreign key (RESIDENCE) references RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      this.mapper =
          new Hibernate2GraphMapper(
              dataSource,
              HibernateMapperTest.XML_TABLE_PER_CLASS,
              null,
              null,
              dbQueryEngine,
              dataTypeHandler,
              executionStrategy,
              nameResolver,
              statistics);

      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new JavaConventionNameResolver());

      /*
       *  Testing context information
       */

      assertThat(statistics.totalNumberOfEntities).isEqualTo(2);
      assertThat(statistics.builtEntities).isEqualTo(2);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(1);
      assertThat(statistics.builtRelationships).isEqualTo(1);

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(4);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(4);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(1);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(1);

      /*
       *  Testing built source db schema
       */

      Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      Entity regularEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByNameIgnoreCase("REGULAR_EMPLOYEE");
      Entity contractEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByNameIgnoreCase("CONTRACT_EMPLOYEE");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(4);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity).isNotNull();
      assertThat(regularEmployeeEntity).isNotNull();
      assertThat(contractEmployeeEntity).isNotNull();
      assertThat(residenceEntity).isNotNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(3);

      assertThat(employeeEntity.getAttributeByName("ID")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(employeeEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(employeeEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(employeeEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("RESIDENCE")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getName()).isEqualTo("RESIDENCE");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition()).isEqualTo(3);
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getAttributes().size()).isEqualTo(2);

      assertThat(regularEmployeeEntity.getAttributeByName("SALARY")).isNotNull();
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getName()).isEqualTo("SALARY");
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName())
          .isEqualTo("Regular_Employee");

      assertThat(regularEmployeeEntity.getAttributeByName("BONUS")).isNotNull();
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getName()).isEqualTo("BONUS");
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName())
          .isEqualTo("Regular_Employee");

      assertThat(contractEmployeeEntity.getAttributes().size()).isEqualTo(2);

      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR")).isNotNull();
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName())
          .isEqualTo("PAY_PER_HOUR");
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              contractEmployeeEntity
                  .getAttributeByName("PAY_PER_HOUR")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("Contract_Employee");

      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION")).isNotNull();
      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName())
          .isEqualTo("CONTRACT_DURATION");
      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeEntity
                  .getAttributeByName("CONTRACT_DURATION")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("Contract_Employee");

      // inherited attributes check
      assertThat(employeeEntity.getInheritedAttributes().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getInheritedAttributes().size()).isEqualTo(3);

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("ID")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName())
          .isEqualTo("RESIDENCE");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("RESIDENCE")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributes().size()).isEqualTo(3);

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getName())
          .isEqualTo("ID");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("ID")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName())
          .isEqualTo("RESIDENCE");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("RESIDENCE")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      // primary key check
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size()).isEqualTo(1);
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("ID");
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size())
          .isEqualTo(1);
      assertThat(contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("ID");
      assertThat(
              contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      // relationship, primary and foreign key check
      assertThat(regularEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(residenceEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(regularEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(residenceEntity.getInCanonicalRelationships().size()).isEqualTo(1);

      assertThat(regularEmployeeEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(residenceEntity.getForeignKeys().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itRes =
          residenceEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentResRel = itRes.next();
      assertThat(currentResRel).isEqualTo(currentEmpRel);

      // inherited relationships check
      assertThat(regularEmployeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritedOutCanonicalRelationships().size())
          .isEqualTo(1);
      assertThat(employeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(residenceEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      Iterator<CanonicalRelationship> itContEmp =
          contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      CanonicalRelationship currentRegEmpRel = itRegEmp.next();
      CanonicalRelationship currentContEmpRel = itContEmp.next();
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentRegEmpRel.getFromColumns().get(0).getName()).isEqualTo("RESIDENCE");
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentContEmpRel.getFromColumns().get(0).getName()).isEqualTo("RESIDENCE");
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      // inheritance check
      assertThat(regularEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(contractEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(employeeEntity.getParentEntity()).isNull();
      assertThat(residenceEntity.getParentEntity()).isNull();

      assertThat(regularEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeEntity.getInheritanceLevel()).isEqualTo(0);
      assertThat(residenceEntity.getInheritanceLevel()).isEqualTo(0);

      // Hierarchical Bag check
      assertThat(mapper.getDataBaseSchema().getHierarchicalBags().size()).isEqualTo(1);
      HierarchicalBag hierarchicalBag = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      assertThat(hierarchicalBag.getInheritancePattern()).isEqualTo("table-per-hierarchy");

      assertThat(hierarchicalBag.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag.getDepth2entities().get(0).size()).isEqualTo(1);
      Iterator<Entity> it = hierarchicalBag.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag.getDepth2entities().get(1).size()).isEqualTo(2);
      it = hierarchicalBag.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("Regular_Employee");
      assertThat(it.next().getName()).isEqualTo("Contract_Employee");
      assertThat(it.hasNext()).isFalse();

      assertThat(employeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);
      assertThat(regularEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);
      assertThat(contractEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);

      assertThat(hierarchicalBag.getDiscriminatorColumn()).isNotNull();
      assertThat(hierarchicalBag.getDiscriminatorColumn()).isEqualTo("TYPE");

      assertThat(hierarchicalBag.getEntityName2discriminatorValue().size()).isEqualTo(3);
      assertThat(hierarchicalBag.getEntityName2discriminatorValue().get("EMPLOYEE"))
          .isEqualTo("emp");
      assertThat(hierarchicalBag.getEntityName2discriminatorValue().get("Regular_Employee"))
          .isEqualTo("reg_emp");
      assertThat(hierarchicalBag.getEntityName2discriminatorValue().get("Contract_Employee"))
          .isEqualTo("cont_emp");

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      VertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(4);
      assertThat(employeeVertexType).isNotNull();
      assertThat(regularEmployeeVertexType).isNotNull();
      assertThat(contractEmployeeVertexType).isNotNull();
      assertThat(residenceVertexType).isNotNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(3);

      assertThat(employeeVertexType.getPropertyByName("id")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(employeeVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(employeeVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(employeeVertexType.getPropertyByName("name")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(employeeVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(employeeVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(employeeVertexType.getPropertyByName("residence")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(employeeVertexType.getPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("residence").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(employeeVertexType.getPropertyByName("residence").isFromPrimaryKey()).isFalse();

      assertThat(regularEmployeeVertexType.getProperties().size()).isEqualTo(2);

      assertThat(regularEmployeeVertexType.getPropertyByName("salary")).isNotNull();
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getName())
          .isEqualTo("salary");
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getPropertyByName("bonus")).isNotNull();
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getName()).isEqualTo("bonus");
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey()).isFalse();

      assertThat(contractEmployeeVertexType.getProperties().size()).isEqualTo(2);

      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour")).isNotNull();
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getName())
          .isEqualTo("payPerHour");
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration")).isNotNull();
      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration").getName())
          .isEqualTo("contractDuration");
      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey())
          .isFalse();

      assertThat(residenceVertexType.getProperties().size()).isEqualTo(3);

      assertThat(residenceVertexType.getPropertyByName("id")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(residenceVertexType.getPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(residenceVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(residenceVertexType.getPropertyByName("city")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("city").getName()).isEqualTo("city");
      assertThat(residenceVertexType.getPropertyByName("city").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("city").getOrdinalPosition()).isEqualTo(2);
      assertThat(residenceVertexType.getPropertyByName("city").isFromPrimaryKey()).isFalse();

      assertThat(residenceVertexType.getPropertyByName("country")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("country").getName()).isEqualTo("country");
      assertThat(residenceVertexType.getPropertyByName("country").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("country").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(residenceVertexType.getPropertyByName("country").isFromPrimaryKey()).isFalse();

      // inherited properties check
      assertThat(employeeVertexType.getInheritedProperties().size()).isEqualTo(0);

      assertThat(regularEmployeeVertexType.getInheritedProperties().size()).isEqualTo(3);

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isTrue();

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("residence")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeVertexType
                  .getInheritedPropertyByName("residence")
                  .getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedProperties().size()).isEqualTo(3);

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isTrue();

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("residence")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType
                  .getInheritedPropertyByName("residence")
                  .getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey())
          .isFalse();

      assertThat(residenceVertexType.getInheritedProperties().size()).isEqualTo(0);

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasResidence");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasResidence");

      assertThat(regularEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(regularEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasResidence");

      assertThat(contractEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasResidence");

      // inheritance check
      assertThat(regularEmployeeVertexType.getParentType()).isEqualTo(employeeVertexType);
      assertThat(contractEmployeeVertexType.getParentType()).isEqualTo(employeeVertexType);
      assertThat(employeeVertexType.getParentType()).isNull();

      assertThat(regularEmployeeVertexType.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeVertexType.getInheritanceLevel()).isEqualTo(0);
    } catch (Exception e) {
      e.printStackTrace();
      fail("");
    } finally {
      try {
        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail("");
      }
    }
  }

  /*
   * Table per Subclass Inheritance (<joined-subclass> tag)
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-subclass )
   */

  @Test
  void TablePerSubclassInheritanceSyntax1() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residence =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, NAME varchar(256), RESIDENCE"
              + " varchar(256), primary key (ID), foreign key (RESIDENCE) references"
              + " RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding =
          "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, SALARY decimal(10,2),"
              + " BONUS decimal(10,0), primary key (EID), foreign key (EID) references"
              + " EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding =
          "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, PAY_PER_HOUR"
              + " decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key"
              + " (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper =
          new Hibernate2GraphMapper(
              dataSource,
              HibernateMapperTest.XML_TABLE_PER_SUBCLASS1,
              null,
              null,
              dbQueryEngine,
              dataTypeHandler,
              executionStrategy,
              nameResolver,
              statistics);

      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new JavaConventionNameResolver());

      /*
       *  Testing context information
       */

      assertThat(statistics.totalNumberOfEntities).isEqualTo(4);
      assertThat(statistics.builtEntities).isEqualTo(4);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(3);
      assertThat(statistics.builtRelationships).isEqualTo(3);

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(4);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(4);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(1);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(1);

      /*
       *  Testing built source db schema
       */

      Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      Entity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      Entity contractEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(4);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(3);
      assertThat(employeeEntity).isNotNull();
      assertThat(regularEmployeeEntity).isNotNull();
      assertThat(contractEmployeeEntity).isNotNull();
      assertThat(residenceEntity).isNotNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(3);

      assertThat(employeeEntity.getAttributeByName("ID")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(employeeEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(employeeEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(employeeEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("RESIDENCE")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getName()).isEqualTo("RESIDENCE");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition()).isEqualTo(3);
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getAttributes().size()).isEqualTo(2);

      assertThat(regularEmployeeEntity.getAttributeByName("SALARY")).isNotNull();
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getName()).isEqualTo("SALARY");
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName())
          .isEqualTo("REGULAR_EMPLOYEE");

      assertThat(regularEmployeeEntity.getAttributeByName("BONUS")).isNotNull();
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getName()).isEqualTo("BONUS");
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName())
          .isEqualTo("REGULAR_EMPLOYEE");

      assertThat(contractEmployeeEntity.getAttributes().size()).isEqualTo(2);

      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR")).isNotNull();
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName())
          .isEqualTo("PAY_PER_HOUR");
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              contractEmployeeEntity
                  .getAttributeByName("PAY_PER_HOUR")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("CONTRACT_EMPLOYEE");

      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION")).isNotNull();
      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName())
          .isEqualTo("CONTRACT_DURATION");
      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeEntity
                  .getAttributeByName("CONTRACT_DURATION")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("CONTRACT_EMPLOYEE");

      // inherited attributes check
      assertThat(employeeEntity.getInheritedAttributes().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getInheritedAttributes().size()).isEqualTo(3);

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("ID")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName())
          .isEqualTo("RESIDENCE");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("RESIDENCE")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributes().size()).isEqualTo(3);

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getName())
          .isEqualTo("ID");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("ID")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName())
          .isEqualTo("RESIDENCE");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("RESIDENCE")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      // primary key check
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size()).isEqualTo(1);
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("EID");
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("REGULAR_EMPLOYEE");

      assertThat(contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size())
          .isEqualTo(1);
      assertThat(contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("EID");
      assertThat(
              contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("CONTRACT_EMPLOYEE");

      // relationship, primary and foreign key check
      assertThat(regularEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(residenceEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(regularEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(2);
      assertThat(residenceEntity.getInCanonicalRelationships().size()).isEqualTo(1);

      assertThat(regularEmployeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(residenceEntity.getForeignKeys().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      Iterator<CanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getOutCanonicalRelationships().iterator();
      Iterator<CanonicalRelationship> itContEmp =
          contractEmployeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      CanonicalRelationship currentRegEmpRel = itRegEmp.next();
      CanonicalRelationship currentContEmpRel = itContEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("REGULAR_EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("CONTRACT_EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(employeeEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getForeignKey())
          .isEqualTo(regularEmployeeEntity.getForeignKeys().get(0));
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(employeeEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getForeignKey())
          .isEqualTo(contractEmployeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itRes =
          residenceEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentResRel = itRes.next();
      assertThat(currentResRel).isEqualTo(currentEmpRel);

      itEmp = employeeEntity.getInCanonicalRelationships().iterator();
      currentEmpRel = itEmp.next();
      assertThat(currentContEmpRel).isEqualTo(currentEmpRel);

      currentEmpRel = itEmp.next();
      assertThat(currentRegEmpRel).isEqualTo(currentEmpRel);

      // inherited relationships check
      assertThat(regularEmployeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritedOutCanonicalRelationships().size())
          .isEqualTo(1);
      assertThat(employeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(residenceEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);

      itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      currentRegEmpRel = itRegEmp.next();
      currentContEmpRel = itContEmp.next();
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentRegEmpRel.getFromColumns().get(0).getName()).isEqualTo("RESIDENCE");
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentContEmpRel.getFromColumns().get(0).getName()).isEqualTo("RESIDENCE");
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      assertThat(currentRegEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentRegEmpRel.getFromColumns().get(0).getName()).isEqualTo("RESIDENCE");

      // inheritance check
      assertThat(regularEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(contractEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(employeeEntity.getParentEntity()).isNull();
      assertThat(residenceEntity.getParentEntity()).isNull();

      assertThat(regularEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeEntity.getInheritanceLevel()).isEqualTo(0);
      assertThat(residenceEntity.getInheritanceLevel()).isEqualTo(0);

      // Hierarchical Bag check
      assertThat(mapper.getDataBaseSchema().getHierarchicalBags().size()).isEqualTo(1);
      HierarchicalBag hierarchicalBag = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      assertThat(hierarchicalBag.getInheritancePattern()).isEqualTo("table-per-type");

      assertThat(hierarchicalBag.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag.getDepth2entities().get(0).size()).isEqualTo(1);
      Iterator<Entity> it = hierarchicalBag.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag.getDepth2entities().get(1).size()).isEqualTo(2);
      it = hierarchicalBag.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("REGULAR_EMPLOYEE");
      assertThat(it.next().getName()).isEqualTo("CONTRACT_EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(employeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);
      assertThat(regularEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);
      assertThat(contractEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);

      assertThat(hierarchicalBag.getDiscriminatorColumn()).isNull();

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      VertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(4);
      assertThat(employeeVertexType).isNotNull();
      assertThat(regularEmployeeVertexType).isNotNull();
      assertThat(contractEmployeeVertexType).isNotNull();
      assertThat(residenceVertexType).isNotNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(3);

      assertThat(employeeVertexType.getPropertyByName("id")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(employeeVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(employeeVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(employeeVertexType.getPropertyByName("name")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(employeeVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(employeeVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(employeeVertexType.getPropertyByName("residence")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(employeeVertexType.getPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("residence").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(employeeVertexType.getPropertyByName("residence").isFromPrimaryKey()).isFalse();

      assertThat(regularEmployeeVertexType.getProperties().size()).isEqualTo(2);

      assertThat(regularEmployeeVertexType.getPropertyByName("salary")).isNotNull();
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getName())
          .isEqualTo("salary");
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getPropertyByName("bonus")).isNotNull();
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getName()).isEqualTo("bonus");
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey()).isFalse();

      assertThat(contractEmployeeVertexType.getProperties().size()).isEqualTo(2);

      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour")).isNotNull();
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getName())
          .isEqualTo("payPerHour");
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration")).isNotNull();
      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration").getName())
          .isEqualTo("contractDuration");
      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey())
          .isFalse();

      assertThat(residenceVertexType.getProperties().size()).isEqualTo(3);

      assertThat(residenceVertexType.getPropertyByName("id")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(residenceVertexType.getPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(residenceVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(residenceVertexType.getPropertyByName("city")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("city").getName()).isEqualTo("city");
      assertThat(residenceVertexType.getPropertyByName("city").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("city").getOrdinalPosition()).isEqualTo(2);
      assertThat(residenceVertexType.getPropertyByName("city").isFromPrimaryKey()).isFalse();

      assertThat(residenceVertexType.getPropertyByName("country")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("country").getName()).isEqualTo("country");
      assertThat(residenceVertexType.getPropertyByName("country").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("country").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(residenceVertexType.getPropertyByName("country").isFromPrimaryKey()).isFalse();

      // inherited properties check
      assertThat(employeeVertexType.getInheritedProperties().size()).isEqualTo(0);

      assertThat(regularEmployeeVertexType.getInheritedProperties().size()).isEqualTo(3);

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("residence")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeVertexType
                  .getInheritedPropertyByName("residence")
                  .getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedProperties().size()).isEqualTo(3);

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("residence")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType
                  .getInheritedPropertyByName("residence")
                  .getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey())
          .isFalse();

      assertThat(residenceVertexType.getInheritedProperties().size()).isEqualTo(0);

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasResidence");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasResidence");

      assertThat(regularEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(regularEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasResidence");

      assertThat(contractEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasResidence");

      // inheritance check
      assertThat(regularEmployeeVertexType.getParentType()).isEqualTo(employeeVertexType);
      assertThat(contractEmployeeVertexType.getParentType()).isEqualTo(employeeVertexType);
      assertThat(employeeVertexType.getParentType()).isNull();

      assertThat(regularEmployeeVertexType.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeVertexType.getInheritanceLevel()).isEqualTo(0);
    } catch (Exception e) {
      e.printStackTrace();
      fail("");
    } finally {
      try {
        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail("");
      }
    }
  }

  /*
   * Table per Subclass Inheritance (<subclass> <join> <join/> </subclass> tags)
   * 3 tables, one parent and 2 children ( http://www.javatpoint.com/table-per-subclass )
   */

  @Test
  void TablePerSubclassInheritanceSyntax2() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residence =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, NAME varchar(256), RESIDENCE"
              + " varchar(256), primary key (ID), foreign key (RESIDENCE) references"
              + " RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding =
          "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, SALARY decimal(10,2),"
              + " BONUS decimal(10,0), primary key (EID), foreign key (EID) references"
              + " EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding =
          "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, PAY_PER_HOUR"
              + " decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key"
              + " (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper =
          new Hibernate2GraphMapper(
              dataSource,
              HibernateMapperTest.XML_TABLE_PER_SUBCLASS2,
              null,
              null,
              dbQueryEngine,
              dataTypeHandler,
              executionStrategy,
              nameResolver,
              statistics);

      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new JavaConventionNameResolver());

      /*
       *  Testing context information
       */

      assertThat(statistics.totalNumberOfEntities).isEqualTo(4);
      assertThat(statistics.builtEntities).isEqualTo(4);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(3);
      assertThat(statistics.builtRelationships).isEqualTo(3);

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(4);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(4);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(1);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(1);

      /*
       *  Testing built source db schema
       */

      Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      Entity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      Entity contractEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(4);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(3);
      assertThat(employeeEntity).isNotNull();
      assertThat(regularEmployeeEntity).isNotNull();
      assertThat(contractEmployeeEntity).isNotNull();
      assertThat(residenceEntity).isNotNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(3);

      assertThat(employeeEntity.getAttributeByName("ID")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(employeeEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(employeeEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(employeeEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("RESIDENCE")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getName()).isEqualTo("RESIDENCE");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition()).isEqualTo(3);
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getAttributes().size()).isEqualTo(2);

      assertThat(regularEmployeeEntity.getAttributeByName("SALARY")).isNotNull();
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getName()).isEqualTo("SALARY");
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName())
          .isEqualTo("REGULAR_EMPLOYEE");

      assertThat(regularEmployeeEntity.getAttributeByName("BONUS")).isNotNull();
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getName()).isEqualTo("BONUS");
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName())
          .isEqualTo("REGULAR_EMPLOYEE");

      assertThat(contractEmployeeEntity.getAttributes().size()).isEqualTo(2);

      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR")).isNotNull();
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName())
          .isEqualTo("PAY_PER_HOUR");
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              contractEmployeeEntity
                  .getAttributeByName("PAY_PER_HOUR")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("CONTRACT_EMPLOYEE");

      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION")).isNotNull();
      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName())
          .isEqualTo("CONTRACT_DURATION");
      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeEntity
                  .getAttributeByName("CONTRACT_DURATION")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("CONTRACT_EMPLOYEE");

      // inherited attributes check
      assertThat(employeeEntity.getInheritedAttributes().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getInheritedAttributes().size()).isEqualTo(3);

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("ID")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName())
          .isEqualTo("RESIDENCE");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("RESIDENCE")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributes().size()).isEqualTo(3);

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getName())
          .isEqualTo("ID");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("ID")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName())
          .isEqualTo("RESIDENCE");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("RESIDENCE")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      // primary key check
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size()).isEqualTo(1);
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("EID");
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("REGULAR_EMPLOYEE");

      assertThat(contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size())
          .isEqualTo(1);
      assertThat(contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("EID");
      assertThat(
              contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("CONTRACT_EMPLOYEE");

      // relationship, primary and foreign key check
      assertThat(regularEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(residenceEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(regularEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(2);
      assertThat(residenceEntity.getInCanonicalRelationships().size()).isEqualTo(1);

      assertThat(regularEmployeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(residenceEntity.getForeignKeys().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      Iterator<CanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getOutCanonicalRelationships().iterator();
      Iterator<CanonicalRelationship> itContEmp =
          contractEmployeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      CanonicalRelationship currentRegEmpRel = itRegEmp.next();
      CanonicalRelationship currentContEmpRel = itContEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("REGULAR_EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("CONTRACT_EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(employeeEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getForeignKey())
          .isEqualTo(regularEmployeeEntity.getForeignKeys().get(0));
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(employeeEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getForeignKey())
          .isEqualTo(contractEmployeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itRes =
          residenceEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentResRel = itRes.next();
      assertThat(currentResRel).isEqualTo(currentEmpRel);

      itEmp = employeeEntity.getInCanonicalRelationships().iterator();
      currentEmpRel = itEmp.next();
      assertThat(currentContEmpRel).isEqualTo(currentEmpRel);

      currentEmpRel = itEmp.next();
      assertThat(currentRegEmpRel).isEqualTo(currentEmpRel);

      // inherited relationships check
      assertThat(regularEmployeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritedOutCanonicalRelationships().size())
          .isEqualTo(1);
      assertThat(employeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(residenceEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);

      itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      currentRegEmpRel = itRegEmp.next();
      currentContEmpRel = itContEmp.next();
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentRegEmpRel.getFromColumns().get(0).getName()).isEqualTo("RESIDENCE");
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentContEmpRel.getFromColumns().get(0).getName()).isEqualTo("RESIDENCE");
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      // inheritance check
      assertThat(regularEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(contractEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(employeeEntity.getParentEntity()).isNull();
      assertThat(residenceEntity.getParentEntity()).isNull();

      assertThat(regularEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeEntity.getInheritanceLevel()).isEqualTo(0);
      assertThat(residenceEntity.getInheritanceLevel()).isEqualTo(0);

      // Hierarchical Bag check
      assertThat(mapper.getDataBaseSchema().getHierarchicalBags().size()).isEqualTo(1);
      HierarchicalBag hierarchicalBag = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      assertThat(hierarchicalBag.getInheritancePattern()).isEqualTo("table-per-type");

      assertThat(hierarchicalBag.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag.getDepth2entities().get(0).size()).isEqualTo(1);
      Iterator<Entity> it = hierarchicalBag.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag.getDepth2entities().get(1).size()).isEqualTo(2);
      it = hierarchicalBag.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("REGULAR_EMPLOYEE");
      assertThat(it.next().getName()).isEqualTo("CONTRACT_EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(employeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);
      assertThat(regularEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);
      assertThat(contractEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);

      assertThat(hierarchicalBag.getDiscriminatorColumn()).isNotNull();
      assertThat(hierarchicalBag.getDiscriminatorColumn()).isEqualTo("employee_type");

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      VertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(4);
      assertThat(employeeVertexType).isNotNull();
      assertThat(regularEmployeeVertexType).isNotNull();
      assertThat(contractEmployeeVertexType).isNotNull();
      assertThat(residenceVertexType).isNotNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(3);

      assertThat(employeeVertexType.getPropertyByName("id")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(employeeVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(employeeVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(employeeVertexType.getPropertyByName("name")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(employeeVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(employeeVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(employeeVertexType.getPropertyByName("residence")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(employeeVertexType.getPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("residence").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(employeeVertexType.getPropertyByName("residence").isFromPrimaryKey()).isFalse();

      assertThat(regularEmployeeVertexType.getProperties().size()).isEqualTo(2);

      assertThat(regularEmployeeVertexType.getPropertyByName("salary")).isNotNull();
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getName())
          .isEqualTo("salary");
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getPropertyByName("bonus")).isNotNull();
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getName()).isEqualTo("bonus");
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey()).isFalse();

      assertThat(contractEmployeeVertexType.getProperties().size()).isEqualTo(2);

      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour")).isNotNull();
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getName())
          .isEqualTo("payPerHour");
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration")).isNotNull();
      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration").getName())
          .isEqualTo("contractDuration");
      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey())
          .isFalse();

      assertThat(residenceVertexType.getProperties().size()).isEqualTo(3);

      assertThat(residenceVertexType.getPropertyByName("id")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(residenceVertexType.getPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(residenceVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(residenceVertexType.getPropertyByName("city")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("city").getName()).isEqualTo("city");
      assertThat(residenceVertexType.getPropertyByName("city").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("city").getOrdinalPosition()).isEqualTo(2);
      assertThat(residenceVertexType.getPropertyByName("city").isFromPrimaryKey()).isFalse();

      assertThat(residenceVertexType.getPropertyByName("country")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("country").getName()).isEqualTo("country");
      assertThat(residenceVertexType.getPropertyByName("country").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("country").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(residenceVertexType.getPropertyByName("country").isFromPrimaryKey()).isFalse();

      // inherited properties check
      assertThat(employeeVertexType.getInheritedProperties().size()).isEqualTo(0);

      assertThat(regularEmployeeVertexType.getInheritedProperties().size()).isEqualTo(3);

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("residence")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeVertexType
                  .getInheritedPropertyByName("residence")
                  .getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedProperties().size()).isEqualTo(3);

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("residence")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType
                  .getInheritedPropertyByName("residence")
                  .getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey())
          .isFalse();

      assertThat(residenceVertexType.getInheritedProperties().size()).isEqualTo(0);

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasResidence");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasResidence");

      assertThat(regularEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(regularEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasResidence");

      assertThat(contractEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasResidence");

      // inheritance check
      assertThat(regularEmployeeVertexType.getParentType()).isEqualTo(employeeVertexType);
      assertThat(contractEmployeeVertexType.getParentType()).isEqualTo(employeeVertexType);
      assertThat(employeeVertexType.getParentType()).isNull();

      assertThat(regularEmployeeVertexType.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeVertexType.getInheritanceLevel()).isEqualTo(0);
    } catch (Exception e) {
      e.printStackTrace();
      fail("");
    } finally {
      try {
        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail("");
      }
    }
  }

  /*
   * Table per Concrete Class Inheritance (<union-subclass> tag)
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-concrete-class )
   */

  @Test
  void TablePerConcreteClassInheritance() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residence =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, NAME varchar(256), RESIDENCE"
              + " varchar(256), primary key (ID), foreign key (RESIDENCE) references"
              + " RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding =
          "create memory table REGULAR_EMPLOYEE (ID varchar(256) not null, NAME varchar(256),"
              + " RESIDENCE varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), primary key"
              + " (ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding =
          "create memory table CONTRACT_EMPLOYEE (ID varchar(256) not null, NAME varchar(256),"
              + " RESIDENCE varchar(256), PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION"
              + " varchar(256), primary key (ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper =
          new Hibernate2GraphMapper(
              dataSource,
              HibernateMapperTest.XML_TABLE_PER_CONCRETE_CLASS,
              null,
              null,
              dbQueryEngine,
              dataTypeHandler,
              executionStrategy,
              nameResolver,
              statistics);

      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new JavaConventionNameResolver());

      /*
       *  Testing context information
       */

      assertThat(statistics.totalNumberOfEntities).isEqualTo(4);
      assertThat(statistics.builtEntities).isEqualTo(4);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(1);
      assertThat(statistics.builtRelationships).isEqualTo(1);

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(4);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(4);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(1);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(1);

      /*
       *  Testing built source db schema
       */

      Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      Entity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      Entity contractEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(4);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity).isNotNull();
      assertThat(regularEmployeeEntity).isNotNull();
      assertThat(contractEmployeeEntity).isNotNull();
      assertThat(residenceEntity).isNotNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(3);

      assertThat(employeeEntity.getAttributeByName("ID")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(employeeEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(employeeEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(employeeEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("RESIDENCE")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getName()).isEqualTo("RESIDENCE");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition()).isEqualTo(3);
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getAttributes().size()).isEqualTo(2);

      assertThat(regularEmployeeEntity.getAttributeByName("SALARY")).isNotNull();
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getName()).isEqualTo("SALARY");
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName())
          .isEqualTo("REGULAR_EMPLOYEE");

      assertThat(regularEmployeeEntity.getAttributeByName("BONUS")).isNotNull();
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getName()).isEqualTo("BONUS");
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName())
          .isEqualTo("REGULAR_EMPLOYEE");

      assertThat(contractEmployeeEntity.getAttributes().size()).isEqualTo(2);

      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR")).isNotNull();
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName())
          .isEqualTo("PAY_PER_HOUR");
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType())
          .isEqualTo("DECIMAL");
      assertThat(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              contractEmployeeEntity
                  .getAttributeByName("PAY_PER_HOUR")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("CONTRACT_EMPLOYEE");

      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION")).isNotNull();
      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName())
          .isEqualTo("CONTRACT_DURATION");
      assertThat(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeEntity
                  .getAttributeByName("CONTRACT_DURATION")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("CONTRACT_EMPLOYEE");

      // inherited attributes check
      assertThat(employeeEntity.getInheritedAttributes().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getInheritedAttributes().size()).isEqualTo(3);

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("ID")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName())
          .isEqualTo("RESIDENCE");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("RESIDENCE")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributes().size()).isEqualTo(3);

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getName())
          .isEqualTo("ID");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("ID")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName())
          .isEqualTo("RESIDENCE");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("RESIDENCE")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      // primary key check (not "inherited")
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size()).isEqualTo(1);
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("ID");
      assertThat(regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("REGULAR_EMPLOYEE");

      assertThat(contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size())
          .isEqualTo(1);
      assertThat(contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("ID");
      assertThat(
              contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("CONTRACT_EMPLOYEE");

      // relationship, primary and foreign key check
      assertThat(regularEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(residenceEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(regularEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(residenceEntity.getInCanonicalRelationships().size()).isEqualTo(1);

      assertThat(regularEmployeeEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(residenceEntity.getForeignKeys().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itRes =
          residenceEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentResRel = itRes.next();
      assertThat(currentResRel).isEqualTo(currentEmpRel);

      // inherited relationships check
      assertThat(regularEmployeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritedOutCanonicalRelationships().size())
          .isEqualTo(1);
      assertThat(employeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(residenceEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      Iterator<CanonicalRelationship> itContEmp =
          contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      CanonicalRelationship currentRegEmpRel = itRegEmp.next();
      CanonicalRelationship currentContEmpRel = itContEmp.next();
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("RESIDENCE");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentRegEmpRel.getFromColumns().get(0).getName()).isEqualTo("RESIDENCE");
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(residenceEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentContEmpRel.getFromColumns().get(0).getName()).isEqualTo("RESIDENCE");
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      // inheritance check
      assertThat(regularEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(contractEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(employeeEntity.getParentEntity()).isNull();
      assertThat(residenceEntity.getParentEntity()).isNull();

      assertThat(regularEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeEntity.getInheritanceLevel()).isEqualTo(0);
      assertThat(residenceEntity.getInheritanceLevel()).isEqualTo(0);

      // Hierarchical Bag check
      assertThat(mapper.getDataBaseSchema().getHierarchicalBags().size()).isEqualTo(1);
      HierarchicalBag hierarchicalBag = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      assertThat(hierarchicalBag.getInheritancePattern()).isEqualTo("table-per-concrete-type");

      assertThat(hierarchicalBag.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag.getDepth2entities().get(0).size()).isEqualTo(1);
      Iterator<Entity> it = hierarchicalBag.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag.getDepth2entities().get(1).size()).isEqualTo(2);
      it = hierarchicalBag.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("REGULAR_EMPLOYEE");
      assertThat(it.next().getName()).isEqualTo("CONTRACT_EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(employeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);
      assertThat(regularEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);
      assertThat(contractEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag);

      assertThat(hierarchicalBag.getDiscriminatorColumn()).isNull();

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      VertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(4);
      assertThat(employeeVertexType).isNotNull();
      assertThat(regularEmployeeVertexType).isNotNull();
      assertThat(contractEmployeeVertexType).isNotNull();
      assertThat(residenceVertexType).isNotNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(3);

      assertThat(employeeVertexType.getPropertyByName("id")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(employeeVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(employeeVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(employeeVertexType.getPropertyByName("name")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(employeeVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(employeeVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(employeeVertexType.getPropertyByName("residence")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(employeeVertexType.getPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("residence").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(employeeVertexType.getPropertyByName("residence").isFromPrimaryKey()).isFalse();

      assertThat(regularEmployeeVertexType.getProperties().size()).isEqualTo(2);

      assertThat(regularEmployeeVertexType.getPropertyByName("salary")).isNotNull();
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getName())
          .isEqualTo("salary");
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getPropertyByName("bonus")).isNotNull();
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getName()).isEqualTo("bonus");
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey()).isFalse();

      assertThat(contractEmployeeVertexType.getProperties().size()).isEqualTo(2);

      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour")).isNotNull();
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getName())
          .isEqualTo("payPerHour");
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration")).isNotNull();
      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration").getName())
          .isEqualTo("contractDuration");
      assertThat(contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey())
          .isFalse();

      assertThat(residenceVertexType.getProperties().size()).isEqualTo(3);

      assertThat(residenceVertexType.getPropertyByName("id")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(residenceVertexType.getPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(residenceVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(residenceVertexType.getPropertyByName("city")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("city").getName()).isEqualTo("city");
      assertThat(residenceVertexType.getPropertyByName("city").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("city").getOrdinalPosition()).isEqualTo(2);
      assertThat(residenceVertexType.getPropertyByName("city").isFromPrimaryKey()).isFalse();

      assertThat(residenceVertexType.getPropertyByName("country")).isNotNull();
      assertThat(residenceVertexType.getPropertyByName("country").getName()).isEqualTo("country");
      assertThat(residenceVertexType.getPropertyByName("country").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(residenceVertexType.getPropertyByName("country").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(residenceVertexType.getPropertyByName("country").isFromPrimaryKey()).isFalse();

      // inherited properties check
      assertThat(employeeVertexType.getInheritedProperties().size()).isEqualTo(0);

      assertThat(regularEmployeeVertexType.getInheritedProperties().size()).isEqualTo(3);

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isTrue();

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("residence")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeVertexType
                  .getInheritedPropertyByName("residence")
                  .getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedProperties().size()).isEqualTo(3);

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isTrue();

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("residence")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType
                  .getInheritedPropertyByName("residence")
                  .getOrdinalPosition())
          .isEqualTo(3);
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey())
          .isFalse();

      assertThat(residenceVertexType.getInheritedProperties().size()).isEqualTo(0);

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasResidence");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasResidence");

      assertThat(regularEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(regularEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasResidence");

      assertThat(contractEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasResidence");

      // inheritance check
      assertThat(regularEmployeeVertexType.getParentType()).isEqualTo(employeeVertexType);
      assertThat(contractEmployeeVertexType.getParentType()).isEqualTo(employeeVertexType);
      assertThat(employeeVertexType.getParentType()).isNull();

      assertThat(regularEmployeeVertexType.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeVertexType.getInheritanceLevel()).isEqualTo(0);
    } catch (Exception e) {
      e.printStackTrace();
      fail("", e);
    } finally {
      try {
        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail("", e);
      }
    }
  }
}
