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

package com.arcadeanalytics.provider.rdbms.filter;

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
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.classmapper.EVClassMapper;
import com.arcadeanalytics.provider.rdbms.model.dbschema.CanonicalRelationship;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.HierarchicalBag;
import com.arcadeanalytics.provider.rdbms.model.dbschema.SourceDatabaseInfo;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.EdgeType;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.VertexType;
import com.arcadeanalytics.provider.rdbms.nameresolver.JavaConventionNameResolver;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
import com.arcadeanalytics.provider.rdbms.persistence.handler.HSQLDBDataTypeHandler;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Gabriele Ponzi
 */
class FilterTableMappingTest {

  private static final String XML_TABLE_PER_CLASS =
      "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerClassHierarchyImportTest.xml";
  private static final String XML_TABLE_PER_SUBCLASS1 =
      "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerSubclassImportTest1.xml";
  private static final String XML_TABLE_PER_SUBCLASS2 =
      "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerSubclassImportTest2.xml";
  private static final String XML_TABLE_PER_CONCRETE_CLASS =
      "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerConcreteClassImportTest.xml";
  private ER2GraphMapper mapper;
  private DBQueryEngine dbQueryEngine;
  private String driver = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private SourceDatabaseInfo sourceDBInfo;
  private DataSourceInfo dataSource;
  private String executionStrategy;
  private NameResolver nameResolver;
  private DBMSDataTypeHandler dataTypeHandler;
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
   * Filtering out a table through include-tables (without inheritance).
   */ @Test
  void filterOutThroughIncludeWithNoInheritance() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String countryTableBuilding =
          "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID), foreign key (COUNTRY) references COUNTRY(ID))";
      st.execute(residenceTableBuilding);

      String managerTableBuilding =
          "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT"
              + " varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, NAME varchar(256), SALARY"
              + " decimal(10,2), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID),"
              + " foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER)"
              + " references MANAGER(ID))";
      st.execute(employeeTableBuilding);

      // Records Inserting

      String countryFilling =
          "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
              + "('R001','Rome','C001'),"
              + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling =
          "insert into MANAGER (ID,NAME,PROJECT) values (" + "('M001','Bill Right','New World'))";
      st.execute(managerFilling);

      String employeeFilling =
          "insert into EMPLOYEE (ID,NAME,SALARY,RESIDENCE,MANAGER) values ("
              + "('E001','John Black',1500.00,'R001',null),"
              + "('E002','Andrew Brown','1000.00','R001','M001'),"
              + "('E003','Jack Johnson',2000.00,'R002',null))";
      st.execute(employeeFilling);

      List<String> includedTables = new ArrayList<String>();
      includedTables.add("COUNTRY");
      includedTables.add("MANAGER");
      includedTables.add("EMPLOYEE");

      this.mapper =
          new ER2GraphMapper(
              dataSource,
              includedTables,
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

      assertThat(statistics.totalNumberOfEntities).isEqualTo(3);
      assertThat(statistics.builtEntities).isEqualTo(3);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(1);
      assertThat(statistics.builtRelationships).isEqualTo(1);

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(3);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(3);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(1);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(1);

      /*
       *  Testing built source db schema
       */

      Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
      Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(3);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity).isNotNull();
      assertThat(countryEntity).isNotNull();
      assertThat(managerEntity).isNotNull();
      assertThat(residenceEntity).isNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(5);

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

      assertThat(employeeEntity.getAttributeByName("SALARY")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("SALARY").getName()).isEqualTo("SALARY");
      assertThat(employeeEntity.getAttributeByName("SALARY").getDataType()).isEqualTo("DECIMAL");
      assertThat(employeeEntity.getAttributeByName("SALARY").getOrdinalPosition()).isEqualTo(3);
      assertThat(employeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("RESIDENCE")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getName()).isEqualTo("RESIDENCE");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("MANAGER")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("MANAGER").getName()).isEqualTo("MANAGER");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition()).isEqualTo(5);
      assertThat(employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(countryEntity.getAttributes().size()).isEqualTo(3);

      assertThat(countryEntity.getAttributeByName("ID")).isNotNull();
      assertThat(countryEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(countryEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(countryEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(countryEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("CONTINENT")).isNotNull();
      assertThat(countryEntity.getAttributeByName("CONTINENT").getName()).isEqualTo("CONTINENT");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition()).isEqualTo(3);
      assertThat(countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(managerEntity.getAttributes().size()).isEqualTo(3);

      assertThat(managerEntity.getAttributeByName("ID")).isNotNull();
      assertThat(managerEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(managerEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(managerEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(managerEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(managerEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(managerEntity.getAttributeByName("PROJECT")).isNotNull();
      assertThat(managerEntity.getAttributeByName("PROJECT").getName()).isEqualTo("PROJECT");
      assertThat(managerEntity.getAttributeByName("PROJECT").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("PROJECT").getOrdinalPosition()).isEqualTo(3);
      assertThat(managerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      // relationship, primary and foreign key check
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(managerEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(countryEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(managerEntity.getInCanonicalRelationships().size()).isEqualTo(1);
      assertThat(countryEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itManager =
          managerEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentManRel = itManager.next();
      assertThat(currentManRel).isEqualTo(currentEmpRel);

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
      VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(3);
      assertThat(employeeVertexType).isNotNull();
      assertThat(countryVertexType).isNotNull();
      assertThat(managerVertexType).isNotNull();
      assertThat(residenceVertexType).isNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(5);

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

      assertThat(employeeVertexType.getPropertyByName("salary")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("salary").getName()).isEqualTo("salary");
      assertThat(employeeVertexType.getPropertyByName("salary").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(employeeVertexType.getPropertyByName("salary").getOrdinalPosition()).isEqualTo(3);
      assertThat(employeeVertexType.getPropertyByName("salary").isFromPrimaryKey()).isFalse();

      assertThat(employeeVertexType.getPropertyByName("residence")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(employeeVertexType.getPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("residence").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(employeeVertexType.getPropertyByName("residence").isFromPrimaryKey()).isFalse();

      assertThat(employeeVertexType.getPropertyByName("manager")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("manager").getName()).isEqualTo("manager");
      assertThat(employeeVertexType.getPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("manager").getOrdinalPosition()).isEqualTo(5);
      assertThat(employeeVertexType.getPropertyByName("manager").isFromPrimaryKey()).isFalse();

      assertThat(countryVertexType.getProperties().size()).isEqualTo(3);

      assertThat(countryVertexType.getPropertyByName("id")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(countryVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(countryVertexType.getPropertyByName("name")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(countryVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(countryVertexType.getPropertyByName("continent")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("continent").getName()).isEqualTo("continent");
      assertThat(countryVertexType.getPropertyByName("continent").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("continent").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(countryVertexType.getPropertyByName("continent").isFromPrimaryKey()).isFalse();

      assertThat(managerVertexType.getProperties().size()).isEqualTo(3);

      assertThat(managerVertexType.getPropertyByName("id")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(managerVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(managerVertexType.getPropertyByName("name")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(managerVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(managerVertexType.getPropertyByName("project")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("project").getName()).isEqualTo("project");
      assertThat(managerVertexType.getPropertyByName("project").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("project").getOrdinalPosition()).isEqualTo(3);
      assertThat(managerVertexType.getPropertyByName("project").isFromPrimaryKey()).isFalse();

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasManager");

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(3);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(3);

      assertThat(mapper.getEVClassMappersByVertex(employeeVertexType).size()).isEqualTo(1);
      EVClassMapper employeeClassMapper =
          mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).get(0))
          .isEqualTo(employeeClassMapper);
      assertThat(employeeEntity).isEqualTo(employeeClassMapper.getEntity());
      assertThat(employeeVertexType).isEqualTo(employeeClassMapper.getVertexType());

      assertThat(employeeClassMapper.getAttribute2property().size()).isEqualTo(5);
      assertThat(employeeClassMapper.getProperty2attribute().size()).isEqualTo(5);
      assertThat(employeeClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(employeeClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(employeeClassMapper.getAttribute2property().get("SALARY")).isEqualTo("salary");
      assertThat(employeeClassMapper.getAttribute2property().get("RESIDENCE"))
          .isEqualTo("residence");
      assertThat(employeeClassMapper.getAttribute2property().get("MANAGER")).isEqualTo("manager");
      assertThat(employeeClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(employeeClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(employeeClassMapper.getProperty2attribute().get("salary")).isEqualTo("SALARY");
      assertThat(employeeClassMapper.getProperty2attribute().get("residence"))
          .isEqualTo("RESIDENCE");
      assertThat(employeeClassMapper.getProperty2attribute().get("manager")).isEqualTo("MANAGER");

      assertThat(mapper.getEVClassMappersByVertex(countryVertexType).size()).isEqualTo(1);
      EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).get(0))
          .isEqualTo(countryClassMapper);
      assertThat(countryEntity).isEqualTo(countryClassMapper.getEntity());
      assertThat(countryVertexType).isEqualTo(countryClassMapper.getVertexType());

      assertThat(countryClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(countryClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(countryClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(countryClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(countryClassMapper.getAttribute2property().get("CONTINENT"))
          .isEqualTo("continent");
      assertThat(countryClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(countryClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(countryClassMapper.getProperty2attribute().get("continent"))
          .isEqualTo("CONTINENT");

      assertThat(mapper.getEVClassMappersByVertex(managerVertexType).size()).isEqualTo(1);
      EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).get(0))
          .isEqualTo(managerClassMapper);
      assertThat(managerEntity).isEqualTo(managerClassMapper.getEntity());
      assertThat(managerVertexType).isEqualTo(managerClassMapper.getVertexType());

      assertThat(managerClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(managerClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(managerClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(managerClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(managerClassMapper.getAttribute2property().get("PROJECT")).isEqualTo("project");
      assertThat(managerClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(managerClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(managerClassMapper.getProperty2attribute().get("project")).isEqualTo("PROJECT");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> it = employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasManagerRelationship = it.next();
      assertThat(it.hasNext()).isFalse();

      EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);
      assertThat(mapper.getRelationship2edgeType().get(hasManagerRelationship))
          .isEqualTo(hasManagerEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(hasManagerEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(hasManagerEdgeType)
                  .contains(hasManagerRelationship))
          .isTrue();

      // JoinVertexes-AggregatorEdges Mapping

      assertThat(mapper.getJoinVertex2aggregatorEdges().size()).isEqualTo(0);
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
   * Filtering out a table through exclude-tables (without inheritance).
   */ @Test
  void filterOutThroughExcludeWithNoInheritance() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String countryTableBuilding =
          "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID), foreign key (COUNTRY) references COUNTRY(ID))";
      st.execute(residenceTableBuilding);

      String managerTableBuilding =
          "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT"
              + " varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, NAME varchar(256), SALARY"
              + " decimal(10,2), RESIDENCE varchar(256), MANAGER varchar(256), primary key (id),"
              + " foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER)"
              + " references MANAGER(ID))";
      st.execute(employeeTableBuilding);

      // Records Inserting

      String countryFilling =
          "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
              + "('R001','Rome','C001'),"
              + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling =
          "insert into MANAGER (ID,NAME,PROJECT) values (" + "('M001','Bill Right','New World'))";
      st.execute(managerFilling);

      String employeeFilling =
          "insert into EMPLOYEE (ID,NAME,SALARY,RESIDENCE,MANAGER) values ("
              + "('E001','John Black',1500.00,'R001',null),"
              + "('E002','Andrew Brown','1000.00','R001','M001'),"
              + "('E003','Jack Johnson',2000.00,'R002',null))";
      st.execute(employeeFilling);

      List<String> excludedTables = new ArrayList<String>();
      excludedTables.add("RESIDENCE");

      this.mapper =
          new ER2GraphMapper(
              dataSource,
              null,
              excludedTables,
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

      assertThat(statistics.totalNumberOfEntities).isEqualTo(3);
      assertThat(statistics.builtEntities).isEqualTo(3);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(1);
      assertThat(statistics.builtRelationships).isEqualTo(1);

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(3);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(3);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(1);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(1);

      /*
       *  Testing built source db schema
       */

      Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
      Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(3);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity).isNotNull();
      assertThat(countryEntity).isNotNull();
      assertThat(managerEntity).isNotNull();
      assertThat(residenceEntity).isNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(5);

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

      assertThat(employeeEntity.getAttributeByName("SALARY")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("SALARY").getName()).isEqualTo("SALARY");
      assertThat(employeeEntity.getAttributeByName("SALARY").getDataType()).isEqualTo("DECIMAL");
      assertThat(employeeEntity.getAttributeByName("SALARY").getOrdinalPosition()).isEqualTo(3);
      assertThat(employeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("RESIDENCE")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getName()).isEqualTo("RESIDENCE");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(employeeEntity.getAttributeByName("MANAGER")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("MANAGER").getName()).isEqualTo("MANAGER");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition()).isEqualTo(5);
      assertThat(employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName())
          .isEqualTo("EMPLOYEE");

      assertThat(countryEntity.getAttributes().size()).isEqualTo(3);

      assertThat(countryEntity.getAttributeByName("ID")).isNotNull();
      assertThat(countryEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(countryEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(countryEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(countryEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("CONTINENT")).isNotNull();
      assertThat(countryEntity.getAttributeByName("CONTINENT").getName()).isEqualTo("CONTINENT");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition()).isEqualTo(3);
      assertThat(countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(managerEntity.getAttributes().size()).isEqualTo(3);

      assertThat(managerEntity.getAttributeByName("ID")).isNotNull();
      assertThat(managerEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(managerEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(managerEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(managerEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(managerEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(managerEntity.getAttributeByName("PROJECT")).isNotNull();
      assertThat(managerEntity.getAttributeByName("PROJECT").getName()).isEqualTo("PROJECT");
      assertThat(managerEntity.getAttributeByName("PROJECT").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("PROJECT").getOrdinalPosition()).isEqualTo(3);
      assertThat(managerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      // relationship, primary and foreign key check
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(managerEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(managerEntity.getInCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itManager =
          managerEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentManRel = itManager.next();
      assertThat(currentManRel).isEqualTo(currentEmpRel);

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
      VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(3);
      assertThat(employeeVertexType).isNotNull();
      assertThat(countryVertexType).isNotNull();
      assertThat(managerVertexType).isNotNull();
      assertThat(residenceVertexType).isNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(5);

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

      assertThat(employeeVertexType.getPropertyByName("salary")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("salary").getName()).isEqualTo("salary");
      assertThat(employeeVertexType.getPropertyByName("salary").getOriginalType())
          .isEqualTo("DECIMAL");
      assertThat(employeeVertexType.getPropertyByName("salary").getOrdinalPosition()).isEqualTo(3);
      assertThat(employeeVertexType.getPropertyByName("salary").isFromPrimaryKey()).isFalse();

      assertThat(employeeVertexType.getPropertyByName("residence")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("residence").getName())
          .isEqualTo("residence");
      assertThat(employeeVertexType.getPropertyByName("residence").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("residence").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(employeeVertexType.getPropertyByName("residence").isFromPrimaryKey()).isFalse();

      assertThat(employeeVertexType.getPropertyByName("manager")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("manager").getName()).isEqualTo("manager");
      assertThat(employeeVertexType.getPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("manager").getOrdinalPosition()).isEqualTo(5);
      assertThat(employeeVertexType.getPropertyByName("manager").isFromPrimaryKey()).isFalse();

      assertThat(countryVertexType.getProperties().size()).isEqualTo(3);

      assertThat(countryVertexType.getPropertyByName("id")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(countryVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(countryVertexType.getPropertyByName("name")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(countryVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(countryVertexType.getPropertyByName("continent")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("continent").getName()).isEqualTo("continent");
      assertThat(countryVertexType.getPropertyByName("continent").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("continent").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(countryVertexType.getPropertyByName("continent").isFromPrimaryKey()).isFalse();

      assertThat(managerVertexType.getProperties().size()).isEqualTo(3);

      assertThat(managerVertexType.getPropertyByName("id")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(managerVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(managerVertexType.getPropertyByName("name")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(managerVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(managerVertexType.getPropertyByName("project")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("project").getName()).isEqualTo("project");
      assertThat(managerVertexType.getPropertyByName("project").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("project").getOrdinalPosition()).isEqualTo(3);
      assertThat(managerVertexType.getPropertyByName("project").isFromPrimaryKey()).isFalse();

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasManager");

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(3);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(3);

      assertThat(mapper.getEVClassMappersByVertex(employeeVertexType).size()).isEqualTo(1);
      EVClassMapper employeeClassMapper =
          mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).get(0))
          .isEqualTo(employeeClassMapper);
      assertThat(employeeEntity).isEqualTo(employeeClassMapper.getEntity());
      assertThat(employeeVertexType).isEqualTo(employeeClassMapper.getVertexType());

      assertThat(employeeClassMapper.getAttribute2property().size()).isEqualTo(5);
      assertThat(employeeClassMapper.getProperty2attribute().size()).isEqualTo(5);
      assertThat(employeeClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(employeeClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(employeeClassMapper.getAttribute2property().get("SALARY")).isEqualTo("salary");
      assertThat(employeeClassMapper.getAttribute2property().get("RESIDENCE"))
          .isEqualTo("residence");
      assertThat(employeeClassMapper.getAttribute2property().get("MANAGER")).isEqualTo("manager");
      assertThat(employeeClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(employeeClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(employeeClassMapper.getProperty2attribute().get("salary")).isEqualTo("SALARY");
      assertThat(employeeClassMapper.getProperty2attribute().get("residence"))
          .isEqualTo("RESIDENCE");
      assertThat(employeeClassMapper.getProperty2attribute().get("manager")).isEqualTo("MANAGER");

      assertThat(mapper.getEVClassMappersByVertex(countryVertexType).size()).isEqualTo(1);
      EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).get(0))
          .isEqualTo(countryClassMapper);
      assertThat(countryEntity).isEqualTo(countryClassMapper.getEntity());
      assertThat(countryVertexType).isEqualTo(countryClassMapper.getVertexType());

      assertThat(countryClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(countryClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(countryClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(countryClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(countryClassMapper.getAttribute2property().get("CONTINENT"))
          .isEqualTo("continent");
      assertThat(countryClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(countryClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(countryClassMapper.getProperty2attribute().get("continent"))
          .isEqualTo("CONTINENT");

      assertThat(mapper.getEVClassMappersByVertex(managerVertexType).size()).isEqualTo(1);
      EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).get(0))
          .isEqualTo(managerClassMapper);
      assertThat(managerEntity).isEqualTo(managerClassMapper.getEntity());
      assertThat(managerVertexType).isEqualTo(managerClassMapper.getVertexType());

      assertThat(managerClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(managerClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(managerClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(managerClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(managerClassMapper.getAttribute2property().get("PROJECT")).isEqualTo("project");
      assertThat(managerClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(managerClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(managerClassMapper.getProperty2attribute().get("project")).isEqualTo("PROJECT");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> it = employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasManagerRelationship = it.next();
      assertThat(it.hasNext()).isFalse();

      EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);
      assertThat(mapper.getRelationship2edgeType().get(hasManagerRelationship))
          .isEqualTo(hasManagerEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(hasManagerEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(hasManagerEdgeType)
                  .contains(hasManagerRelationship))
          .isTrue();

      // JoinVertexes-AggregatorEdges Mapping

      assertThat(mapper.getJoinVertex2aggregatorEdges().size()).isEqualTo(0);
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
   * Filtering out a table through include-tables (with Table per Hierarchy inheritance).
   */ @Test
  void filterOutThroughIncludeWithTablePerHierarchyInheritance() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String countryTableBuilding =
          "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID), foreign key (COUNTRY) references COUNTRY(ID))";
      st.execute(residenceTableBuilding);

      String managerTableBuilding =
          "create memory table MANAGER(ID varchar(256) not null, TYPE varchar(256), NAME"
              + " varchar(256), PROJECT varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, TYPE varchar(256), NAME"
              + " varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), PAY_PER_HOUR"
              + " decimal(10,2), CONTRACT_DURATION varchar(256), RESIDENCE varchar(256), MANAGER"
              + " varchar(256), primary key (id), foreign key (RESIDENCE) references RESIDENCE(ID),"
              + " foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);

      // Records Inserting

      String countryFilling =
          "insert into country (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
              + "('R001','Rome','C001'),"
              + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling =
          "insert into MANAGER (ID,TYPE,NAME,PROJECT) values ("
              + "('M001','prj_mgr','Bill Right','New World'))";
      st.execute(managerFilling);

      String employeeFilling =
          "insert into EMPLOYEE"
              + " (ID,TYPE,NAME,SALARY,BONUS,PAY_PER_HOUR,CONTRACT_DURATION,RESIDENCE,MANAGER)"
              + " values (('E001','emp','John"
              + " Black',null,null,null,null,'R001',null),('E002','reg_emp','Andrew"
              + " Brown','1000.00','10',null,null,'R001','M001'),('E003','cont_emp','Jack"
              + " Johnson',null,null,'50.00','6','R002',null))";
      st.execute(employeeFilling);

      List<String> includedTables = new ArrayList<String>();
      includedTables.add("COUNTRY");
      includedTables.add("MANAGER");
      includedTables.add("EMPLOYEE");

      this.mapper =
          new Hibernate2GraphMapper(
              dataSource,
              FilterTableMappingTest.XML_TABLE_PER_CLASS,
              includedTables,
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

      assertThat(statistics.totalNumberOfEntities).isEqualTo(3);
      assertThat(statistics.builtEntities).isEqualTo(3);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(1);
      assertThat(statistics.builtRelationships).isEqualTo(1);

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(6);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(6);
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
      Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
      Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
      Entity projectManagerEntity =
          mapper.getDataBaseSchema().getEntityByNameIgnoreCase("PROJECT_MANAGER");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(6);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity).isNotNull();
      assertThat(regularEmployeeEntity).isNotNull();
      assertThat(contractEmployeeEntity).isNotNull();
      assertThat(countryEntity).isNotNull();
      assertThat(managerEntity).isNotNull();
      assertThat(residenceEntity).isNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(4);

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

      assertThat(employeeEntity.getAttributeByName("MANAGER")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("MANAGER").getName()).isEqualTo("MANAGER");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName())
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

      assertThat(countryEntity.getAttributes().size()).isEqualTo(3);

      assertThat(countryEntity.getAttributeByName("ID")).isNotNull();
      assertThat(countryEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(countryEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(countryEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(countryEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("CONTINENT")).isNotNull();
      assertThat(countryEntity.getAttributeByName("CONTINENT").getName()).isEqualTo("CONTINENT");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition()).isEqualTo(3);
      assertThat(countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(managerEntity.getAttributes().size()).isEqualTo(2);

      assertThat(managerEntity.getAttributeByName("ID")).isNotNull();
      assertThat(managerEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(managerEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(managerEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(managerEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(managerEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(projectManagerEntity.getAttributes().size()).isEqualTo(1);

      assertThat(projectManagerEntity.getAttributeByName("PROJECT")).isNotNull();
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getName()).isEqualTo("PROJECT");
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName())
          .isEqualTo("Project_Manager");

      // inherited attributes check
      assertThat(employeeEntity.getInheritedAttributes().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getInheritedAttributes().size()).isEqualTo(4);

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

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getName())
          .isEqualTo("MANAGER");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("MANAGER")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributes().size()).isEqualTo(4);

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

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getName())
          .isEqualTo("MANAGER");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("MANAGER")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(projectManagerEntity.getInheritedAttributes().size()).isEqualTo(2);

      assertThat(projectManagerEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              projectManagerEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              projectManagerEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("MANAGER");

      assertThat(countryEntity.getInheritedAttributes().size()).isEqualTo(0);
      assertThat(managerEntity.getInheritedAttributes().size()).isEqualTo(0);

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

      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("ID");
      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              projectManagerEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("MANAGER");

      // relationship, primary and foreign key check
      assertThat(regularEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(countryEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(managerEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(regularEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(countryEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(managerEntity.getInCanonicalRelationships().size()).isEqualTo(1);

      assertThat(regularEmployeeEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(countryEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(managerEntity.getForeignKeys().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itManager =
          managerEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentManRel = itManager.next();
      assertThat(currentManRel).isEqualTo(currentEmpRel);

      // inherited relationships check
      assertThat(regularEmployeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritedOutCanonicalRelationships().size())
          .isEqualTo(1);
      assertThat(employeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      Iterator<CanonicalRelationship> itContEmp =
          contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      CanonicalRelationship currentRegEmpRel = itRegEmp.next();
      CanonicalRelationship currentContEmpRel = itContEmp.next();
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentRegEmpRel.getFromColumns().get(0).getName()).isEqualTo("MANAGER");
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentContEmpRel.getFromColumns().get(0).getName()).isEqualTo("MANAGER");
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      // inheritance check
      assertThat(regularEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(contractEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(employeeEntity.getParentEntity()).isNull();

      assertThat(regularEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeEntity.getInheritanceLevel()).isEqualTo(0);

      // Hierarchical Bag check
      assertThat(mapper.getDataBaseSchema().getHierarchicalBags().size()).isEqualTo(2);

      HierarchicalBag hierarchicalBag1 = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      HierarchicalBag hierarchicalBag2 = mapper.getDataBaseSchema().getHierarchicalBags().get(1);
      assertThat(hierarchicalBag1.getInheritancePattern()).isEqualTo("table-per-hierarchy");
      assertThat(hierarchicalBag2.getInheritancePattern()).isEqualTo("table-per-hierarchy");

      assertThat(hierarchicalBag1.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag1.getDepth2entities().get(0).size()).isEqualTo(1);
      Iterator<Entity> it = hierarchicalBag1.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag1.getDepth2entities().get(1).size()).isEqualTo(2);
      it = hierarchicalBag1.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("Regular_Employee");
      assertThat(it.next().getName()).isEqualTo("Contract_Employee");
      assertThat(it.hasNext()).isFalse();

      assertThat(employeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);
      assertThat(regularEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);
      assertThat(contractEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);

      assertThat(hierarchicalBag1.getDiscriminatorColumn()).isNotNull();
      assertThat(hierarchicalBag1.getDiscriminatorColumn()).isEqualTo("TYPE");

      assertThat(hierarchicalBag1.getEntityName2discriminatorValue().size()).isEqualTo(3);
      assertThat(hierarchicalBag1.getEntityName2discriminatorValue().get("EMPLOYEE"))
          .isEqualTo("emp");
      assertThat(hierarchicalBag1.getEntityName2discriminatorValue().get("Regular_Employee"))
          .isEqualTo("reg_emp");
      assertThat(hierarchicalBag1.getEntityName2discriminatorValue().get("Contract_Employee"))
          .isEqualTo("cont_emp");

      assertThat(hierarchicalBag2.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag2.getDepth2entities().get(0).size()).isEqualTo(1);
      it = hierarchicalBag2.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("MANAGER");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag2.getDepth2entities().get(1).size()).isEqualTo(1);
      it = hierarchicalBag2.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("Project_Manager");
      assertThat(it.hasNext()).isFalse();

      assertThat(managerEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag2);
      assertThat(projectManagerEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag2);

      assertThat(hierarchicalBag2.getDiscriminatorColumn()).isNotNull();
      assertThat(hierarchicalBag2.getDiscriminatorColumn()).isEqualTo("TYPE");

      assertThat(hierarchicalBag2.getEntityName2discriminatorValue().size()).isEqualTo(2);
      assertThat(hierarchicalBag2.getEntityName2discriminatorValue().get("MANAGER"))
          .isEqualTo("mgr");
      assertThat(hierarchicalBag2.getEntityName2discriminatorValue().get("Project_Manager"))
          .isEqualTo("prj_mgr");

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      VertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
      VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
      VertexType projectManagerVertexType =
          mapper.getGraphModel().getVertexTypeByName("ProjectManager");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(6);
      assertThat(employeeVertexType).isNotNull();
      assertThat(regularEmployeeVertexType).isNotNull();
      assertThat(contractEmployeeVertexType).isNotNull();
      assertThat(countryVertexType).isNotNull();
      assertThat(managerVertexType).isNotNull();
      assertThat(projectManagerVertexType).isNotNull();
      assertThat(residenceVertexType).isNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(4);

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

      assertThat(employeeVertexType.getPropertyByName("manager")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("manager").getName()).isEqualTo("manager");
      assertThat(employeeVertexType.getPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("manager").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeVertexType.getPropertyByName("manager").isFromPrimaryKey()).isFalse();

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

      assertThat(countryVertexType.getProperties().size()).isEqualTo(3);

      assertThat(countryVertexType.getPropertyByName("id")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(countryVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(countryVertexType.getPropertyByName("name")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(countryVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(countryVertexType.getPropertyByName("continent")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("continent").getName()).isEqualTo("continent");
      assertThat(countryVertexType.getPropertyByName("continent").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("continent").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(countryVertexType.getPropertyByName("continent").isFromPrimaryKey()).isFalse();

      assertThat(managerVertexType.getProperties().size()).isEqualTo(2);

      assertThat(managerVertexType.getPropertyByName("id")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(managerVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(managerVertexType.getPropertyByName("name")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(managerVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(projectManagerVertexType.getProperties().size()).isEqualTo(1);

      assertThat(projectManagerVertexType.getPropertyByName("project")).isNotNull();
      assertThat(projectManagerVertexType.getPropertyByName("project").getName())
          .isEqualTo("project");
      assertThat(projectManagerVertexType.getPropertyByName("project").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getPropertyByName("project").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerVertexType.getPropertyByName("project").isFromPrimaryKey())
          .isFalse();

      // inherited properties check
      assertThat(employeeVertexType.getInheritedProperties().size()).isEqualTo(0);

      assertThat(regularEmployeeVertexType.getInheritedProperties().size()).isEqualTo(4);

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

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").getName())
          .isEqualTo("manager");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedProperties().size()).isEqualTo(4);

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

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager").getName())
          .isEqualTo("manager");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey())
          .isFalse();

      assertThat(projectManagerVertexType.getInheritedProperties().size()).isEqualTo(2);

      assertThat(projectManagerVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isTrue();

      assertThat(projectManagerVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(countryVertexType.getInheritedProperties().size()).isEqualTo(0);
      assertThat(managerVertexType.getInheritedProperties().size()).isEqualTo(0);

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(regularEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(regularEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasManager");

      assertThat(contractEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasManager");

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(6);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(6);

      assertThat(mapper.getEVClassMappersByVertex(employeeVertexType).size()).isEqualTo(1);
      EVClassMapper employeeClassMapper =
          mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).get(0))
          .isEqualTo(employeeClassMapper);
      assertThat(employeeEntity).isEqualTo(employeeClassMapper.getEntity());
      assertThat(employeeVertexType).isEqualTo(employeeClassMapper.getVertexType());

      assertThat(employeeClassMapper.getAttribute2property().size()).isEqualTo(4);
      assertThat(employeeClassMapper.getProperty2attribute().size()).isEqualTo(4);
      assertThat(employeeClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(employeeClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(employeeClassMapper.getAttribute2property().get("RESIDENCE"))
          .isEqualTo("residence");
      assertThat(employeeClassMapper.getAttribute2property().get("MANAGER")).isEqualTo("manager");
      assertThat(employeeClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(employeeClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(employeeClassMapper.getProperty2attribute().get("residence"))
          .isEqualTo("RESIDENCE");
      assertThat(employeeClassMapper.getProperty2attribute().get("manager")).isEqualTo("MANAGER");

      assertThat(mapper.getEVClassMappersByVertex(regularEmployeeVertexType).size()).isEqualTo(1);
      EVClassMapper regularEmployeeClassMapper =
          mapper.getEVClassMappersByVertex(regularEmployeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(regularEmployeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(regularEmployeeEntity).get(0))
          .isEqualTo(regularEmployeeClassMapper);
      assertThat(regularEmployeeEntity).isEqualTo(regularEmployeeClassMapper.getEntity());
      assertThat(regularEmployeeVertexType).isEqualTo(regularEmployeeClassMapper.getVertexType());

      assertThat(regularEmployeeClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(regularEmployeeClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(regularEmployeeClassMapper.getAttribute2property().get("SALARY"))
          .isEqualTo("salary");
      assertThat(regularEmployeeClassMapper.getAttribute2property().get("BONUS"))
          .isEqualTo("bonus");
      assertThat(regularEmployeeClassMapper.getProperty2attribute().get("salary"))
          .isEqualTo("SALARY");
      assertThat(regularEmployeeClassMapper.getProperty2attribute().get("bonus"))
          .isEqualTo("BONUS");

      assertThat(mapper.getEVClassMappersByVertex(contractEmployeeVertexType).size()).isEqualTo(1);
      EVClassMapper contractEmployeeClassMapper =
          mapper.getEVClassMappersByVertex(contractEmployeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(contractEmployeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(contractEmployeeEntity).get(0))
          .isEqualTo(contractEmployeeClassMapper);
      assertThat(contractEmployeeEntity).isEqualTo(contractEmployeeClassMapper.getEntity());
      assertThat(contractEmployeeVertexType).isEqualTo(contractEmployeeClassMapper.getVertexType());

      assertThat(contractEmployeeClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(contractEmployeeClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(contractEmployeeClassMapper.getAttribute2property().get("PAY_PER_HOUR"))
          .isEqualTo("payPerHour");
      assertThat(contractEmployeeClassMapper.getAttribute2property().get("CONTRACT_DURATION"))
          .isEqualTo("contractDuration");
      assertThat(contractEmployeeClassMapper.getProperty2attribute().get("payPerHour"))
          .isEqualTo("PAY_PER_HOUR");
      assertThat(contractEmployeeClassMapper.getProperty2attribute().get("contractDuration"))
          .isEqualTo("CONTRACT_DURATION");

      assertThat(mapper.getEVClassMappersByVertex(countryVertexType).size()).isEqualTo(1);
      EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).get(0))
          .isEqualTo(countryClassMapper);
      assertThat(countryEntity).isEqualTo(countryClassMapper.getEntity());
      assertThat(countryVertexType).isEqualTo(countryClassMapper.getVertexType());

      assertThat(countryClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(countryClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(countryClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(countryClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(countryClassMapper.getAttribute2property().get("CONTINENT"))
          .isEqualTo("continent");
      assertThat(countryClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(countryClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(countryClassMapper.getProperty2attribute().get("continent"))
          .isEqualTo("CONTINENT");

      assertThat(mapper.getEVClassMappersByVertex(managerVertexType).size()).isEqualTo(1);
      EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).get(0))
          .isEqualTo(managerClassMapper);
      assertThat(managerEntity).isEqualTo(managerClassMapper.getEntity());
      assertThat(managerVertexType).isEqualTo(managerClassMapper.getVertexType());

      assertThat(managerClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(managerClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(managerClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(managerClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(managerClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(managerClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");

      assertThat(mapper.getEVClassMappersByVertex(projectManagerVertexType).size()).isEqualTo(1);
      EVClassMapper projectManagerClassMapper =
          mapper.getEVClassMappersByVertex(projectManagerVertexType).get(0);

      assertThat(mapper.getEVClassMappersByEntity(projectManagerEntity).get(0))
          .isEqualTo(projectManagerClassMapper);
      assertThat(projectManagerEntity).isEqualTo(projectManagerClassMapper.getEntity());
      assertThat(projectManagerVertexType).isEqualTo(projectManagerClassMapper.getVertexType());

      assertThat(projectManagerClassMapper.getAttribute2property().size()).isEqualTo(1);
      assertThat(projectManagerClassMapper.getProperty2attribute().size()).isEqualTo(1);
      assertThat(projectManagerClassMapper.getAttribute2property().get("PROJECT"))
          .isEqualTo("project");
      assertThat(projectManagerClassMapper.getProperty2attribute().get("project"))
          .isEqualTo("PROJECT");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> itRelationships =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasManagerRelationship = itRelationships.next();
      assertThat(itRelationships.hasNext()).isFalse();

      EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);
      assertThat(mapper.getRelationship2edgeType().get(hasManagerRelationship))
          .isEqualTo(hasManagerEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(hasManagerEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(hasManagerEdgeType)
                  .contains(hasManagerRelationship))
          .isTrue();

      // JoinVertexes-AggregatorEdges Mapping

      assertThat(mapper.getJoinVertex2aggregatorEdges().size()).isEqualTo(0);
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
   * Filtering out a table through include-tables (with Table per Type inheritance).
   */ @Test
  void filterOutThroughIncludeWithTablePerTypeInheritance() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String countryTableBuilding =
          "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding =
          "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key"
              + " (ID))";
      st.execute(managerTableBuilding);

      String projectManagerTableBuilding =
          "create memory table PROJECT_MANAGER(EID varchar(256) not null, PROJECT varchar(256),"
              + " primary key (EID), foreign key (EID) references MANAGER(ID))";
      st.execute(projectManagerTableBuilding);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, NAME varchar(256), RESIDENCE"
              + " varchar(256), MANAGER varchar(256), primary key (ID), foreign key (RESIDENCE)"
              + " references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
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

      // Records Inserting

      String countryFilling =
          "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
              + "('R001','Rome','C001'),"
              + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values (" + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling =
          "insert into PROJECT_MANAGER (EID,PROJECT) values (" + "('M001','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling =
          "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values ("
              + "('E001','John Black','R001',null),"
              + "('E002','Andrew Brown','R001','M001'),"
              + "('E003','Jack Johnson','R002',null))";
      st.execute(employeeFilling);

      String regularEmployeeFilling =
          "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values (" + "('E002','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling =
          "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values ("
              + "('E003','50.00','6'))";
      st.execute(contractEmployeeFilling);

      List<String> includedTables = new ArrayList<String>();
      includedTables.add("COUNTRY");
      includedTables.add("MANAGER");
      includedTables.add("PROJECT_MANAGER");
      includedTables.add("EMPLOYEE");
      includedTables.add("REGULAR_EMPLOYEE");
      includedTables.add("CONTRACT_EMPLOYEE");

      this.mapper =
          new Hibernate2GraphMapper(
              dataSource,
              FilterTableMappingTest.XML_TABLE_PER_SUBCLASS1,
              includedTables,
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

      assertThat(statistics.totalNumberOfEntities).isEqualTo(6);
      assertThat(statistics.builtEntities).isEqualTo(6);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(4);
      assertThat(statistics.builtRelationships)
          .isEqualTo(4); // 3 of these are hierarchical relationships

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(6);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(6);
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
      Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
      Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
      Entity projectManagerEntity =
          mapper.getDataBaseSchema().getEntityByNameIgnoreCase("PROJECT_MANAGER");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(6);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(4);
      assertThat(employeeEntity).isNotNull();
      assertThat(regularEmployeeEntity).isNotNull();
      assertThat(contractEmployeeEntity).isNotNull();
      assertThat(countryEntity).isNotNull();
      assertThat(managerEntity).isNotNull();
      assertThat(residenceEntity).isNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(4);

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

      assertThat(employeeEntity.getAttributeByName("MANAGER")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("MANAGER").getName()).isEqualTo("MANAGER");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName())
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

      assertThat(countryEntity.getAttributes().size()).isEqualTo(3);

      assertThat(countryEntity.getAttributeByName("ID")).isNotNull();
      assertThat(countryEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(countryEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(countryEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(countryEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("CONTINENT")).isNotNull();
      assertThat(countryEntity.getAttributeByName("CONTINENT").getName()).isEqualTo("CONTINENT");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition()).isEqualTo(3);
      assertThat(countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(managerEntity.getAttributes().size()).isEqualTo(2);

      assertThat(managerEntity.getAttributeByName("ID")).isNotNull();
      assertThat(managerEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(managerEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(managerEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(managerEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(managerEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(projectManagerEntity.getAttributes().size()).isEqualTo(1);

      assertThat(projectManagerEntity.getAttributeByName("PROJECT")).isNotNull();
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getName()).isEqualTo("PROJECT");
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName())
          .isEqualTo("PROJECT_MANAGER");

      // inherited attributes check
      assertThat(employeeEntity.getInheritedAttributes().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getInheritedAttributes().size()).isEqualTo(4);

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

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getName())
          .isEqualTo("MANAGER");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("MANAGER")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributes().size()).isEqualTo(4);

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

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getName())
          .isEqualTo("MANAGER");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("MANAGER")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(projectManagerEntity.getInheritedAttributes().size()).isEqualTo(2);

      assertThat(projectManagerEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              projectManagerEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              projectManagerEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("MANAGER");

      assertThat(countryEntity.getInheritedAttributes().size()).isEqualTo(0);
      assertThat(managerEntity.getInheritedAttributes().size()).isEqualTo(0);

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

      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("EID");
      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              projectManagerEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("PROJECT_MANAGER");

      // relationship, primary and foreign key check
      assertThat(regularEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(managerEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(countryEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(regularEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(2);
      assertThat(projectManagerEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(managerEntity.getInCanonicalRelationships().size()).isEqualTo(2);
      assertThat(countryEntity.getInCanonicalRelationships().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(managerEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(countryEntity.getForeignKeys().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itManager =
          managerEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentManRel = itManager.next();
      assertThat(currentManRel).isEqualTo(currentEmpRel);

      Iterator<CanonicalRelationship> itContEmp =
          contractEmployeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentContEmpRel = itContEmp.next();
      itEmp = employeeEntity.getInCanonicalRelationships().iterator();
      currentEmpRel = itEmp.next();
      assertThat(currentEmpRel).isEqualTo(currentContEmpRel);

      Iterator<CanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentRegEmpRel = itRegEmp.next();
      currentEmpRel = itEmp.next();
      assertThat(currentEmpRel).isEqualTo(currentRegEmpRel);

      // inherited relationships check
      assertThat(regularEmployeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritedOutCanonicalRelationships().size())
          .isEqualTo(1);
      assertThat(employeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);

      itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      currentRegEmpRel = itRegEmp.next();
      currentContEmpRel = itContEmp.next();
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentRegEmpRel.getFromColumns().get(0).getName()).isEqualTo("MANAGER");
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentContEmpRel.getFromColumns().get(0).getName()).isEqualTo("MANAGER");
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      // inheritance check
      assertThat(regularEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(contractEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(employeeEntity.getParentEntity()).isNull();

      assertThat(regularEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeEntity.getInheritanceLevel()).isEqualTo(0);

      // Hierarchical Bag check
      assertThat(mapper.getDataBaseSchema().getHierarchicalBags().size()).isEqualTo(2);

      HierarchicalBag hierarchicalBag1 = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      HierarchicalBag hierarchicalBag2 = mapper.getDataBaseSchema().getHierarchicalBags().get(1);
      assertThat(hierarchicalBag1.getInheritancePattern()).isEqualTo("table-per-type");
      assertThat(hierarchicalBag2.getInheritancePattern()).isEqualTo("table-per-type");

      assertThat(hierarchicalBag1.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag1.getDepth2entities().get(0).size()).isEqualTo(1);
      Iterator<Entity> it = hierarchicalBag1.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag1.getDepth2entities().get(1).size()).isEqualTo(2);
      it = hierarchicalBag1.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("REGULAR_EMPLOYEE");
      assertThat(it.next().getName()).isEqualTo("CONTRACT_EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(employeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);
      assertThat(regularEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);
      assertThat(contractEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);

      assertThat(hierarchicalBag1.getDiscriminatorColumn()).isNull();

      assertThat(hierarchicalBag2.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag2.getDepth2entities().get(0).size()).isEqualTo(1);
      it = hierarchicalBag2.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("MANAGER");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag2.getDepth2entities().get(1).size()).isEqualTo(1);
      it = hierarchicalBag2.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("PROJECT_MANAGER");
      assertThat(it.hasNext()).isFalse();

      assertThat(managerEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag2);
      assertThat(projectManagerEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag2);

      assertThat(hierarchicalBag2.getDiscriminatorColumn()).isNull();

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      VertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
      VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
      VertexType projectManagerVertexType =
          mapper.getGraphModel().getVertexTypeByName("ProjectManager");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(6);
      assertThat(employeeVertexType).isNotNull();
      assertThat(regularEmployeeVertexType).isNotNull();
      assertThat(contractEmployeeVertexType).isNotNull();
      assertThat(countryVertexType).isNotNull();
      assertThat(managerVertexType).isNotNull();
      assertThat(projectManagerVertexType).isNotNull();
      assertThat(residenceVertexType).isNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(4);

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

      assertThat(employeeVertexType.getPropertyByName("manager")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("manager").getName()).isEqualTo("manager");
      assertThat(employeeVertexType.getPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("manager").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeVertexType.getPropertyByName("manager").isFromPrimaryKey()).isFalse();

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

      assertThat(countryVertexType.getProperties().size()).isEqualTo(3);

      assertThat(countryVertexType.getPropertyByName("id")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(countryVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(countryVertexType.getPropertyByName("name")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(countryVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(countryVertexType.getPropertyByName("continent")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("continent").getName()).isEqualTo("continent");
      assertThat(countryVertexType.getPropertyByName("continent").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("continent").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(countryVertexType.getPropertyByName("continent").isFromPrimaryKey()).isFalse();

      assertThat(managerVertexType.getProperties().size()).isEqualTo(2);

      assertThat(managerVertexType.getPropertyByName("id")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(managerVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(managerVertexType.getPropertyByName("name")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(managerVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(projectManagerVertexType.getProperties().size()).isEqualTo(1);

      assertThat(projectManagerVertexType.getPropertyByName("project")).isNotNull();
      assertThat(projectManagerVertexType.getPropertyByName("project").getName())
          .isEqualTo("project");
      assertThat(projectManagerVertexType.getPropertyByName("project").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getPropertyByName("project").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerVertexType.getPropertyByName("project").isFromPrimaryKey())
          .isFalse();

      // inherited properties check
      assertThat(employeeVertexType.getInheritedProperties().size()).isEqualTo(0);

      assertThat(regularEmployeeVertexType.getInheritedProperties().size()).isEqualTo(4);

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

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").getName())
          .isEqualTo("manager");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedProperties().size()).isEqualTo(4);

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

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager").getName())
          .isEqualTo("manager");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey())
          .isFalse();

      assertThat(projectManagerVertexType.getInheritedProperties().size()).isEqualTo(2);

      assertThat(projectManagerVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isFalse();

      assertThat(projectManagerVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(countryVertexType.getInheritedProperties().size()).isEqualTo(0);
      assertThat(managerVertexType.getInheritedProperties().size()).isEqualTo(0);

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(regularEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(regularEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasManager");

      assertThat(contractEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasManager");

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(6);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(6);

      assertThat(mapper.getEVClassMappersByVertex(employeeVertexType).size()).isEqualTo(1);
      EVClassMapper employeeClassMapper =
          mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).get(0))
          .isEqualTo(employeeClassMapper);
      assertThat(employeeEntity).isEqualTo(employeeClassMapper.getEntity());
      assertThat(employeeVertexType).isEqualTo(employeeClassMapper.getVertexType());

      assertThat(employeeClassMapper.getAttribute2property().size()).isEqualTo(4);
      assertThat(employeeClassMapper.getProperty2attribute().size()).isEqualTo(4);
      assertThat(employeeClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(employeeClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(employeeClassMapper.getAttribute2property().get("RESIDENCE"))
          .isEqualTo("residence");
      assertThat(employeeClassMapper.getAttribute2property().get("MANAGER")).isEqualTo("manager");
      assertThat(employeeClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(employeeClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(employeeClassMapper.getProperty2attribute().get("residence"))
          .isEqualTo("RESIDENCE");
      assertThat(employeeClassMapper.getProperty2attribute().get("manager")).isEqualTo("MANAGER");

      assertThat(mapper.getEVClassMappersByVertex(regularEmployeeVertexType).size()).isEqualTo(1);
      EVClassMapper regularEmployeeClassMapper =
          mapper.getEVClassMappersByVertex(regularEmployeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(regularEmployeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(regularEmployeeEntity).get(0))
          .isEqualTo(regularEmployeeClassMapper);
      assertThat(regularEmployeeEntity).isEqualTo(regularEmployeeClassMapper.getEntity());
      assertThat(regularEmployeeVertexType).isEqualTo(regularEmployeeClassMapper.getVertexType());

      assertThat(regularEmployeeClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(regularEmployeeClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(regularEmployeeClassMapper.getAttribute2property().get("SALARY"))
          .isEqualTo("salary");
      assertThat(regularEmployeeClassMapper.getAttribute2property().get("BONUS"))
          .isEqualTo("bonus");
      assertThat(regularEmployeeClassMapper.getProperty2attribute().get("salary"))
          .isEqualTo("SALARY");
      assertThat(regularEmployeeClassMapper.getProperty2attribute().get("bonus"))
          .isEqualTo("BONUS");

      assertThat(mapper.getEVClassMappersByVertex(contractEmployeeVertexType).size()).isEqualTo(1);
      EVClassMapper contractEmployeeClassMapper =
          mapper.getEVClassMappersByVertex(contractEmployeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(contractEmployeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(contractEmployeeEntity).get(0))
          .isEqualTo(contractEmployeeClassMapper);
      assertThat(contractEmployeeEntity).isEqualTo(contractEmployeeClassMapper.getEntity());
      assertThat(contractEmployeeVertexType).isEqualTo(contractEmployeeClassMapper.getVertexType());

      assertThat(contractEmployeeClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(contractEmployeeClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(contractEmployeeClassMapper.getAttribute2property().get("PAY_PER_HOUR"))
          .isEqualTo("payPerHour");
      assertThat(contractEmployeeClassMapper.getAttribute2property().get("CONTRACT_DURATION"))
          .isEqualTo("contractDuration");
      assertThat(contractEmployeeClassMapper.getProperty2attribute().get("payPerHour"))
          .isEqualTo("PAY_PER_HOUR");
      assertThat(contractEmployeeClassMapper.getProperty2attribute().get("contractDuration"))
          .isEqualTo("CONTRACT_DURATION");

      assertThat(mapper.getEVClassMappersByVertex(countryVertexType).size()).isEqualTo(1);
      EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).get(0))
          .isEqualTo(countryClassMapper);
      assertThat(countryEntity).isEqualTo(countryClassMapper.getEntity());
      assertThat(countryVertexType).isEqualTo(countryClassMapper.getVertexType());

      assertThat(countryClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(countryClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(countryClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(countryClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(countryClassMapper.getAttribute2property().get("CONTINENT"))
          .isEqualTo("continent");
      assertThat(countryClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(countryClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(countryClassMapper.getProperty2attribute().get("continent"))
          .isEqualTo("CONTINENT");

      assertThat(mapper.getEVClassMappersByVertex(managerVertexType).size()).isEqualTo(1);
      EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).get(0))
          .isEqualTo(managerClassMapper);
      assertThat(managerEntity).isEqualTo(managerClassMapper.getEntity());
      assertThat(managerVertexType).isEqualTo(managerClassMapper.getVertexType());

      assertThat(managerClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(managerClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(managerClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(managerClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(managerClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(managerClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");

      assertThat(mapper.getEVClassMappersByVertex(projectManagerVertexType).size()).isEqualTo(1);
      EVClassMapper projectManagerClassMapper =
          mapper.getEVClassMappersByVertex(projectManagerVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(projectManagerEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(projectManagerEntity).get(0))
          .isEqualTo(projectManagerClassMapper);
      assertThat(projectManagerEntity).isEqualTo(projectManagerClassMapper.getEntity());
      assertThat(projectManagerVertexType).isEqualTo(projectManagerClassMapper.getVertexType());

      assertThat(projectManagerClassMapper.getAttribute2property().size()).isEqualTo(1);
      assertThat(projectManagerClassMapper.getProperty2attribute().size()).isEqualTo(1);
      assertThat(projectManagerClassMapper.getAttribute2property().get("PROJECT"))
          .isEqualTo("project");
      assertThat(projectManagerClassMapper.getProperty2attribute().get("project"))
          .isEqualTo("PROJECT");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> itRelationships =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasManagerRelationship = itRelationships.next();
      assertThat(itRelationships.hasNext()).isFalse();

      EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);
      assertThat(mapper.getRelationship2edgeType().get(hasManagerRelationship))
          .isEqualTo(hasManagerEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(hasManagerEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(hasManagerEdgeType)
                  .contains(hasManagerRelationship))
          .isTrue();

      // JoinVertexes-AggregatorEdges Mapping

      assertThat(mapper.getJoinVertex2aggregatorEdges().size()).isEqualTo(0);
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
   * Filtering out a table through exclude-tables (with Table per Type inheritance).
   */ @Test
  void filterOutThroughExcludeWithTablePerTypeInheritance() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String countryTableBuilding =
          "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding =
          "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key"
              + " (ID))";
      st.execute(managerTableBuilding);

      String projectManagerTableBuilding =
          "create memory table PROJECT_MANAGER(EID varchar(256) not null, PROJECT varchar(256),"
              + " primary key (EID), foreign key (EID) references MANAGER(ID))";
      st.execute(projectManagerTableBuilding);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, NAME varchar(256), RESIDENCE"
              + " varchar(256), MANAGER varchar(256), primary key (ID), foreign key (RESIDENCE)"
              + " references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
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

      // Records Inserting

      String countryFilling =
          "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
              + "('R001','Rome','C001'),"
              + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values (" + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling =
          "insert into PROJECT_MANAGER (EID,PROJECT) values (" + "('M001','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling =
          "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values ("
              + "('E001','John Black','R001',null),"
              + "('E002','Andrew Brown','R001','M001'),"
              + "('E003','Jack Johnson','R002',null))";
      st.execute(employeeFilling);

      String regularEmployeeFilling =
          "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values (" + "('E002','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling =
          "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values ("
              + "('E003','50.00','6'))";
      st.execute(contractEmployeeFilling);

      List<String> excludedTables = new ArrayList<String>();
      excludedTables.add("RESIDENCE");

      this.mapper =
          new Hibernate2GraphMapper(
              dataSource,
              FilterTableMappingTest.XML_TABLE_PER_SUBCLASS2,
              null,
              excludedTables,
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

      assertThat(statistics.totalNumberOfEntities).isEqualTo(6);
      assertThat(statistics.builtEntities).isEqualTo(6);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(4);
      assertThat(statistics.builtRelationships)
          .isEqualTo(4); // 3 of these are hierarchical relationships

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(6);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(6);
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
      Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
      Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
      Entity projectManagerEntity =
          mapper.getDataBaseSchema().getEntityByNameIgnoreCase("PROJECT_MANAGER");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(6);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(4);
      assertThat(employeeEntity).isNotNull();
      assertThat(regularEmployeeEntity).isNotNull();
      assertThat(contractEmployeeEntity).isNotNull();
      assertThat(countryEntity).isNotNull();
      assertThat(managerEntity).isNotNull();
      assertThat(residenceEntity).isNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(4);

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

      assertThat(employeeEntity.getAttributeByName("MANAGER")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("MANAGER").getName()).isEqualTo("MANAGER");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName())
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

      assertThat(countryEntity.getAttributes().size()).isEqualTo(3);

      assertThat(countryEntity.getAttributeByName("ID")).isNotNull();
      assertThat(countryEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(countryEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(countryEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(countryEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("CONTINENT")).isNotNull();
      assertThat(countryEntity.getAttributeByName("CONTINENT").getName()).isEqualTo("CONTINENT");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition()).isEqualTo(3);
      assertThat(countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(managerEntity.getAttributes().size()).isEqualTo(2);

      assertThat(managerEntity.getAttributeByName("ID")).isNotNull();
      assertThat(managerEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(managerEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(managerEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(managerEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(managerEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(projectManagerEntity.getAttributes().size()).isEqualTo(1);

      assertThat(projectManagerEntity.getAttributeByName("PROJECT")).isNotNull();
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getName()).isEqualTo("PROJECT");
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName())
          .isEqualTo("PROJECT_MANAGER");

      // inherited attributes check
      assertThat(employeeEntity.getInheritedAttributes().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getInheritedAttributes().size()).isEqualTo(4);

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

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getName())
          .isEqualTo("MANAGER");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("MANAGER")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributes().size()).isEqualTo(4);

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

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getName())
          .isEqualTo("MANAGER");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("MANAGER")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(projectManagerEntity.getInheritedAttributes().size()).isEqualTo(2);

      assertThat(projectManagerEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              projectManagerEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              projectManagerEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("MANAGER");

      assertThat(countryEntity.getInheritedAttributes().size()).isEqualTo(0);
      assertThat(managerEntity.getInheritedAttributes().size()).isEqualTo(0);

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

      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("EID");
      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              projectManagerEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("PROJECT_MANAGER");

      // relationship, primary and foreign key check
      assertThat(regularEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(managerEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(countryEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(regularEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(2);
      assertThat(projectManagerEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(managerEntity.getInCanonicalRelationships().size()).isEqualTo(2);
      assertThat(countryEntity.getInCanonicalRelationships().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(managerEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(countryEntity.getForeignKeys().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itManager =
          managerEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentManRel = itManager.next();
      assertThat(currentManRel).isEqualTo(currentEmpRel);

      Iterator<CanonicalRelationship> itContEmp =
          contractEmployeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentContEmpRel = itContEmp.next();
      itEmp = employeeEntity.getInCanonicalRelationships().iterator();
      currentEmpRel = itEmp.next();
      assertThat(currentEmpRel).isEqualTo(currentContEmpRel);

      Iterator<CanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentRegEmpRel = itRegEmp.next();
      currentEmpRel = itEmp.next();
      assertThat(currentEmpRel).isEqualTo(currentRegEmpRel);

      // inherited relationships check
      assertThat(regularEmployeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritedOutCanonicalRelationships().size())
          .isEqualTo(1);
      assertThat(employeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);

      itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      currentRegEmpRel = itRegEmp.next();
      currentContEmpRel = itContEmp.next();
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentRegEmpRel.getFromColumns().get(0).getName()).isEqualTo("MANAGER");
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentContEmpRel.getFromColumns().get(0).getName()).isEqualTo("MANAGER");
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      // inheritance check
      assertThat(regularEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(contractEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(employeeEntity.getParentEntity()).isNull();

      assertThat(regularEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeEntity.getInheritanceLevel()).isEqualTo(0);

      // Hierarchical Bag check
      assertThat(mapper.getDataBaseSchema().getHierarchicalBags().size()).isEqualTo(2);

      HierarchicalBag hierarchicalBag1 = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      HierarchicalBag hierarchicalBag2 = mapper.getDataBaseSchema().getHierarchicalBags().get(1);
      assertThat(hierarchicalBag1.getInheritancePattern()).isEqualTo("table-per-type");
      assertThat(hierarchicalBag2.getInheritancePattern()).isEqualTo("table-per-type");

      assertThat(hierarchicalBag1.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag1.getDepth2entities().get(0).size()).isEqualTo(1);
      Iterator<Entity> it = hierarchicalBag1.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag1.getDepth2entities().get(1).size()).isEqualTo(2);
      it = hierarchicalBag1.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("REGULAR_EMPLOYEE");
      assertThat(it.next().getName()).isEqualTo("CONTRACT_EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(employeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);
      assertThat(regularEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);
      assertThat(contractEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);

      assertThat(hierarchicalBag1.getDiscriminatorColumn()).isNotNull();

      assertThat(hierarchicalBag2.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag2.getDepth2entities().get(0).size()).isEqualTo(1);
      it = hierarchicalBag2.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("MANAGER");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag2.getDepth2entities().get(1).size()).isEqualTo(1);
      it = hierarchicalBag2.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("PROJECT_MANAGER");
      assertThat(it.hasNext()).isFalse();

      assertThat(managerEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag2);
      assertThat(projectManagerEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag2);

      assertThat(hierarchicalBag2.getDiscriminatorColumn()).isNotNull();

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      VertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
      VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
      VertexType projectManagerVertexType =
          mapper.getGraphModel().getVertexTypeByName("ProjectManager");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(6);
      assertThat(employeeVertexType).isNotNull();
      assertThat(regularEmployeeVertexType).isNotNull();
      assertThat(contractEmployeeVertexType).isNotNull();
      assertThat(countryVertexType).isNotNull();
      assertThat(managerVertexType).isNotNull();
      assertThat(projectManagerVertexType).isNotNull();
      assertThat(residenceVertexType).isNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(4);

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

      assertThat(employeeVertexType.getPropertyByName("manager")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("manager").getName()).isEqualTo("manager");
      assertThat(employeeVertexType.getPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("manager").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeVertexType.getPropertyByName("manager").isFromPrimaryKey()).isFalse();

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

      assertThat(countryVertexType.getProperties().size()).isEqualTo(3);

      assertThat(countryVertexType.getPropertyByName("id")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(countryVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(countryVertexType.getPropertyByName("name")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(countryVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(countryVertexType.getPropertyByName("continent")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("continent").getName()).isEqualTo("continent");
      assertThat(countryVertexType.getPropertyByName("continent").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("continent").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(countryVertexType.getPropertyByName("continent").isFromPrimaryKey()).isFalse();

      assertThat(managerVertexType.getProperties().size()).isEqualTo(2);

      assertThat(managerVertexType.getPropertyByName("id")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(managerVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(managerVertexType.getPropertyByName("name")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(managerVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(projectManagerVertexType.getProperties().size()).isEqualTo(1);

      assertThat(projectManagerVertexType.getPropertyByName("project")).isNotNull();
      assertThat(projectManagerVertexType.getPropertyByName("project").getName())
          .isEqualTo("project");
      assertThat(projectManagerVertexType.getPropertyByName("project").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getPropertyByName("project").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerVertexType.getPropertyByName("project").isFromPrimaryKey())
          .isFalse();

      // inherited properties check
      assertThat(employeeVertexType.getInheritedProperties().size()).isEqualTo(0);

      assertThat(regularEmployeeVertexType.getInheritedProperties().size()).isEqualTo(4);

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

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").getName())
          .isEqualTo("manager");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedProperties().size()).isEqualTo(4);

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

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager").getName())
          .isEqualTo("manager");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey())
          .isFalse();

      assertThat(projectManagerVertexType.getInheritedProperties().size()).isEqualTo(2);

      assertThat(projectManagerVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isFalse();

      assertThat(projectManagerVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(countryVertexType.getInheritedProperties().size()).isEqualTo(0);
      assertThat(managerVertexType.getInheritedProperties().size()).isEqualTo(0);

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(regularEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(regularEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasManager");

      assertThat(contractEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasManager");

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(6);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(6);

      assertThat(mapper.getEVClassMappersByVertex(employeeVertexType).size()).isEqualTo(1);
      EVClassMapper employeeClassMapper =
          mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).get(0))
          .isEqualTo(employeeClassMapper);
      assertThat(employeeEntity).isEqualTo(employeeClassMapper.getEntity());
      assertThat(employeeVertexType).isEqualTo(employeeClassMapper.getVertexType());

      assertThat(employeeClassMapper.getAttribute2property().size()).isEqualTo(4);
      assertThat(employeeClassMapper.getProperty2attribute().size()).isEqualTo(4);
      assertThat(employeeClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(employeeClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(employeeClassMapper.getAttribute2property().get("RESIDENCE"))
          .isEqualTo("residence");
      assertThat(employeeClassMapper.getAttribute2property().get("MANAGER")).isEqualTo("manager");
      assertThat(employeeClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(employeeClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(employeeClassMapper.getProperty2attribute().get("residence"))
          .isEqualTo("RESIDENCE");
      assertThat(employeeClassMapper.getProperty2attribute().get("manager")).isEqualTo("MANAGER");

      assertThat(mapper.getEVClassMappersByVertex(regularEmployeeVertexType).size()).isEqualTo(1);
      EVClassMapper regularEmployeeClassMapper =
          mapper.getEVClassMappersByVertex(regularEmployeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(regularEmployeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(regularEmployeeEntity).get(0))
          .isEqualTo(regularEmployeeClassMapper);
      assertThat(regularEmployeeEntity).isEqualTo(regularEmployeeClassMapper.getEntity());
      assertThat(regularEmployeeVertexType).isEqualTo(regularEmployeeClassMapper.getVertexType());

      assertThat(regularEmployeeClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(regularEmployeeClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(regularEmployeeClassMapper.getAttribute2property().get("SALARY"))
          .isEqualTo("salary");
      assertThat(regularEmployeeClassMapper.getAttribute2property().get("BONUS"))
          .isEqualTo("bonus");
      assertThat(regularEmployeeClassMapper.getProperty2attribute().get("salary"))
          .isEqualTo("SALARY");
      assertThat(regularEmployeeClassMapper.getProperty2attribute().get("bonus"))
          .isEqualTo("BONUS");

      assertThat(mapper.getEVClassMappersByVertex(contractEmployeeVertexType).size()).isEqualTo(1);
      EVClassMapper contractEmployeeClassMapper =
          mapper.getEVClassMappersByVertex(contractEmployeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(contractEmployeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(contractEmployeeEntity).get(0))
          .isEqualTo(contractEmployeeClassMapper);
      assertThat(contractEmployeeEntity).isEqualTo(contractEmployeeClassMapper.getEntity());
      assertThat(contractEmployeeVertexType).isEqualTo(contractEmployeeClassMapper.getVertexType());

      assertThat(contractEmployeeClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(contractEmployeeClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(contractEmployeeClassMapper.getAttribute2property().get("PAY_PER_HOUR"))
          .isEqualTo("payPerHour");
      assertThat(contractEmployeeClassMapper.getAttribute2property().get("CONTRACT_DURATION"))
          .isEqualTo("contractDuration");
      assertThat(contractEmployeeClassMapper.getProperty2attribute().get("payPerHour"))
          .isEqualTo("PAY_PER_HOUR");
      assertThat(contractEmployeeClassMapper.getProperty2attribute().get("contractDuration"))
          .isEqualTo("CONTRACT_DURATION");

      assertThat(mapper.getEVClassMappersByVertex(countryVertexType).size()).isEqualTo(1);
      EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).get(0))
          .isEqualTo(countryClassMapper);
      assertThat(countryEntity).isEqualTo(countryClassMapper.getEntity());
      assertThat(countryVertexType).isEqualTo(countryClassMapper.getVertexType());

      assertThat(countryClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(countryClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(countryClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(countryClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(countryClassMapper.getAttribute2property().get("CONTINENT"))
          .isEqualTo("continent");
      assertThat(countryClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(countryClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(countryClassMapper.getProperty2attribute().get("continent"))
          .isEqualTo("CONTINENT");

      assertThat(mapper.getEVClassMappersByVertex(managerVertexType).size()).isEqualTo(1);
      EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).get(0))
          .isEqualTo(managerClassMapper);
      assertThat(managerEntity).isEqualTo(managerClassMapper.getEntity());
      assertThat(managerVertexType).isEqualTo(managerClassMapper.getVertexType());

      assertThat(managerClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(managerClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(managerClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(managerClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(managerClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(managerClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");

      assertThat(mapper.getEVClassMappersByVertex(projectManagerVertexType).size()).isEqualTo(1);
      EVClassMapper projectManagerClassMapper =
          mapper.getEVClassMappersByVertex(projectManagerVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(projectManagerEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(projectManagerEntity).get(0))
          .isEqualTo(projectManagerClassMapper);
      assertThat(projectManagerEntity).isEqualTo(projectManagerClassMapper.getEntity());
      assertThat(projectManagerVertexType).isEqualTo(projectManagerClassMapper.getVertexType());

      assertThat(projectManagerClassMapper.getAttribute2property().size()).isEqualTo(1);
      assertThat(projectManagerClassMapper.getProperty2attribute().size()).isEqualTo(1);
      assertThat(projectManagerClassMapper.getAttribute2property().get("PROJECT"))
          .isEqualTo("project");
      assertThat(projectManagerClassMapper.getProperty2attribute().get("project"))
          .isEqualTo("PROJECT");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> itRelationships =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasManagerRelationship = itRelationships.next();
      assertThat(itRelationships.hasNext()).isFalse();

      EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);
      assertThat(mapper.getRelationship2edgeType().get(hasManagerRelationship))
          .isEqualTo(hasManagerEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(hasManagerEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(hasManagerEdgeType)
                  .contains(hasManagerRelationship))
          .isTrue();

      // JoinVertexes-AggregatorEdges Mapping

      assertThat(mapper.getJoinVertex2aggregatorEdges().size()).isEqualTo(0);
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
   * Filtering out a table through include-tables (with Table per Concrete Type inheritance).
   */ @Test
  void filterOutThroughIncludeWithTablePerConcreteTypeInheritance() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String countryTableBuilding =
          "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY"
              + " varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding =
          "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key"
              + " (ID))";
      st.execute(managerTableBuilding);

      String projectManagerTableBuilding =
          "create memory table PROJECT_MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT"
              + " varchar(256), primary key (ID))";
      st.execute(projectManagerTableBuilding);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null, NAME varchar(256), RESIDENCE"
              + " varchar(256), MANAGER varchar(256), primary key (ID), foreign key (RESIDENCE)"
              + " references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding =
          "create memory table REGULAR_EMPLOYEE (ID varchar(256) not null, "
              + "NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256),"
              + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding =
          "create memory table CONTRACT_EMPLOYEE (ID varchar(256) not null, "
              + "NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256),"
              + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (ID))";
      st.execute(contractEmployeeTableBuilding);

      // Records Inserting

      String countryFilling =
          "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
              + "('R001','Rome','C001'),"
              + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values (" + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling =
          "insert into PROJECT_MANAGER (ID,NAME,PROJECT) values ("
              + "('M001','Bill Right','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling =
          "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values ("
              + "('E001','John Black','R001',null),"
              + "('E002','Andrew Brown','R001','M001'),"
              + "('E003','Jack Johnson','R002',null))";
      st.execute(employeeFilling);

      String regularEmployeeFilling =
          "insert into REGULAR_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,SALARY,BONUS) values ("
              + "('E002','Andrew Brown','R001','M001','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling =
          "insert into CONTRACT_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,PAY_PER_HOUR,CONTRACT_DURATION)"
              + " values (('E003','Jack Johnson','R002',null,'50.00','6'))";
      st.execute(contractEmployeeFilling);

      List<String> includedTables = new ArrayList<String>();
      includedTables.add("COUNTRY");
      includedTables.add("MANAGER");
      includedTables.add("PROJECT_MANAGER");
      includedTables.add("EMPLOYEE");
      includedTables.add("REGULAR_EMPLOYEE");
      includedTables.add("CONTRACT_EMPLOYEE");

      this.mapper =
          new Hibernate2GraphMapper(
              dataSource,
              FilterTableMappingTest.XML_TABLE_PER_CONCRETE_CLASS,
              includedTables,
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

      assertThat(statistics.totalNumberOfEntities).isEqualTo(6);
      assertThat(statistics.builtEntities).isEqualTo(6);
      assertThat(statistics.totalNumberOfRelationships).isEqualTo(1);
      assertThat(statistics.builtRelationships).isEqualTo(1);

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(6);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(6);
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
      Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
      Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
      Entity projectManagerEntity =
          mapper.getDataBaseSchema().getEntityByNameIgnoreCase("PROJECT_MANAGER");
      Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(6);
      assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(1);
      assertThat(employeeEntity).isNotNull();
      assertThat(regularEmployeeEntity).isNotNull();
      assertThat(contractEmployeeEntity).isNotNull();
      assertThat(countryEntity).isNotNull();
      assertThat(managerEntity).isNotNull();
      assertThat(residenceEntity).isNull();

      // attributes check
      assertThat(employeeEntity.getAttributes().size()).isEqualTo(4);

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

      assertThat(employeeEntity.getAttributeByName("MANAGER")).isNotNull();
      assertThat(employeeEntity.getAttributeByName("MANAGER").getName()).isEqualTo("MANAGER");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getDataType()).isEqualTo("VARCHAR");
      assertThat(employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName())
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

      assertThat(countryEntity.getAttributes().size()).isEqualTo(3);

      assertThat(countryEntity.getAttributeByName("ID")).isNotNull();
      assertThat(countryEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(countryEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(countryEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(countryEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(countryEntity.getAttributeByName("CONTINENT")).isNotNull();
      assertThat(countryEntity.getAttributeByName("CONTINENT").getName()).isEqualTo("CONTINENT");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getDataType()).isEqualTo("VARCHAR");
      assertThat(countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition()).isEqualTo(3);
      assertThat(countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName())
          .isEqualTo("COUNTRY");

      assertThat(managerEntity.getAttributes().size()).isEqualTo(2);

      assertThat(managerEntity.getAttributeByName("ID")).isNotNull();
      assertThat(managerEntity.getAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(managerEntity.getAttributeByName("ID").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("ID").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerEntity.getAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(managerEntity.getAttributeByName("NAME")).isNotNull();
      assertThat(managerEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
      assertThat(managerEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
      assertThat(managerEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerEntity.getAttributeByName("NAME").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(projectManagerEntity.getAttributes().size()).isEqualTo(1);

      assertThat(projectManagerEntity.getAttributeByName("PROJECT")).isNotNull();
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getName()).isEqualTo("PROJECT");
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName())
          .isEqualTo("PROJECT_MANAGER");

      // inherited attributes check
      assertThat(employeeEntity.getInheritedAttributes().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getInheritedAttributes().size()).isEqualTo(4);

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

      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER")).isNotNull();
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getName())
          .isEqualTo("MANAGER");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              regularEmployeeEntity
                  .getInheritedAttributeByName("MANAGER")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(contractEmployeeEntity.getInheritedAttributes().size()).isEqualTo(4);

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

      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER")).isNotNull();
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getName())
          .isEqualTo("MANAGER");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              contractEmployeeEntity
                  .getInheritedAttributeByName("MANAGER")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("EMPLOYEE");

      assertThat(projectManagerEntity.getInheritedAttributes().size()).isEqualTo(2);

      assertThat(projectManagerEntity.getInheritedAttributeByName("ID")).isNotNull();
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getName()).isEqualTo("ID");
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getInheritedAttributeByName("ID").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(
              projectManagerEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName())
          .isEqualTo("MANAGER");

      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME")).isNotNull();
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getName())
          .isEqualTo("NAME");
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getDataType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerEntity.getInheritedAttributeByName("NAME").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(
              projectManagerEntity
                  .getInheritedAttributeByName("NAME")
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("MANAGER");

      assertThat(countryEntity.getInheritedAttributes().size()).isEqualTo(0);
      assertThat(managerEntity.getInheritedAttributes().size()).isEqualTo(0);

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

      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName())
          .isEqualTo("ID");
      assertThat(projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType())
          .isEqualTo("VARCHAR");
      assertThat(
              projectManagerEntity
                  .getPrimaryKey()
                  .getInvolvedAttributes()
                  .get(0)
                  .getBelongingEntity()
                  .getName())
          .isEqualTo("PROJECT_MANAGER");

      // relationship, primary and foreign key check
      assertThat(regularEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(managerEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(countryEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
      assertThat(regularEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(employeeEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(projectManagerEntity.getInCanonicalRelationships().size()).isEqualTo(0);
      assertThat(managerEntity.getInCanonicalRelationships().size()).isEqualTo(1);
      assertThat(countryEntity.getInCanonicalRelationships().size()).isEqualTo(0);

      assertThat(regularEmployeeEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(contractEmployeeEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(employeeEntity.getForeignKeys().size()).isEqualTo(1);
      assertThat(projectManagerEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(managerEntity.getForeignKeys().size()).isEqualTo(0);
      assertThat(countryEntity.getForeignKeys().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship currentEmpRel = itEmp.next();
      assertThat(currentEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentEmpRel.getForeignKey()).isEqualTo(employeeEntity.getForeignKeys().get(0));
      assertThat(itEmp.hasNext()).isFalse();

      Iterator<CanonicalRelationship> itManager =
          managerEntity.getInCanonicalRelationships().iterator();
      CanonicalRelationship currentManRel = itManager.next();
      assertThat(currentManRel).isEqualTo(currentEmpRel);

      // inherited relationships check
      assertThat(regularEmployeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritedOutCanonicalRelationships().size())
          .isEqualTo(1);
      assertThat(employeeEntity.getInheritedOutCanonicalRelationships().size()).isEqualTo(0);

      Iterator<CanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      Iterator<CanonicalRelationship> itContEmp =
          contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      CanonicalRelationship currentRegEmpRel = itRegEmp.next();
      CanonicalRelationship currentContEmpRel = itContEmp.next();
      assertThat(currentRegEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentRegEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentContEmpRel.getParentEntity().getName()).isEqualTo("MANAGER");
      assertThat(currentContEmpRel.getForeignEntity().getName()).isEqualTo("EMPLOYEE");
      assertThat(currentRegEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentRegEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentRegEmpRel.getFromColumns().get(0).getName()).isEqualTo("MANAGER");
      assertThat(currentContEmpRel.getPrimaryKey()).isEqualTo(managerEntity.getPrimaryKey());
      assertThat(currentContEmpRel.getFromColumns().size()).isEqualTo(1);
      assertThat(currentContEmpRel.getFromColumns().get(0).getName()).isEqualTo("MANAGER");
      assertThat(itRegEmp.hasNext()).isFalse();
      assertThat(itContEmp.hasNext()).isFalse();

      // inheritance check
      assertThat(regularEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(contractEmployeeEntity.getParentEntity()).isEqualTo(employeeEntity);
      assertThat(employeeEntity.getParentEntity()).isNull();

      assertThat(regularEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(contractEmployeeEntity.getInheritanceLevel()).isEqualTo(1);
      assertThat(employeeEntity.getInheritanceLevel()).isEqualTo(0);

      // Hierarchical Bag check
      assertThat(mapper.getDataBaseSchema().getHierarchicalBags().size()).isEqualTo(2);

      HierarchicalBag hierarchicalBag1 = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      HierarchicalBag hierarchicalBag2 = mapper.getDataBaseSchema().getHierarchicalBags().get(1);
      assertThat(hierarchicalBag1.getInheritancePattern()).isEqualTo("table-per-concrete-type");
      assertThat(hierarchicalBag2.getInheritancePattern()).isEqualTo("table-per-concrete-type");

      assertThat(hierarchicalBag1.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag1.getDepth2entities().get(0).size()).isEqualTo(1);
      Iterator<Entity> it = hierarchicalBag1.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag1.getDepth2entities().get(1).size()).isEqualTo(2);
      it = hierarchicalBag1.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("REGULAR_EMPLOYEE");
      assertThat(it.next().getName()).isEqualTo("CONTRACT_EMPLOYEE");
      assertThat(it.hasNext()).isFalse();

      assertThat(employeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);
      assertThat(regularEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);
      assertThat(contractEmployeeEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag1);

      assertThat(hierarchicalBag1.getDiscriminatorColumn()).isNull();

      assertThat(hierarchicalBag2.getDepth2entities().size()).isEqualTo(2);

      assertThat(hierarchicalBag2.getDepth2entities().get(0).size()).isEqualTo(1);
      it = hierarchicalBag2.getDepth2entities().get(0).iterator();
      assertThat(it.next().getName()).isEqualTo("MANAGER");
      assertThat(it.hasNext()).isFalse();

      assertThat(hierarchicalBag2.getDepth2entities().get(1).size()).isEqualTo(1);
      it = hierarchicalBag2.getDepth2entities().get(1).iterator();
      assertThat(it.next().getName()).isEqualTo("PROJECT_MANAGER");
      assertThat(it.hasNext()).isFalse();

      assertThat(managerEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag2);
      assertThat(projectManagerEntity.getHierarchicalBag()).isEqualTo(hierarchicalBag2);

      assertThat(hierarchicalBag2.getDiscriminatorColumn()).isNull();

      /*
       *  Testing built graph model
       */

      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      VertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
      VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
      VertexType projectManagerVertexType =
          mapper.getGraphModel().getVertexTypeByName("ProjectManager");
      VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(6);
      assertThat(employeeVertexType).isNotNull();
      assertThat(regularEmployeeVertexType).isNotNull();
      assertThat(contractEmployeeVertexType).isNotNull();
      assertThat(countryVertexType).isNotNull();
      assertThat(managerVertexType).isNotNull();
      assertThat(projectManagerVertexType).isNotNull();
      assertThat(residenceVertexType).isNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(4);

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

      assertThat(employeeVertexType.getPropertyByName("manager")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("manager").getName()).isEqualTo("manager");
      assertThat(employeeVertexType.getPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("manager").getOrdinalPosition()).isEqualTo(4);
      assertThat(employeeVertexType.getPropertyByName("manager").isFromPrimaryKey()).isFalse();

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

      assertThat(countryVertexType.getProperties().size()).isEqualTo(3);

      assertThat(countryVertexType.getPropertyByName("id")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(countryVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(countryVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(countryVertexType.getPropertyByName("name")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(countryVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(countryVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(countryVertexType.getPropertyByName("continent")).isNotNull();
      assertThat(countryVertexType.getPropertyByName("continent").getName()).isEqualTo("continent");
      assertThat(countryVertexType.getPropertyByName("continent").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(countryVertexType.getPropertyByName("continent").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(countryVertexType.getPropertyByName("continent").isFromPrimaryKey()).isFalse();

      assertThat(managerVertexType.getProperties().size()).isEqualTo(2);

      assertThat(managerVertexType.getPropertyByName("id")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(managerVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(managerVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(managerVertexType.getPropertyByName("name")).isNotNull();
      assertThat(managerVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(managerVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(managerVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(managerVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(projectManagerVertexType.getProperties().size()).isEqualTo(1);

      assertThat(projectManagerVertexType.getPropertyByName("project")).isNotNull();
      assertThat(projectManagerVertexType.getPropertyByName("project").getName())
          .isEqualTo("project");
      assertThat(projectManagerVertexType.getPropertyByName("project").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getPropertyByName("project").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerVertexType.getPropertyByName("project").isFromPrimaryKey())
          .isFalse();

      // inherited properties check
      assertThat(employeeVertexType.getInheritedProperties().size()).isEqualTo(0);

      assertThat(regularEmployeeVertexType.getInheritedProperties().size()).isEqualTo(4);

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

      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager")).isNotNull();
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").getName())
          .isEqualTo("manager");
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              regularEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(regularEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey())
          .isFalse();

      assertThat(contractEmployeeVertexType.getInheritedProperties().size()).isEqualTo(4);

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

      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager")).isNotNull();
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager").getName())
          .isEqualTo("manager");
      assertThat(contractEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(
              contractEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey())
          .isFalse();

      assertThat(projectManagerVertexType.getInheritedProperties().size()).isEqualTo(2);

      assertThat(projectManagerVertexType.getInheritedPropertyByName("id")).isNotNull();
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getName())
          .isEqualTo("id");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(projectManagerVertexType.getInheritedPropertyByName("id").isFromPrimaryKey())
          .isTrue();

      assertThat(projectManagerVertexType.getInheritedPropertyByName("name")).isNotNull();
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getName())
          .isEqualTo("name");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(projectManagerVertexType.getInheritedPropertyByName("name").isFromPrimaryKey())
          .isFalse();

      assertThat(countryVertexType.getInheritedProperties().size()).isEqualTo(0);
      assertThat(managerVertexType.getInheritedProperties().size()).isEqualTo(0);

      // edges check

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);

      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(mapper.getGraphModel().getEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(employeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(employeeVertexType.getOutEdgesType().get(0).getName()).isEqualTo("HasManager");

      assertThat(regularEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(regularEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasManager");

      assertThat(contractEmployeeVertexType.getOutEdgesType().size()).isEqualTo(1);
      assertThat(contractEmployeeVertexType.getOutEdgesType().get(0).getName())
          .isEqualTo("HasManager");

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(6);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(6);

      assertThat(mapper.getEVClassMappersByVertex(employeeVertexType).size()).isEqualTo(1);
      EVClassMapper employeeClassMapper =
          mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).get(0))
          .isEqualTo(employeeClassMapper);
      assertThat(employeeEntity).isEqualTo(employeeClassMapper.getEntity());
      assertThat(employeeVertexType).isEqualTo(employeeClassMapper.getVertexType());

      assertThat(employeeClassMapper.getAttribute2property().size()).isEqualTo(4);
      assertThat(employeeClassMapper.getProperty2attribute().size()).isEqualTo(4);
      assertThat(employeeClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(employeeClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(employeeClassMapper.getAttribute2property().get("RESIDENCE"))
          .isEqualTo("residence");
      assertThat(employeeClassMapper.getAttribute2property().get("MANAGER")).isEqualTo("manager");
      assertThat(employeeClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(employeeClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(employeeClassMapper.getProperty2attribute().get("residence"))
          .isEqualTo("RESIDENCE");
      assertThat(employeeClassMapper.getProperty2attribute().get("manager")).isEqualTo("MANAGER");

      assertThat(mapper.getEVClassMappersByVertex(regularEmployeeVertexType).size()).isEqualTo(1);
      EVClassMapper regularEmployeeClassMapper =
          mapper.getEVClassMappersByVertex(regularEmployeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(regularEmployeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(regularEmployeeEntity).get(0))
          .isEqualTo(regularEmployeeClassMapper);
      assertThat(regularEmployeeEntity).isEqualTo(regularEmployeeClassMapper.getEntity());
      assertThat(regularEmployeeVertexType).isEqualTo(regularEmployeeClassMapper.getVertexType());

      assertThat(regularEmployeeClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(regularEmployeeClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(regularEmployeeClassMapper.getAttribute2property().get("SALARY"))
          .isEqualTo("salary");
      assertThat(regularEmployeeClassMapper.getAttribute2property().get("BONUS"))
          .isEqualTo("bonus");
      assertThat(regularEmployeeClassMapper.getProperty2attribute().get("salary"))
          .isEqualTo("SALARY");
      assertThat(regularEmployeeClassMapper.getProperty2attribute().get("bonus"))
          .isEqualTo("BONUS");

      assertThat(mapper.getEVClassMappersByVertex(contractEmployeeVertexType).size()).isEqualTo(1);
      EVClassMapper contractEmployeeClassMapper =
          mapper.getEVClassMappersByVertex(contractEmployeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(contractEmployeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(contractEmployeeEntity).get(0))
          .isEqualTo(contractEmployeeClassMapper);
      assertThat(contractEmployeeEntity).isEqualTo(contractEmployeeClassMapper.getEntity());
      assertThat(contractEmployeeVertexType).isEqualTo(contractEmployeeClassMapper.getVertexType());

      assertThat(contractEmployeeClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(contractEmployeeClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(contractEmployeeClassMapper.getAttribute2property().get("PAY_PER_HOUR"))
          .isEqualTo("payPerHour");
      assertThat(contractEmployeeClassMapper.getAttribute2property().get("CONTRACT_DURATION"))
          .isEqualTo("contractDuration");
      assertThat(contractEmployeeClassMapper.getProperty2attribute().get("payPerHour"))
          .isEqualTo("PAY_PER_HOUR");
      assertThat(contractEmployeeClassMapper.getProperty2attribute().get("contractDuration"))
          .isEqualTo("CONTRACT_DURATION");

      assertThat(mapper.getEVClassMappersByVertex(countryVertexType).size()).isEqualTo(1);
      EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(countryEntity).get(0))
          .isEqualTo(countryClassMapper);
      assertThat(countryEntity).isEqualTo(countryClassMapper.getEntity());
      assertThat(countryVertexType).isEqualTo(countryClassMapper.getVertexType());

      assertThat(countryClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(countryClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(countryClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(countryClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(countryClassMapper.getAttribute2property().get("CONTINENT"))
          .isEqualTo("continent");
      assertThat(countryClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(countryClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(countryClassMapper.getProperty2attribute().get("continent"))
          .isEqualTo("CONTINENT");

      assertThat(mapper.getEVClassMappersByVertex(managerVertexType).size()).isEqualTo(1);
      EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(managerEntity).get(0))
          .isEqualTo(managerClassMapper);
      assertThat(managerEntity).isEqualTo(managerClassMapper.getEntity());
      assertThat(managerVertexType).isEqualTo(managerClassMapper.getVertexType());

      assertThat(managerClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(managerClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(managerClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(managerClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(managerClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(managerClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");

      assertThat(mapper.getEVClassMappersByVertex(projectManagerVertexType).size()).isEqualTo(1);
      EVClassMapper projectManagerClassMapper =
          mapper.getEVClassMappersByVertex(projectManagerVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(projectManagerEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(projectManagerEntity).get(0))
          .isEqualTo(projectManagerClassMapper);
      assertThat(projectManagerEntity).isEqualTo(projectManagerClassMapper.getEntity());
      assertThat(projectManagerVertexType).isEqualTo(projectManagerClassMapper.getVertexType());

      assertThat(projectManagerClassMapper.getAttribute2property().size()).isEqualTo(1);
      assertThat(projectManagerClassMapper.getProperty2attribute().size()).isEqualTo(1);
      assertThat(projectManagerClassMapper.getAttribute2property().get("PROJECT"))
          .isEqualTo("project");
      assertThat(projectManagerClassMapper.getProperty2attribute().get("project"))
          .isEqualTo("PROJECT");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> itRelationships =
          employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasManagerRelationship = itRelationships.next();
      assertThat(itRelationships.hasNext()).isFalse();

      EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);
      assertThat(mapper.getRelationship2edgeType().get(hasManagerRelationship))
          .isEqualTo(hasManagerEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(hasManagerEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(hasManagerEdgeType)
                  .contains(hasManagerRelationship))
          .isTrue();

      // JoinVertexes-AggregatorEdges Mapping

      assertThat(mapper.getJoinVertex2aggregatorEdges().size()).isEqualTo(0);
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
}
