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
 * Copyright (C) 2018 - 2019 ArcadeAnalytics
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Gabriele Ponzi
*/

public class FilterTableMappingTest {

    private final static String XML_TABLE_PER_CLASS = "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerClassHierarchyImportTest.xml";
    private final static String XML_TABLE_PER_SUBCLASS1 = "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerSubclassImportTest1.xml";
    private final static String XML_TABLE_PER_SUBCLASS2 = "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerSubclassImportTest2.xml";
    private final static String XML_TABLE_PER_CONCRETE_CLASS = "src/test/resources/provider/rdbms/inheritance/hibernate/tablePerConcreteClassImportTest.xml";
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
    public void init() {
        this.dataSource = new DataSourceInfo(
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
                "",
                22,
                "",
                false
        );

        dbQueryEngine = new DBQueryEngine(dataSource, 300);
        executionStrategy = "not_specified";
        nameResolver = new JavaConventionNameResolver();
        dataTypeHandler = new HSQLDBDataTypeHandler();
        statistics = new Statistics();

    }

    @Test
    /*
     * Filtering out a table through include-tables (without inheritance).
     */ public void filterOutThroughIncludeWithNoInheritance() {

        Connection connection = null;
        Statement st = null;

        try {

            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
            st = connection.createStatement();
            st.execute(countryTableBuilding);

            String residenceTableBuilding =
                    "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), "
                            + "primary key (ID), foreign key (COUNTRY) references COUNTRY(ID))";
            st.execute(residenceTableBuilding);

            String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT varchar(256), primary key (ID))";
            st.execute(managerTableBuilding);

            String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
                    + " NAME varchar(256), SALARY decimal(10,2), RESIDENCE varchar(256), MANAGER varchar(256), "
                    + "primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
            st.execute(employeeTableBuilding);

            // Records Inserting

            String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
            st.execute(countryFilling);

            String residenceFilling =
                    "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','C001')," + "('R002','Milan','C001'))";
            st.execute(residenceFilling);

            String managerFilling = "insert into MANAGER (ID,NAME,PROJECT) values (" + "('M001','Bill Right','New World'))";
            st.execute(managerFilling);

            String employeeFilling =
                    "insert into EMPLOYEE (ID,NAME,SALARY,RESIDENCE,MANAGER) values (" + "('E001','John Black',1500.00,'R001',null),"
                            + "('E002','Andrew Brown','1000.00','R001','M001')," + "('E003','Jack Johnson',2000.00,'R002',null))";
            st.execute(employeeFilling);

            List<String> includedTables = new ArrayList<String>();
            includedTables.add("COUNTRY");
            includedTables.add("MANAGER");
            includedTables.add("EMPLOYEE");

            this.mapper = new ER2GraphMapper(dataSource, includedTables, null, dbQueryEngine, dataTypeHandler, executionStrategy, nameResolver, statistics);
            mapper.buildSourceDatabaseSchema();
            mapper.buildGraphModel(new JavaConventionNameResolver());


            /*
             *  Testing context information
             */

            assertEquals(3, statistics.totalNumberOfEntities);
            assertEquals(3, statistics.builtEntities);
            assertEquals(1, statistics.totalNumberOfRelationships);
            assertEquals(1, statistics.builtRelationships);

            assertEquals(3, statistics.totalNumberOfModelVertices);
            assertEquals(3, statistics.builtModelVertexTypes);
            assertEquals(1, statistics.totalNumberOfModelEdges);
            assertEquals(1, statistics.builtModelEdgeTypes);


            /*
             *  Testing built source db schema
             */

            Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
            Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
            Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
            Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

            // entities check
            assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
            assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
            assertNotNull(employeeEntity);
            assertNotNull(countryEntity);
            assertNotNull(managerEntity);
            assertNull(residenceEntity);

            // attributes check
            assertEquals(5, employeeEntity.getAttributes().size());

            assertNotNull(employeeEntity.getAttributeByName("ID"));
            assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("NAME"));
            assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("SALARY"));
            assertEquals("SALARY", employeeEntity.getAttributeByName("SALARY").getName());
            assertEquals("DECIMAL", employeeEntity.getAttributeByName("SALARY").getDataType());
            assertEquals(3, employeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
            assertEquals(4, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("MANAGER"));
            assertEquals("MANAGER", employeeEntity.getAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("MANAGER").getDataType());
            assertEquals(5, employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(3, countryEntity.getAttributes().size());

            assertNotNull(countryEntity.getAttributeByName("ID"));
            assertEquals("ID", countryEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, countryEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("NAME"));
            assertEquals("NAME", countryEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, countryEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("CONTINENT"));
            assertEquals("CONTINENT", countryEntity.getAttributeByName("CONTINENT").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("CONTINENT").getDataType());
            assertEquals(3, countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName());

            assertEquals(3, managerEntity.getAttributes().size());

            assertNotNull(managerEntity.getAttributeByName("ID"));
            assertEquals("ID", managerEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, managerEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(managerEntity.getAttributeByName("NAME"));
            assertEquals("NAME", managerEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, managerEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(managerEntity.getAttributeByName("PROJECT"));
            assertEquals("PROJECT", managerEntity.getAttributeByName("PROJECT").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("PROJECT").getDataType());
            assertEquals(3, managerEntity.getAttributeByName("PROJECT").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

            // relationship, primary and foreign key check
            assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
            assertEquals(0, managerEntity.getOutCanonicalRelationships().size());
            assertEquals(0, countryEntity.getOutCanonicalRelationships().size());
            assertEquals(0, employeeEntity.getInCanonicalRelationships().size());
            assertEquals(1, managerEntity.getInCanonicalRelationships().size());
            assertEquals(0, countryEntity.getOutCanonicalRelationships().size());
            assertEquals(1, employeeEntity.getForeignKeys().size());

            Iterator<CanonicalRelationship> itEmp = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentEmpRel = itEmp.next();
            assertEquals("MANAGER", currentEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
            assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
            assertFalse(itEmp.hasNext());

            Iterator<CanonicalRelationship> itManager = managerEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentManRel = itManager.next();
            assertEquals(currentEmpRel, currentManRel);


            /*
             *  Testing built graph model
             */

            VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
            VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
            VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
            VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

            // vertices check
            assertEquals(3, mapper.getGraphModel().getVerticesType().size());
            assertNotNull(employeeVertexType);
            assertNotNull(countryVertexType);
            assertNotNull(managerVertexType);
            assertNull(residenceVertexType);

            // properties check
            assertEquals(5, employeeVertexType.getProperties().size());

            assertNotNull(employeeVertexType.getPropertyByName("id"));
            assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("name"));
            assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("salary"));
            assertEquals("salary", employeeVertexType.getPropertyByName("salary").getName());
            assertEquals("DECIMAL", employeeVertexType.getPropertyByName("salary").getOriginalType());
            assertEquals(3, employeeVertexType.getPropertyByName("salary").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("residence"));
            assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
            assertEquals(4, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("manager"));
            assertEquals("manager", employeeVertexType.getPropertyByName("manager").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("manager").getOriginalType());
            assertEquals(5, employeeVertexType.getPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("manager").isFromPrimaryKey());

            assertEquals(3, countryVertexType.getProperties().size());

            assertNotNull(countryVertexType.getPropertyByName("id"));
            assertEquals("id", countryVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, countryVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, countryVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("name"));
            assertEquals("name", countryVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, countryVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("continent"));
            assertEquals("continent", countryVertexType.getPropertyByName("continent").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("continent").getOriginalType());
            assertEquals(3, countryVertexType.getPropertyByName("continent").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("continent").isFromPrimaryKey());

            assertEquals(3, managerVertexType.getProperties().size());

            assertNotNull(managerVertexType.getPropertyByName("id"));
            assertEquals("id", managerVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, managerVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, managerVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(managerVertexType.getPropertyByName("name"));
            assertEquals("name", managerVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, managerVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, managerVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(managerVertexType.getPropertyByName("project"));
            assertEquals("project", managerVertexType.getPropertyByName("project").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("project").getOriginalType());
            assertEquals(3, managerVertexType.getPropertyByName("project").getOrdinalPosition());
            assertEquals(false, managerVertexType.getPropertyByName("project").isFromPrimaryKey());

            // edges check

            assertEquals(1, mapper.getRelationship2edgeType().size());

            assertEquals(1, mapper.getGraphModel().getEdgesType().size());
            assertEquals("HasManager", mapper.getGraphModel().getEdgesType().get(0).getName());

            assertEquals(1, employeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", employeeVertexType.getOutEdgesType().get(0).getName());

            /*
             * Rules check
             */

            // Classes Mapping

            assertEquals(3, mapper.getVertexType2EVClassMappers().size());
            assertEquals(3, mapper.getEntity2EVClassMappers().size());

            assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
            EVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
            assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
            assertEquals(employeeClassMapper.getEntity(), employeeEntity);
            assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

            assertEquals(5, employeeClassMapper.getAttribute2property().size());
            assertEquals(5, employeeClassMapper.getProperty2attribute().size());
            assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", employeeClassMapper.getAttribute2property().get("NAME"));
            assertEquals("salary", employeeClassMapper.getAttribute2property().get("SALARY"));
            assertEquals("residence", employeeClassMapper.getAttribute2property().get("RESIDENCE"));
            assertEquals("manager", employeeClassMapper.getAttribute2property().get("MANAGER"));
            assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", employeeClassMapper.getProperty2attribute().get("name"));
            assertEquals("SALARY", employeeClassMapper.getProperty2attribute().get("salary"));
            assertEquals("RESIDENCE", employeeClassMapper.getProperty2attribute().get("residence"));
            assertEquals("MANAGER", employeeClassMapper.getProperty2attribute().get("manager"));

            assertEquals(1, mapper.getEVClassMappersByVertex(countryVertexType).size());
            EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(countryEntity).size());
            assertEquals(countryClassMapper, mapper.getEVClassMappersByEntity(countryEntity).get(0));
            assertEquals(countryClassMapper.getEntity(), countryEntity);
            assertEquals(countryClassMapper.getVertexType(), countryVertexType);

            assertEquals(3, countryClassMapper.getAttribute2property().size());
            assertEquals(3, countryClassMapper.getProperty2attribute().size());
            assertEquals("id", countryClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", countryClassMapper.getAttribute2property().get("NAME"));
            assertEquals("continent", countryClassMapper.getAttribute2property().get("CONTINENT"));
            assertEquals("ID", countryClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", countryClassMapper.getProperty2attribute().get("name"));
            assertEquals("CONTINENT", countryClassMapper.getProperty2attribute().get("continent"));

            assertEquals(1, mapper.getEVClassMappersByVertex(managerVertexType).size());
            EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(managerEntity).size());
            assertEquals(managerClassMapper, mapper.getEVClassMappersByEntity(managerEntity).get(0));
            assertEquals(managerClassMapper.getEntity(), managerEntity);
            assertEquals(managerClassMapper.getVertexType(), managerVertexType);

            assertEquals(3, managerClassMapper.getAttribute2property().size());
            assertEquals(3, managerClassMapper.getProperty2attribute().size());
            assertEquals("id", managerClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", managerClassMapper.getAttribute2property().get("NAME"));
            assertEquals("project", managerClassMapper.getAttribute2property().get("PROJECT"));
            assertEquals("ID", managerClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", managerClassMapper.getProperty2attribute().get("name"));
            assertEquals("PROJECT", managerClassMapper.getProperty2attribute().get("project"));

            // Relationships-Edges Mapping

            Iterator<CanonicalRelationship> it = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship hasManagerRelationship = it.next();
            assertFalse(it.hasNext());

            EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

            assertEquals(1, mapper.getRelationship2edgeType().size());
            assertEquals(hasManagerEdgeType, mapper.getRelationship2edgeType().get(hasManagerRelationship));

            assertEquals(1, mapper.getEdgeType2relationships().size());
            assertEquals(1, mapper.getEdgeType2relationships().get(hasManagerEdgeType).size());
            assertTrue(mapper.getEdgeType2relationships().get(hasManagerEdgeType).contains(hasManagerRelationship));

            // JoinVertexes-AggregatorEdges Mapping

            assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {

                // Dropping Source DB Schema and OrientGraph
                String dbDropping = "drop schema public cascade";
                st.execute(dbDropping);
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    @Test
    /*
     * Filtering out a table through exclude-tables (without inheritance).
     */ public void filterOutThroughExcludeWithNoInheritance() {

        Connection connection = null;
        Statement st = null;

        try {

            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
            st = connection.createStatement();
            st.execute(countryTableBuilding);

            String residenceTableBuilding =
                    "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), "
                            + "primary key (ID), foreign key (COUNTRY) references COUNTRY(ID))";
            st.execute(residenceTableBuilding);

            String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT varchar(256), primary key (ID))";
            st.execute(managerTableBuilding);

            String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
                    + " NAME varchar(256), SALARY decimal(10,2), RESIDENCE varchar(256), MANAGER varchar(256), "
                    + "primary key (id), foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
            st.execute(employeeTableBuilding);

            // Records Inserting

            String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
            st.execute(countryFilling);

            String residenceFilling =
                    "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','C001')," + "('R002','Milan','C001'))";
            st.execute(residenceFilling);

            String managerFilling = "insert into MANAGER (ID,NAME,PROJECT) values (" + "('M001','Bill Right','New World'))";
            st.execute(managerFilling);

            String employeeFilling =
                    "insert into EMPLOYEE (ID,NAME,SALARY,RESIDENCE,MANAGER) values (" + "('E001','John Black',1500.00,'R001',null),"
                            + "('E002','Andrew Brown','1000.00','R001','M001')," + "('E003','Jack Johnson',2000.00,'R002',null))";
            st.execute(employeeFilling);

            List<String> excludedTables = new ArrayList<String>();
            excludedTables.add("RESIDENCE");

            this.mapper = new ER2GraphMapper(dataSource, null, excludedTables, dbQueryEngine, dataTypeHandler, executionStrategy, nameResolver, statistics);

            mapper.buildSourceDatabaseSchema();
            mapper.buildGraphModel(new JavaConventionNameResolver());


            /*
             *  Testing context information
             */

            assertEquals(3, statistics.totalNumberOfEntities);
            assertEquals(3, statistics.builtEntities);
            assertEquals(1, statistics.totalNumberOfRelationships);
            assertEquals(1, statistics.builtRelationships);

            assertEquals(3, statistics.totalNumberOfModelVertices);
            assertEquals(3, statistics.builtModelVertexTypes);
            assertEquals(1, statistics.totalNumberOfModelEdges);
            assertEquals(1, statistics.builtModelEdgeTypes);


            /*
             *  Testing built source db schema
             */

            Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
            Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
            Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
            Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

            // entities check
            assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
            assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
            assertNotNull(employeeEntity);
            assertNotNull(countryEntity);
            assertNotNull(managerEntity);
            assertNull(residenceEntity);

            // attributes check
            assertEquals(5, employeeEntity.getAttributes().size());

            assertNotNull(employeeEntity.getAttributeByName("ID"));
            assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("NAME"));
            assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("SALARY"));
            assertEquals("SALARY", employeeEntity.getAttributeByName("SALARY").getName());
            assertEquals("DECIMAL", employeeEntity.getAttributeByName("SALARY").getDataType());
            assertEquals(3, employeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
            assertEquals(4, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("MANAGER"));
            assertEquals("MANAGER", employeeEntity.getAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("MANAGER").getDataType());
            assertEquals(5, employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(3, countryEntity.getAttributes().size());

            assertNotNull(countryEntity.getAttributeByName("ID"));
            assertEquals("ID", countryEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, countryEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("NAME"));
            assertEquals("NAME", countryEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, countryEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("CONTINENT"));
            assertEquals("CONTINENT", countryEntity.getAttributeByName("CONTINENT").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("CONTINENT").getDataType());
            assertEquals(3, countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName());

            assertEquals(3, managerEntity.getAttributes().size());

            assertNotNull(managerEntity.getAttributeByName("ID"));
            assertEquals("ID", managerEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, managerEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(managerEntity.getAttributeByName("NAME"));
            assertEquals("NAME", managerEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, managerEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(managerEntity.getAttributeByName("PROJECT"));
            assertEquals("PROJECT", managerEntity.getAttributeByName("PROJECT").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("PROJECT").getDataType());
            assertEquals(3, managerEntity.getAttributeByName("PROJECT").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

            // relationship, primary and foreign key check
            assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
            assertEquals(0, managerEntity.getOutCanonicalRelationships().size());
            assertEquals(0, employeeEntity.getInCanonicalRelationships().size());
            assertEquals(1, managerEntity.getInCanonicalRelationships().size());
            assertEquals(1, employeeEntity.getForeignKeys().size());

            Iterator<CanonicalRelationship> itEmp = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentEmpRel = itEmp.next();
            assertEquals("MANAGER", currentEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
            assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
            assertFalse(itEmp.hasNext());

            Iterator<CanonicalRelationship> itManager = managerEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentManRel = itManager.next();
            assertEquals(currentEmpRel, currentManRel);


            /*
             *  Testing built graph model
             */

            VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
            VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
            VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
            VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

            // vertices check
            assertEquals(3, mapper.getGraphModel().getVerticesType().size());
            assertNotNull(employeeVertexType);
            assertNotNull(countryVertexType);
            assertNotNull(managerVertexType);
            assertNull(residenceVertexType);

            // properties check
            assertEquals(5, employeeVertexType.getProperties().size());

            assertNotNull(employeeVertexType.getPropertyByName("id"));
            assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("name"));
            assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("salary"));
            assertEquals("salary", employeeVertexType.getPropertyByName("salary").getName());
            assertEquals("DECIMAL", employeeVertexType.getPropertyByName("salary").getOriginalType());
            assertEquals(3, employeeVertexType.getPropertyByName("salary").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("residence"));
            assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
            assertEquals(4, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("manager"));
            assertEquals("manager", employeeVertexType.getPropertyByName("manager").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("manager").getOriginalType());
            assertEquals(5, employeeVertexType.getPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("manager").isFromPrimaryKey());

            assertEquals(3, countryVertexType.getProperties().size());

            assertNotNull(countryVertexType.getPropertyByName("id"));
            assertEquals("id", countryVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, countryVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, countryVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("name"));
            assertEquals("name", countryVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, countryVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("continent"));
            assertEquals("continent", countryVertexType.getPropertyByName("continent").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("continent").getOriginalType());
            assertEquals(3, countryVertexType.getPropertyByName("continent").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("continent").isFromPrimaryKey());

            assertEquals(3, managerVertexType.getProperties().size());

            assertNotNull(managerVertexType.getPropertyByName("id"));
            assertEquals("id", managerVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, managerVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, managerVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(managerVertexType.getPropertyByName("name"));
            assertEquals("name", managerVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, managerVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, managerVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(managerVertexType.getPropertyByName("project"));
            assertEquals("project", managerVertexType.getPropertyByName("project").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("project").getOriginalType());
            assertEquals(3, managerVertexType.getPropertyByName("project").getOrdinalPosition());
            assertEquals(false, managerVertexType.getPropertyByName("project").isFromPrimaryKey());

            // edges check

            assertEquals(1, mapper.getRelationship2edgeType().size());

            assertEquals(1, mapper.getGraphModel().getEdgesType().size());
            assertEquals("HasManager", mapper.getGraphModel().getEdgesType().get(0).getName());

            assertEquals(1, employeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", employeeVertexType.getOutEdgesType().get(0).getName());

            /*
             * Rules check
             */

            // Classes Mapping

            assertEquals(3, mapper.getVertexType2EVClassMappers().size());
            assertEquals(3, mapper.getEntity2EVClassMappers().size());

            assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
            EVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
            assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
            assertEquals(employeeClassMapper.getEntity(), employeeEntity);
            assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

            assertEquals(5, employeeClassMapper.getAttribute2property().size());
            assertEquals(5, employeeClassMapper.getProperty2attribute().size());
            assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", employeeClassMapper.getAttribute2property().get("NAME"));
            assertEquals("salary", employeeClassMapper.getAttribute2property().get("SALARY"));
            assertEquals("residence", employeeClassMapper.getAttribute2property().get("RESIDENCE"));
            assertEquals("manager", employeeClassMapper.getAttribute2property().get("MANAGER"));
            assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", employeeClassMapper.getProperty2attribute().get("name"));
            assertEquals("SALARY", employeeClassMapper.getProperty2attribute().get("salary"));
            assertEquals("RESIDENCE", employeeClassMapper.getProperty2attribute().get("residence"));
            assertEquals("MANAGER", employeeClassMapper.getProperty2attribute().get("manager"));

            assertEquals(1, mapper.getEVClassMappersByVertex(countryVertexType).size());
            EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(countryEntity).size());
            assertEquals(countryClassMapper, mapper.getEVClassMappersByEntity(countryEntity).get(0));
            assertEquals(countryClassMapper.getEntity(), countryEntity);
            assertEquals(countryClassMapper.getVertexType(), countryVertexType);

            assertEquals(3, countryClassMapper.getAttribute2property().size());
            assertEquals(3, countryClassMapper.getProperty2attribute().size());
            assertEquals("id", countryClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", countryClassMapper.getAttribute2property().get("NAME"));
            assertEquals("continent", countryClassMapper.getAttribute2property().get("CONTINENT"));
            assertEquals("ID", countryClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", countryClassMapper.getProperty2attribute().get("name"));
            assertEquals("CONTINENT", countryClassMapper.getProperty2attribute().get("continent"));

            assertEquals(1, mapper.getEVClassMappersByVertex(managerVertexType).size());
            EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(managerEntity).size());
            assertEquals(managerClassMapper, mapper.getEVClassMappersByEntity(managerEntity).get(0));
            assertEquals(managerClassMapper.getEntity(), managerEntity);
            assertEquals(managerClassMapper.getVertexType(), managerVertexType);

            assertEquals(3, managerClassMapper.getAttribute2property().size());
            assertEquals(3, managerClassMapper.getProperty2attribute().size());
            assertEquals("id", managerClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", managerClassMapper.getAttribute2property().get("NAME"));
            assertEquals("project", managerClassMapper.getAttribute2property().get("PROJECT"));
            assertEquals("ID", managerClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", managerClassMapper.getProperty2attribute().get("name"));
            assertEquals("PROJECT", managerClassMapper.getProperty2attribute().get("project"));

            // Relationships-Edges Mapping

            Iterator<CanonicalRelationship> it = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship hasManagerRelationship = it.next();
            assertFalse(it.hasNext());

            EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

            assertEquals(1, mapper.getRelationship2edgeType().size());
            assertEquals(hasManagerEdgeType, mapper.getRelationship2edgeType().get(hasManagerRelationship));

            assertEquals(1, mapper.getEdgeType2relationships().size());
            assertEquals(1, mapper.getEdgeType2relationships().get(hasManagerEdgeType).size());
            assertTrue(mapper.getEdgeType2relationships().get(hasManagerEdgeType).contains(hasManagerRelationship));

            // JoinVertexes-AggregatorEdges Mapping

            assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {

                // Dropping Source DB Schema and OrientGraph
                String dbDropping = "drop schema public cascade";
                st.execute(dbDropping);
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    @Test
    /*
     * Filtering out a table through include-tables (with Table per Hierarchy inheritance).
     */ public void filterOutThroughIncludeWithTablePerHierarchyInheritance() {

        Connection connection = null;
        Statement st = null;

        try {

            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
            st = connection.createStatement();
            st.execute(countryTableBuilding);

            String residenceTableBuilding =
                    "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), "
                            + "primary key (ID), foreign key (COUNTRY) references COUNTRY(ID))";
            st.execute(residenceTableBuilding);

            String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, TYPE varchar(256), NAME varchar(256), PROJECT varchar(256), primary key (ID))";
            st.execute(managerTableBuilding);

            String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
                    + " TYPE varchar(256), NAME varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), "
                    + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), "
                    + "primary key (id), foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
            st.execute(employeeTableBuilding);

            // Records Inserting

            String countryFilling = "insert into country (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
            st.execute(countryFilling);

            String residenceFilling =
                    "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','C001')," + "('R002','Milan','C001'))";
            st.execute(residenceFilling);

            String managerFilling =
                    "insert into MANAGER (ID,TYPE,NAME,PROJECT) values (" + "('M001','prj_mgr','Bill Right','New World'))";
            st.execute(managerFilling);

            String employeeFilling =
                    "insert into EMPLOYEE (ID,TYPE,NAME,SALARY,BONUS,PAY_PER_HOUR,CONTRACT_DURATION,RESIDENCE,MANAGER) values ("
                            + "('E001','emp','John Black',null,null,null,null,'R001',null),"
                            + "('E002','reg_emp','Andrew Brown','1000.00','10',null,null,'R001','M001'),"
                            + "('E003','cont_emp','Jack Johnson',null,null,'50.00','6','R002',null))";
            st.execute(employeeFilling);

            List<String> includedTables = new ArrayList<String>();
            includedTables.add("COUNTRY");
            includedTables.add("MANAGER");
            includedTables.add("EMPLOYEE");


            this.mapper = new Hibernate2GraphMapper(dataSource, FilterTableMappingTest.XML_TABLE_PER_CLASS, includedTables, null,
                dbQueryEngine, dataTypeHandler, executionStrategy, nameResolver, statistics);
            mapper.buildSourceDatabaseSchema();
            mapper.buildGraphModel(new JavaConventionNameResolver());


            /*
             *  Testing context information
             */

            assertEquals(3, statistics.totalNumberOfEntities);
            assertEquals(3, statistics.builtEntities);
            assertEquals(1, statistics.totalNumberOfRelationships);
            assertEquals(1, statistics.builtRelationships);

            assertEquals(6, statistics.totalNumberOfModelVertices);
            assertEquals(6, statistics.builtModelVertexTypes);
            assertEquals(1, statistics.totalNumberOfModelEdges);
            assertEquals(1, statistics.builtModelEdgeTypes);


            /*
             *  Testing built source db schema
             */

            Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
            Entity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("REGULAR_EMPLOYEE");
            Entity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("CONTRACT_EMPLOYEE");
            Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
            Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
            Entity projectManagerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("PROJECT_MANAGER");
            Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

            // entities check
            assertEquals(6, mapper.getDataBaseSchema().getEntities().size());
            assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
            assertNotNull(employeeEntity);
            assertNotNull(regularEmployeeEntity);
            assertNotNull(contractEmployeeEntity);
            assertNotNull(countryEntity);
            assertNotNull(managerEntity);
            assertNull(residenceEntity);

            // attributes check
            assertEquals(4, employeeEntity.getAttributes().size());

            assertNotNull(employeeEntity.getAttributeByName("ID"));
            assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("NAME"));
            assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("MANAGER"));
            assertEquals("MANAGER", employeeEntity.getAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("MANAGER").getDataType());
            assertEquals(4, employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(2, regularEmployeeEntity.getAttributes().size());

            assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
            assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
            assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
            assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
            assertEquals("Regular_Employee", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
            assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
            assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
            assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
            assertEquals("Regular_Employee", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

            assertEquals(2, contractEmployeeEntity.getAttributes().size());

            assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
            assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
            assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
            assertEquals(1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
            assertEquals("Contract_Employee", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
            assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
            assertEquals(2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
            assertEquals("Contract_Employee",
                    contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

            assertEquals(3, countryEntity.getAttributes().size());

            assertNotNull(countryEntity.getAttributeByName("ID"));
            assertEquals("ID", countryEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, countryEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("NAME"));
            assertEquals("NAME", countryEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, countryEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("CONTINENT"));
            assertEquals("CONTINENT", countryEntity.getAttributeByName("CONTINENT").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("CONTINENT").getDataType());
            assertEquals(3, countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName());

            assertEquals(2, managerEntity.getAttributes().size());

            assertNotNull(managerEntity.getAttributeByName("ID"));
            assertEquals("ID", managerEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, managerEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(managerEntity.getAttributeByName("NAME"));
            assertEquals("NAME", managerEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, managerEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertEquals(1, projectManagerEntity.getAttributes().size());

            assertNotNull(projectManagerEntity.getAttributeByName("PROJECT"));
            assertEquals("PROJECT", projectManagerEntity.getAttributeByName("PROJECT").getName());
            assertEquals("VARCHAR", projectManagerEntity.getAttributeByName("PROJECT").getDataType());
            assertEquals(1, projectManagerEntity.getAttributeByName("PROJECT").getOrdinalPosition());
            assertEquals("Project_Manager", projectManagerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

            // inherited attributes check
            assertEquals(0, employeeEntity.getInheritedAttributes().size());

            assertEquals(4, regularEmployeeEntity.getInheritedAttributes().size());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("MANAGER"));
            assertEquals("MANAGER", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType());
            assertEquals(4, regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(4, contractEmployeeEntity.getInheritedAttributes().size());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("MANAGER"));
            assertEquals("MANAGER", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType());
            assertEquals(4, contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(2, projectManagerEntity.getInheritedAttributes().size());

            assertNotNull(projectManagerEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", projectManagerEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", projectManagerEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, projectManagerEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", projectManagerEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(projectManagerEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", projectManagerEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", projectManagerEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, projectManagerEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", projectManagerEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertEquals(0, countryEntity.getInheritedAttributes().size());
            assertEquals(0, managerEntity.getInheritedAttributes().size());

            // primary key check
            assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("ID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("ID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("EMPLOYEE",
                    contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            assertEquals(1, projectManagerEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("ID", projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("MANAGER", projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            // relationship, primary and foreign key check
            assertEquals(0, regularEmployeeEntity.getOutCanonicalRelationships().size());
            assertEquals(0, contractEmployeeEntity.getOutCanonicalRelationships().size());
            assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
            assertEquals(0, countryEntity.getOutCanonicalRelationships().size());
            assertEquals(0, managerEntity.getOutCanonicalRelationships().size());
            assertEquals(0, regularEmployeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, contractEmployeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, employeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, countryEntity.getInCanonicalRelationships().size());
            assertEquals(1, managerEntity.getInCanonicalRelationships().size());

            assertEquals(0, regularEmployeeEntity.getForeignKeys().size());
            assertEquals(0, contractEmployeeEntity.getForeignKeys().size());
            assertEquals(1, employeeEntity.getForeignKeys().size());
            assertEquals(0, countryEntity.getForeignKeys().size());
            assertEquals(0, managerEntity.getForeignKeys().size());

            Iterator<CanonicalRelationship> itEmp = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentEmpRel = itEmp.next();
            assertEquals("MANAGER", currentEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
            assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
            assertFalse(itEmp.hasNext());

            Iterator<CanonicalRelationship> itManager = managerEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentManRel = itManager.next();
            assertEquals(currentEmpRel, currentManRel);

            // inherited relationships check
            assertEquals(1, regularEmployeeEntity.getInheritedOutCanonicalRelationships().size());
            assertEquals(1, contractEmployeeEntity.getInheritedOutCanonicalRelationships().size());
            assertEquals(0, employeeEntity.getInheritedOutCanonicalRelationships().size());

            Iterator<CanonicalRelationship> itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
            Iterator<CanonicalRelationship> itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRegEmpRel = itRegEmp.next();
            CanonicalRelationship currentContEmpRel = itContEmp.next();
            assertEquals("MANAGER", currentRegEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
            assertEquals("MANAGER", currentContEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
            assertEquals(1, currentRegEmpRel.getFromColumns().size());
            assertEquals("MANAGER", currentRegEmpRel.getFromColumns().get(0).getName());
            assertEquals(managerEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
            assertEquals(1, currentContEmpRel.getFromColumns().size());
            assertEquals("MANAGER", currentContEmpRel.getFromColumns().get(0).getName());
            assertFalse(itRegEmp.hasNext());
            assertFalse(itContEmp.hasNext());

            // inheritance check
            assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
            assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
            assertNull(employeeEntity.getParentEntity());

            assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
            assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
            assertEquals(0, employeeEntity.getInheritanceLevel());

            // Hierarchical Bag check
            assertEquals(2, mapper.getDataBaseSchema().getHierarchicalBags().size());

            HierarchicalBag hierarchicalBag1 = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
            HierarchicalBag hierarchicalBag2 = mapper.getDataBaseSchema().getHierarchicalBags().get(1);
            assertEquals("table-per-hierarchy", hierarchicalBag1.getInheritancePattern());
            assertEquals("table-per-hierarchy", hierarchicalBag2.getInheritancePattern());

            assertEquals(2, hierarchicalBag1.getDepth2entities().size());

            assertEquals(1, hierarchicalBag1.getDepth2entities().get(0).size());
            Iterator<Entity> it = hierarchicalBag1.getDepth2entities().get(0).iterator();
            assertEquals("EMPLOYEE", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(2, hierarchicalBag1.getDepth2entities().get(1).size());
            it = hierarchicalBag1.getDepth2entities().get(1).iterator();
            assertEquals("Regular_Employee", it.next().getName());
            assertEquals("Contract_Employee", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(hierarchicalBag1, employeeEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag1, regularEmployeeEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag1, contractEmployeeEntity.getHierarchicalBag());

            assertNotNull(hierarchicalBag1.getDiscriminatorColumn());
            assertEquals("TYPE", hierarchicalBag1.getDiscriminatorColumn());

            assertEquals(3, hierarchicalBag1.getEntityName2discriminatorValue().size());
            assertEquals("emp", hierarchicalBag1.getEntityName2discriminatorValue().get("EMPLOYEE"));
            assertEquals("reg_emp", hierarchicalBag1.getEntityName2discriminatorValue().get("Regular_Employee"));
            assertEquals("cont_emp", hierarchicalBag1.getEntityName2discriminatorValue().get("Contract_Employee"));

            assertEquals(2, hierarchicalBag2.getDepth2entities().size());

            assertEquals(1, hierarchicalBag2.getDepth2entities().get(0).size());
            it = hierarchicalBag2.getDepth2entities().get(0).iterator();
            assertEquals("MANAGER", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(1, hierarchicalBag2.getDepth2entities().get(1).size());
            it = hierarchicalBag2.getDepth2entities().get(1).iterator();
            assertEquals("Project_Manager", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(hierarchicalBag2, managerEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag2, projectManagerEntity.getHierarchicalBag());

            assertNotNull(hierarchicalBag2.getDiscriminatorColumn());
            assertEquals("TYPE", hierarchicalBag2.getDiscriminatorColumn());

            assertEquals(2, hierarchicalBag2.getEntityName2discriminatorValue().size());
            assertEquals("mgr", hierarchicalBag2.getEntityName2discriminatorValue().get("MANAGER"));
            assertEquals("prj_mgr", hierarchicalBag2.getEntityName2discriminatorValue().get("Project_Manager"));


            /*
             *  Testing built graph model
             */

            VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
            VertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
            VertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
            VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
            VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
            VertexType projectManagerVertexType = mapper.getGraphModel().getVertexTypeByName("ProjectManager");
            VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

            // vertices check
            assertEquals(6, mapper.getGraphModel().getVerticesType().size());
            assertNotNull(employeeVertexType);
            assertNotNull(regularEmployeeVertexType);
            assertNotNull(contractEmployeeVertexType);
            assertNotNull(countryVertexType);
            assertNotNull(managerVertexType);
            assertNotNull(projectManagerVertexType);
            assertNull(residenceVertexType);

            // properties check
            assertEquals(4, employeeVertexType.getProperties().size());

            assertNotNull(employeeVertexType.getPropertyByName("id"));
            assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("name"));
            assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("residence"));
            assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
            assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("manager"));
            assertEquals("manager", employeeVertexType.getPropertyByName("manager").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("manager").getOriginalType());
            assertEquals(4, employeeVertexType.getPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("manager").isFromPrimaryKey());

            assertEquals(2, regularEmployeeVertexType.getProperties().size());

            assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
            assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
            assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getOriginalType());
            assertEquals(1, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
            assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
            assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType());
            assertEquals(2, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

            assertEquals(2, contractEmployeeVertexType.getProperties().size());

            assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
            assertEquals("payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
            assertEquals("DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType());
            assertEquals(1, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
            assertEquals("contractDuration", contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType());
            assertEquals(2, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

            assertEquals(3, countryVertexType.getProperties().size());

            assertNotNull(countryVertexType.getPropertyByName("id"));
            assertEquals("id", countryVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, countryVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, countryVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("name"));
            assertEquals("name", countryVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, countryVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("continent"));
            assertEquals("continent", countryVertexType.getPropertyByName("continent").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("continent").getOriginalType());
            assertEquals(3, countryVertexType.getPropertyByName("continent").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("continent").isFromPrimaryKey());

            assertEquals(2, managerVertexType.getProperties().size());

            assertNotNull(managerVertexType.getPropertyByName("id"));
            assertEquals("id", managerVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, managerVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, managerVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(managerVertexType.getPropertyByName("name"));
            assertEquals("name", managerVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, managerVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, managerVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertEquals(1, projectManagerVertexType.getProperties().size());

            assertNotNull(projectManagerVertexType.getPropertyByName("project"));
            assertEquals("project", projectManagerVertexType.getPropertyByName("project").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getPropertyByName("project").getOriginalType());
            assertEquals(1, projectManagerVertexType.getPropertyByName("project").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getPropertyByName("project").isFromPrimaryKey());

            // inherited properties check
            assertEquals(0, employeeVertexType.getInheritedProperties().size());

            assertEquals(4, regularEmployeeVertexType.getInheritedProperties().size());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(true, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
            assertEquals("residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
            assertEquals(3, regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("manager"));
            assertEquals("manager", regularEmployeeVertexType.getInheritedPropertyByName("manager").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType());
            assertEquals(4, regularEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey());

            assertEquals(4, contractEmployeeVertexType.getInheritedProperties().size());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(true, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
            assertEquals("residence", contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
            assertEquals(3, contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("manager"));
            assertEquals("manager", contractEmployeeVertexType.getInheritedPropertyByName("manager").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType());
            assertEquals(4, contractEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey());

            assertEquals(2, projectManagerVertexType.getInheritedProperties().size());

            assertNotNull(projectManagerVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", projectManagerVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, projectManagerVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(true, projectManagerVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(projectManagerVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", projectManagerVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, projectManagerVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertEquals(0, countryVertexType.getInheritedProperties().size());
            assertEquals(0, managerVertexType.getInheritedProperties().size());

            // edges check

            assertEquals(1, mapper.getRelationship2edgeType().size());

            assertEquals(1, mapper.getGraphModel().getEdgesType().size());
            assertEquals("HasManager", mapper.getGraphModel().getEdgesType().get(0).getName());

            assertEquals(1, employeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", employeeVertexType.getOutEdgesType().get(0).getName());

            assertEquals(1, regularEmployeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", regularEmployeeVertexType.getOutEdgesType().get(0).getName());

            assertEquals(1, contractEmployeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", contractEmployeeVertexType.getOutEdgesType().get(0).getName());

            /*
             * Rules check
             */

            // Classes Mapping

            assertEquals(6, mapper.getVertexType2EVClassMappers().size());
            assertEquals(6, mapper.getEntity2EVClassMappers().size());

            assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
            EVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
            assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
            assertEquals(employeeClassMapper.getEntity(), employeeEntity);
            assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

            assertEquals(4, employeeClassMapper.getAttribute2property().size());
            assertEquals(4, employeeClassMapper.getProperty2attribute().size());
            assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", employeeClassMapper.getAttribute2property().get("NAME"));
            assertEquals("residence", employeeClassMapper.getAttribute2property().get("RESIDENCE"));
            assertEquals("manager", employeeClassMapper.getAttribute2property().get("MANAGER"));
            assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", employeeClassMapper.getProperty2attribute().get("name"));
            assertEquals("RESIDENCE", employeeClassMapper.getProperty2attribute().get("residence"));
            assertEquals("MANAGER", employeeClassMapper.getProperty2attribute().get("manager"));

            assertEquals(1, mapper.getEVClassMappersByVertex(regularEmployeeVertexType).size());
            EVClassMapper regularEmployeeClassMapper = mapper.getEVClassMappersByVertex(regularEmployeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(regularEmployeeEntity).size());
            assertEquals(regularEmployeeClassMapper, mapper.getEVClassMappersByEntity(regularEmployeeEntity).get(0));
            assertEquals(regularEmployeeClassMapper.getEntity(), regularEmployeeEntity);
            assertEquals(regularEmployeeClassMapper.getVertexType(), regularEmployeeVertexType);

            assertEquals(2, regularEmployeeClassMapper.getAttribute2property().size());
            assertEquals(2, regularEmployeeClassMapper.getProperty2attribute().size());
            assertEquals("salary", regularEmployeeClassMapper.getAttribute2property().get("SALARY"));
            assertEquals("bonus", regularEmployeeClassMapper.getAttribute2property().get("BONUS"));
            assertEquals("SALARY", regularEmployeeClassMapper.getProperty2attribute().get("salary"));
            assertEquals("BONUS", regularEmployeeClassMapper.getProperty2attribute().get("bonus"));

            assertEquals(1, mapper.getEVClassMappersByVertex(contractEmployeeVertexType).size());
            EVClassMapper contractEmployeeClassMapper = mapper.getEVClassMappersByVertex(contractEmployeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(contractEmployeeEntity).size());
            assertEquals(contractEmployeeClassMapper, mapper.getEVClassMappersByEntity(contractEmployeeEntity).get(0));
            assertEquals(contractEmployeeClassMapper.getEntity(), contractEmployeeEntity);
            assertEquals(contractEmployeeClassMapper.getVertexType(), contractEmployeeVertexType);

            assertEquals(2, contractEmployeeClassMapper.getAttribute2property().size());
            assertEquals(2, contractEmployeeClassMapper.getProperty2attribute().size());
            assertEquals("payPerHour", contractEmployeeClassMapper.getAttribute2property().get("PAY_PER_HOUR"));
            assertEquals("contractDuration", contractEmployeeClassMapper.getAttribute2property().get("CONTRACT_DURATION"));
            assertEquals("PAY_PER_HOUR", contractEmployeeClassMapper.getProperty2attribute().get("payPerHour"));
            assertEquals("CONTRACT_DURATION", contractEmployeeClassMapper.getProperty2attribute().get("contractDuration"));

            assertEquals(1, mapper.getEVClassMappersByVertex(countryVertexType).size());
            EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(countryEntity).size());
            assertEquals(countryClassMapper, mapper.getEVClassMappersByEntity(countryEntity).get(0));
            assertEquals(countryClassMapper.getEntity(), countryEntity);
            assertEquals(countryClassMapper.getVertexType(), countryVertexType);

            assertEquals(3, countryClassMapper.getAttribute2property().size());
            assertEquals(3, countryClassMapper.getProperty2attribute().size());
            assertEquals("id", countryClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", countryClassMapper.getAttribute2property().get("NAME"));
            assertEquals("continent", countryClassMapper.getAttribute2property().get("CONTINENT"));
            assertEquals("ID", countryClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", countryClassMapper.getProperty2attribute().get("name"));
            assertEquals("CONTINENT", countryClassMapper.getProperty2attribute().get("continent"));

            assertEquals(1, mapper.getEVClassMappersByVertex(managerVertexType).size());
            EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(managerEntity).size());
            assertEquals(managerClassMapper, mapper.getEVClassMappersByEntity(managerEntity).get(0));
            assertEquals(managerClassMapper.getEntity(), managerEntity);
            assertEquals(managerClassMapper.getVertexType(), managerVertexType);

            assertEquals(2, managerClassMapper.getAttribute2property().size());
            assertEquals(2, managerClassMapper.getProperty2attribute().size());
            assertEquals("id", managerClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", managerClassMapper.getAttribute2property().get("NAME"));
            assertEquals("ID", managerClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", managerClassMapper.getProperty2attribute().get("name"));

            assertEquals(1, mapper.getEVClassMappersByVertex(projectManagerVertexType).size());
            EVClassMapper projectManagerClassMapper = mapper.getEVClassMappersByVertex(projectManagerVertexType).get(0);

            assertEquals(projectManagerClassMapper, mapper.getEVClassMappersByEntity(projectManagerEntity).get(0));
            assertEquals(projectManagerClassMapper.getEntity(), projectManagerEntity);
            assertEquals(projectManagerClassMapper.getVertexType(), projectManagerVertexType);

            assertEquals(1, projectManagerClassMapper.getAttribute2property().size());
            assertEquals(1, projectManagerClassMapper.getProperty2attribute().size());
            assertEquals("project", projectManagerClassMapper.getAttribute2property().get("PROJECT"));
            assertEquals("PROJECT", projectManagerClassMapper.getProperty2attribute().get("project"));

            // Relationships-Edges Mapping

            Iterator<CanonicalRelationship> itRelationships = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship hasManagerRelationship = itRelationships.next();
            assertFalse(itRelationships.hasNext());

            EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

            assertEquals(1, mapper.getRelationship2edgeType().size());
            assertEquals(hasManagerEdgeType, mapper.getRelationship2edgeType().get(hasManagerRelationship));

            assertEquals(1, mapper.getEdgeType2relationships().size());
            assertEquals(1, mapper.getEdgeType2relationships().get(hasManagerEdgeType).size());
            assertTrue(mapper.getEdgeType2relationships().get(hasManagerEdgeType).contains(hasManagerRelationship));

            // JoinVertexes-AggregatorEdges Mapping

            assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {

                // Dropping Source DB Schema and OrientGraph
                String dbDropping = "drop schema public cascade";
                st.execute(dbDropping);
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    @Test
    /*
     * Filtering out a table through include-tables (with Table per Type inheritance).
     */ public void filterOutThroughIncludeWithTablePerTypeInheritance() {

        Connection connection = null;
        Statement st = null;

        try {

            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
            st = connection.createStatement();
            st.execute(countryTableBuilding);

            String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
            st = connection.createStatement();
            st.execute(residenceTableBuilding);

            String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key (ID))";
            st.execute(managerTableBuilding);

            String projectManagerTableBuilding = "create memory table PROJECT_MANAGER(EID varchar(256) not null, PROJECT varchar(256), primary key (EID), foreign key (EID) references MANAGER(ID))";
            st.execute(projectManagerTableBuilding);

            String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
                    + " NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID), "
                    + "foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
            st.execute(employeeTableBuilding);

            String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
                    + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
            st.execute(regularEmployeeTableBuilding);

            String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
                    + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
            st.execute(contractEmployeeTableBuilding);

            // Records Inserting

            String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
            st.execute(countryFilling);

            String residenceFilling =
                    "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','C001')," + "('R002','Milan','C001'))";
            st.execute(residenceFilling);

            String managerFilling = "insert into MANAGER (ID,NAME) values (" + "('M001','Bill Right'))";
            st.execute(managerFilling);

            String projectManagerFilling = "insert into PROJECT_MANAGER (EID,PROJECT) values (" + "('M001','New World'))";
            st.execute(projectManagerFilling);

            String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values (" + "('E001','John Black','R001',null),"
                    + "('E002','Andrew Brown','R001','M001')," + "('E003','Jack Johnson','R002',null))";
            st.execute(employeeFilling);

            String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values (" + "('E002','1000.00','10'))";
            st.execute(regularEmployeeFilling);

            String contractEmployeeFilling =
                    "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values (" + "('E003','50.00','6'))";
            st.execute(contractEmployeeFilling);

            List<String> includedTables = new ArrayList<String>();
            includedTables.add("COUNTRY");
            includedTables.add("MANAGER");
            includedTables.add("PROJECT_MANAGER");
            includedTables.add("EMPLOYEE");
            includedTables.add("REGULAR_EMPLOYEE");
            includedTables.add("CONTRACT_EMPLOYEE");

            this.mapper = new Hibernate2GraphMapper(dataSource, FilterTableMappingTest.XML_TABLE_PER_SUBCLASS1, includedTables,
                    null, dbQueryEngine, dataTypeHandler, executionStrategy, nameResolver, statistics);
            mapper.buildSourceDatabaseSchema();
            mapper.buildGraphModel(new JavaConventionNameResolver());


            /*
             *  Testing context information
             */

            assertEquals(6, statistics.totalNumberOfEntities);
            assertEquals(6, statistics.builtEntities);
            assertEquals(4, statistics.totalNumberOfRelationships);
            assertEquals(4, statistics.builtRelationships); // 3 of these are hierarchical relationships

            assertEquals(6, statistics.totalNumberOfModelVertices);
            assertEquals(6, statistics.builtModelVertexTypes);
            assertEquals(1, statistics.totalNumberOfModelEdges);
            assertEquals(1, statistics.builtModelEdgeTypes);


            /*
             *  Testing built source db schema
             */

            Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
            Entity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("REGULAR_EMPLOYEE");
            Entity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("CONTRACT_EMPLOYEE");
            Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
            Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
            Entity projectManagerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("PROJECT_MANAGER");
            Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

            // entities check
            assertEquals(6, mapper.getDataBaseSchema().getEntities().size());
            assertEquals(4, mapper.getDataBaseSchema().getCanonicalRelationships().size());
            assertNotNull(employeeEntity);
            assertNotNull(regularEmployeeEntity);
            assertNotNull(contractEmployeeEntity);
            assertNotNull(countryEntity);
            assertNotNull(managerEntity);
            assertNull(residenceEntity);

            // attributes check
            assertEquals(4, employeeEntity.getAttributes().size());

            assertNotNull(employeeEntity.getAttributeByName("ID"));
            assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("NAME"));
            assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("MANAGER"));
            assertEquals("MANAGER", employeeEntity.getAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("MANAGER").getDataType());
            assertEquals(4, employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(2, regularEmployeeEntity.getAttributes().size());

            assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
            assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
            assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
            assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
            assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
            assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
            assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
            assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
            assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

            assertEquals(2, contractEmployeeEntity.getAttributes().size());

            assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
            assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
            assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
            assertEquals(1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
            assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
            assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
            assertEquals(2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
            assertEquals("CONTRACT_EMPLOYEE",
                    contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

            assertEquals(3, countryEntity.getAttributes().size());

            assertNotNull(countryEntity.getAttributeByName("ID"));
            assertEquals("ID", countryEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, countryEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("NAME"));
            assertEquals("NAME", countryEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, countryEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("CONTINENT"));
            assertEquals("CONTINENT", countryEntity.getAttributeByName("CONTINENT").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("CONTINENT").getDataType());
            assertEquals(3, countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName());

            assertEquals(2, managerEntity.getAttributes().size());

            assertNotNull(managerEntity.getAttributeByName("ID"));
            assertEquals("ID", managerEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, managerEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(managerEntity.getAttributeByName("NAME"));
            assertEquals("NAME", managerEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, managerEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertEquals(1, projectManagerEntity.getAttributes().size());

            assertNotNull(projectManagerEntity.getAttributeByName("PROJECT"));
            assertEquals("PROJECT", projectManagerEntity.getAttributeByName("PROJECT").getName());
            assertEquals("VARCHAR", projectManagerEntity.getAttributeByName("PROJECT").getDataType());
            assertEquals(1, projectManagerEntity.getAttributeByName("PROJECT").getOrdinalPosition());
            assertEquals("PROJECT_MANAGER", projectManagerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

            // inherited attributes check
            assertEquals(0, employeeEntity.getInheritedAttributes().size());

            assertEquals(4, regularEmployeeEntity.getInheritedAttributes().size());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("MANAGER"));
            assertEquals("MANAGER", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType());
            assertEquals(4, regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(4, contractEmployeeEntity.getInheritedAttributes().size());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("MANAGER"));
            assertEquals("MANAGER", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType());
            assertEquals(4, contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(2, projectManagerEntity.getInheritedAttributes().size());

            assertNotNull(projectManagerEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", projectManagerEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", projectManagerEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, projectManagerEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", projectManagerEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(projectManagerEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", projectManagerEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", projectManagerEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, projectManagerEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", projectManagerEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertEquals(0, countryEntity.getInheritedAttributes().size());
            assertEquals(0, managerEntity.getInheritedAttributes().size());

            // primary key check
            assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("EID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("REGULAR_EMPLOYEE",
                    regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("EID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("CONTRACT_EMPLOYEE",
                    contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            assertEquals(1, projectManagerEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("EID", projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("PROJECT_MANAGER",
                    projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            // relationship, primary and foreign key check
            assertEquals(1, regularEmployeeEntity.getOutCanonicalRelationships().size());
            assertEquals(1, contractEmployeeEntity.getOutCanonicalRelationships().size());
            assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
            assertEquals(1, projectManagerEntity.getOutCanonicalRelationships().size());
            assertEquals(0, managerEntity.getOutCanonicalRelationships().size());
            assertEquals(0, countryEntity.getOutCanonicalRelationships().size());
            assertEquals(0, regularEmployeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, contractEmployeeEntity.getInCanonicalRelationships().size());
            assertEquals(2, employeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, projectManagerEntity.getInCanonicalRelationships().size());
            assertEquals(2, managerEntity.getInCanonicalRelationships().size());
            assertEquals(0, countryEntity.getInCanonicalRelationships().size());

            assertEquals(1, regularEmployeeEntity.getForeignKeys().size());
            assertEquals(1, contractEmployeeEntity.getForeignKeys().size());
            assertEquals(1, employeeEntity.getForeignKeys().size());
            assertEquals(1, projectManagerEntity.getForeignKeys().size());
            assertEquals(0, managerEntity.getForeignKeys().size());
            assertEquals(0, countryEntity.getForeignKeys().size());

            Iterator<CanonicalRelationship> itEmp = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentEmpRel = itEmp.next();
            assertEquals("MANAGER", currentEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
            assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
            assertFalse(itEmp.hasNext());

            Iterator<CanonicalRelationship> itManager = managerEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentManRel = itManager.next();
            assertEquals(currentEmpRel, currentManRel);

            Iterator<CanonicalRelationship> itContEmp = contractEmployeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentContEmpRel = itContEmp.next();
            itEmp = employeeEntity.getInCanonicalRelationships().iterator();
            currentEmpRel = itEmp.next();
            assertEquals(currentContEmpRel, currentEmpRel);

            Iterator<CanonicalRelationship> itRegEmp = regularEmployeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRegEmpRel = itRegEmp.next();
            currentEmpRel = itEmp.next();
            assertEquals(currentRegEmpRel, currentEmpRel);

            // inherited relationships check
            assertEquals(1, regularEmployeeEntity.getInheritedOutCanonicalRelationships().size());
            assertEquals(1, contractEmployeeEntity.getInheritedOutCanonicalRelationships().size());
            assertEquals(0, employeeEntity.getInheritedOutCanonicalRelationships().size());

            itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
            itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
            currentRegEmpRel = itRegEmp.next();
            currentContEmpRel = itContEmp.next();
            assertEquals("MANAGER", currentRegEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
            assertEquals("MANAGER", currentContEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
            assertEquals(1, currentRegEmpRel.getFromColumns().size());
            assertEquals("MANAGER", currentRegEmpRel.getFromColumns().get(0).getName());
            assertEquals(managerEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
            assertEquals(1, currentContEmpRel.getFromColumns().size());
            assertEquals("MANAGER", currentContEmpRel.getFromColumns().get(0).getName());
            assertFalse(itRegEmp.hasNext());
            assertFalse(itContEmp.hasNext());

            // inheritance check
            assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
            assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
            assertNull(employeeEntity.getParentEntity());

            assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
            assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
            assertEquals(0, employeeEntity.getInheritanceLevel());

            // Hierarchical Bag check
            assertEquals(2, mapper.getDataBaseSchema().getHierarchicalBags().size());

            HierarchicalBag hierarchicalBag1 = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
            HierarchicalBag hierarchicalBag2 = mapper.getDataBaseSchema().getHierarchicalBags().get(1);
            assertEquals("table-per-type", hierarchicalBag1.getInheritancePattern());
            assertEquals("table-per-type", hierarchicalBag2.getInheritancePattern());

            assertEquals(2, hierarchicalBag1.getDepth2entities().size());

            assertEquals(1, hierarchicalBag1.getDepth2entities().get(0).size());
            Iterator<Entity> it = hierarchicalBag1.getDepth2entities().get(0).iterator();
            assertEquals("EMPLOYEE", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(2, hierarchicalBag1.getDepth2entities().get(1).size());
            it = hierarchicalBag1.getDepth2entities().get(1).iterator();
            assertEquals("REGULAR_EMPLOYEE", it.next().getName());
            assertEquals("CONTRACT_EMPLOYEE", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(hierarchicalBag1, employeeEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag1, regularEmployeeEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag1, contractEmployeeEntity.getHierarchicalBag());

            assertNull(hierarchicalBag1.getDiscriminatorColumn());

            assertEquals(2, hierarchicalBag2.getDepth2entities().size());

            assertEquals(1, hierarchicalBag2.getDepth2entities().get(0).size());
            it = hierarchicalBag2.getDepth2entities().get(0).iterator();
            assertEquals("MANAGER", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(1, hierarchicalBag2.getDepth2entities().get(1).size());
            it = hierarchicalBag2.getDepth2entities().get(1).iterator();
            assertEquals("PROJECT_MANAGER", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(hierarchicalBag2, managerEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag2, projectManagerEntity.getHierarchicalBag());

            assertNull(hierarchicalBag2.getDiscriminatorColumn());


            /*
             *  Testing built graph model
             */

            VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
            VertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
            VertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
            VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
            VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
            VertexType projectManagerVertexType = mapper.getGraphModel().getVertexTypeByName("ProjectManager");
            VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

            // vertices check
            assertEquals(6, mapper.getGraphModel().getVerticesType().size());
            assertNotNull(employeeVertexType);
            assertNotNull(regularEmployeeVertexType);
            assertNotNull(contractEmployeeVertexType);
            assertNotNull(countryVertexType);
            assertNotNull(managerVertexType);
            assertNotNull(projectManagerVertexType);
            assertNull(residenceVertexType);

            // properties check
            assertEquals(4, employeeVertexType.getProperties().size());

            assertNotNull(employeeVertexType.getPropertyByName("id"));
            assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("name"));
            assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("residence"));
            assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
            assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("manager"));
            assertEquals("manager", employeeVertexType.getPropertyByName("manager").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("manager").getOriginalType());
            assertEquals(4, employeeVertexType.getPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("manager").isFromPrimaryKey());

            assertEquals(2, regularEmployeeVertexType.getProperties().size());

            assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
            assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
            assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getOriginalType());
            assertEquals(1, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
            assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
            assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType());
            assertEquals(2, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

            assertEquals(2, contractEmployeeVertexType.getProperties().size());

            assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
            assertEquals("payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
            assertEquals("DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType());
            assertEquals(1, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
            assertEquals("contractDuration", contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType());
            assertEquals(2, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

            assertEquals(3, countryVertexType.getProperties().size());

            assertNotNull(countryVertexType.getPropertyByName("id"));
            assertEquals("id", countryVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, countryVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, countryVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("name"));
            assertEquals("name", countryVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, countryVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("continent"));
            assertEquals("continent", countryVertexType.getPropertyByName("continent").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("continent").getOriginalType());
            assertEquals(3, countryVertexType.getPropertyByName("continent").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("continent").isFromPrimaryKey());

            assertEquals(2, managerVertexType.getProperties().size());

            assertNotNull(managerVertexType.getPropertyByName("id"));
            assertEquals("id", managerVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, managerVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, managerVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(managerVertexType.getPropertyByName("name"));
            assertEquals("name", managerVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, managerVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, managerVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertEquals(1, projectManagerVertexType.getProperties().size());

            assertNotNull(projectManagerVertexType.getPropertyByName("project"));
            assertEquals("project", projectManagerVertexType.getPropertyByName("project").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getPropertyByName("project").getOriginalType());
            assertEquals(1, projectManagerVertexType.getPropertyByName("project").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getPropertyByName("project").isFromPrimaryKey());

            // inherited properties check
            assertEquals(0, employeeVertexType.getInheritedProperties().size());

            assertEquals(4, regularEmployeeVertexType.getInheritedProperties().size());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
            assertEquals("residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
            assertEquals(3, regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("manager"));
            assertEquals("manager", regularEmployeeVertexType.getInheritedPropertyByName("manager").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType());
            assertEquals(4, regularEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey());

            assertEquals(4, contractEmployeeVertexType.getInheritedProperties().size());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
            assertEquals("residence", contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
            assertEquals(3, contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("manager"));
            assertEquals("manager", contractEmployeeVertexType.getInheritedPropertyByName("manager").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType());
            assertEquals(4, contractEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey());

            assertEquals(2, projectManagerVertexType.getInheritedProperties().size());

            assertNotNull(projectManagerVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", projectManagerVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, projectManagerVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(projectManagerVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", projectManagerVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, projectManagerVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertEquals(0, countryVertexType.getInheritedProperties().size());
            assertEquals(0, managerVertexType.getInheritedProperties().size());

            // edges check

            assertEquals(1, mapper.getRelationship2edgeType().size());

            assertEquals(1, mapper.getGraphModel().getEdgesType().size());
            assertEquals("HasManager", mapper.getGraphModel().getEdgesType().get(0).getName());

            assertEquals(1, employeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", employeeVertexType.getOutEdgesType().get(0).getName());

            assertEquals(1, regularEmployeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", regularEmployeeVertexType.getOutEdgesType().get(0).getName());

            assertEquals(1, contractEmployeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", contractEmployeeVertexType.getOutEdgesType().get(0).getName());

            /*
             * Rules check
             */

            // Classes Mapping

            assertEquals(6, mapper.getVertexType2EVClassMappers().size());
            assertEquals(6, mapper.getEntity2EVClassMappers().size());

            assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
            EVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
            assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
            assertEquals(employeeClassMapper.getEntity(), employeeEntity);
            assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

            assertEquals(4, employeeClassMapper.getAttribute2property().size());
            assertEquals(4, employeeClassMapper.getProperty2attribute().size());
            assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", employeeClassMapper.getAttribute2property().get("NAME"));
            assertEquals("residence", employeeClassMapper.getAttribute2property().get("RESIDENCE"));
            assertEquals("manager", employeeClassMapper.getAttribute2property().get("MANAGER"));
            assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", employeeClassMapper.getProperty2attribute().get("name"));
            assertEquals("RESIDENCE", employeeClassMapper.getProperty2attribute().get("residence"));
            assertEquals("MANAGER", employeeClassMapper.getProperty2attribute().get("manager"));

            assertEquals(1, mapper.getEVClassMappersByVertex(regularEmployeeVertexType).size());
            EVClassMapper regularEmployeeClassMapper = mapper.getEVClassMappersByVertex(regularEmployeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(regularEmployeeEntity).size());
            assertEquals(regularEmployeeClassMapper, mapper.getEVClassMappersByEntity(regularEmployeeEntity).get(0));
            assertEquals(regularEmployeeClassMapper.getEntity(), regularEmployeeEntity);
            assertEquals(regularEmployeeClassMapper.getVertexType(), regularEmployeeVertexType);

            assertEquals(2, regularEmployeeClassMapper.getAttribute2property().size());
            assertEquals(2, regularEmployeeClassMapper.getProperty2attribute().size());
            assertEquals("salary", regularEmployeeClassMapper.getAttribute2property().get("SALARY"));
            assertEquals("bonus", regularEmployeeClassMapper.getAttribute2property().get("BONUS"));
            assertEquals("SALARY", regularEmployeeClassMapper.getProperty2attribute().get("salary"));
            assertEquals("BONUS", regularEmployeeClassMapper.getProperty2attribute().get("bonus"));

            assertEquals(1, mapper.getEVClassMappersByVertex(contractEmployeeVertexType).size());
            EVClassMapper contractEmployeeClassMapper = mapper.getEVClassMappersByVertex(contractEmployeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(contractEmployeeEntity).size());
            assertEquals(contractEmployeeClassMapper, mapper.getEVClassMappersByEntity(contractEmployeeEntity).get(0));
            assertEquals(contractEmployeeClassMapper.getEntity(), contractEmployeeEntity);
            assertEquals(contractEmployeeClassMapper.getVertexType(), contractEmployeeVertexType);

            assertEquals(2, contractEmployeeClassMapper.getAttribute2property().size());
            assertEquals(2, contractEmployeeClassMapper.getProperty2attribute().size());
            assertEquals("payPerHour", contractEmployeeClassMapper.getAttribute2property().get("PAY_PER_HOUR"));
            assertEquals("contractDuration", contractEmployeeClassMapper.getAttribute2property().get("CONTRACT_DURATION"));
            assertEquals("PAY_PER_HOUR", contractEmployeeClassMapper.getProperty2attribute().get("payPerHour"));
            assertEquals("CONTRACT_DURATION", contractEmployeeClassMapper.getProperty2attribute().get("contractDuration"));

            assertEquals(1, mapper.getEVClassMappersByVertex(countryVertexType).size());
            EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(countryEntity).size());
            assertEquals(countryClassMapper, mapper.getEVClassMappersByEntity(countryEntity).get(0));
            assertEquals(countryClassMapper.getEntity(), countryEntity);
            assertEquals(countryClassMapper.getVertexType(), countryVertexType);

            assertEquals(3, countryClassMapper.getAttribute2property().size());
            assertEquals(3, countryClassMapper.getProperty2attribute().size());
            assertEquals("id", countryClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", countryClassMapper.getAttribute2property().get("NAME"));
            assertEquals("continent", countryClassMapper.getAttribute2property().get("CONTINENT"));
            assertEquals("ID", countryClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", countryClassMapper.getProperty2attribute().get("name"));
            assertEquals("CONTINENT", countryClassMapper.getProperty2attribute().get("continent"));

            assertEquals(1, mapper.getEVClassMappersByVertex(managerVertexType).size());
            EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(managerEntity).size());
            assertEquals(managerClassMapper, mapper.getEVClassMappersByEntity(managerEntity).get(0));
            assertEquals(managerClassMapper.getEntity(), managerEntity);
            assertEquals(managerClassMapper.getVertexType(), managerVertexType);

            assertEquals(2, managerClassMapper.getAttribute2property().size());
            assertEquals(2, managerClassMapper.getProperty2attribute().size());
            assertEquals("id", managerClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", managerClassMapper.getAttribute2property().get("NAME"));
            assertEquals("ID", managerClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", managerClassMapper.getProperty2attribute().get("name"));

            assertEquals(1, mapper.getEVClassMappersByVertex(projectManagerVertexType).size());
            EVClassMapper projectManagerClassMapper = mapper.getEVClassMappersByVertex(projectManagerVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(projectManagerEntity).size());
            assertEquals(projectManagerClassMapper, mapper.getEVClassMappersByEntity(projectManagerEntity).get(0));
            assertEquals(projectManagerClassMapper.getEntity(), projectManagerEntity);
            assertEquals(projectManagerClassMapper.getVertexType(), projectManagerVertexType);

            assertEquals(1, projectManagerClassMapper.getAttribute2property().size());
            assertEquals(1, projectManagerClassMapper.getProperty2attribute().size());
            assertEquals("project", projectManagerClassMapper.getAttribute2property().get("PROJECT"));
            assertEquals("PROJECT", projectManagerClassMapper.getProperty2attribute().get("project"));

            // Relationships-Edges Mapping

            Iterator<CanonicalRelationship> itRelationships = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship hasManagerRelationship = itRelationships.next();
            assertFalse(itRelationships.hasNext());

            EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

            assertEquals(1, mapper.getRelationship2edgeType().size());
            assertEquals(hasManagerEdgeType, mapper.getRelationship2edgeType().get(hasManagerRelationship));

            assertEquals(1, mapper.getEdgeType2relationships().size());
            assertEquals(1, mapper.getEdgeType2relationships().get(hasManagerEdgeType).size());
            assertTrue(mapper.getEdgeType2relationships().get(hasManagerEdgeType).contains(hasManagerRelationship));

            // JoinVertexes-AggregatorEdges Mapping

            assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {

                // Dropping Source DB Schema and OrientGraph
                String dbDropping = "drop schema public cascade";
                st.execute(dbDropping);
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    @Test
    /*
     * Filtering out a table through exclude-tables (with Table per Type inheritance).
     */ public void filterOutThroughExcludeWithTablePerTypeInheritance() {

        Connection connection = null;
        Statement st = null;

        try {

            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
            st = connection.createStatement();
            st.execute(countryTableBuilding);

            String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
            st = connection.createStatement();
            st.execute(residenceTableBuilding);

            String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key (ID))";
            st.execute(managerTableBuilding);

            String projectManagerTableBuilding = "create memory table PROJECT_MANAGER(EID varchar(256) not null, PROJECT varchar(256), primary key (EID), foreign key (EID) references MANAGER(ID))";
            st.execute(projectManagerTableBuilding);

            String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
                    + " NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID), "
                    + "foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
            st.execute(employeeTableBuilding);

            String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
                    + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
            st.execute(regularEmployeeTableBuilding);

            String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
                    + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
            st.execute(contractEmployeeTableBuilding);

            // Records Inserting

            String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
            st.execute(countryFilling);

            String residenceFilling =
                    "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','C001')," + "('R002','Milan','C001'))";
            st.execute(residenceFilling);

            String managerFilling = "insert into MANAGER (ID,NAME) values (" + "('M001','Bill Right'))";
            st.execute(managerFilling);

            String projectManagerFilling = "insert into PROJECT_MANAGER (EID,PROJECT) values (" + "('M001','New World'))";
            st.execute(projectManagerFilling);

            String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values (" + "('E001','John Black','R001',null),"
                    + "('E002','Andrew Brown','R001','M001')," + "('E003','Jack Johnson','R002',null))";
            st.execute(employeeFilling);

            String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values (" + "('E002','1000.00','10'))";
            st.execute(regularEmployeeFilling);

            String contractEmployeeFilling =
                    "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values (" + "('E003','50.00','6'))";
            st.execute(contractEmployeeFilling);

            List<String> excludedTables = new ArrayList<String>();
            excludedTables.add("RESIDENCE");

            this.mapper = new Hibernate2GraphMapper(dataSource, FilterTableMappingTest.XML_TABLE_PER_SUBCLASS2, null,
                    excludedTables, dbQueryEngine, dataTypeHandler, executionStrategy, nameResolver, statistics);

            mapper.buildSourceDatabaseSchema();
            mapper.buildGraphModel(new JavaConventionNameResolver());


            /*
             *  Testing context information
             */

            assertEquals(6, statistics.totalNumberOfEntities);
            assertEquals(6, statistics.builtEntities);
            assertEquals(4, statistics.totalNumberOfRelationships);
            assertEquals(4, statistics.builtRelationships); // 3 of these are hierarchical relationships

            assertEquals(6, statistics.totalNumberOfModelVertices);
            assertEquals(6, statistics.builtModelVertexTypes);
            assertEquals(1, statistics.totalNumberOfModelEdges);
            assertEquals(1, statistics.builtModelEdgeTypes);


            /*
             *  Testing built source db schema
             */

            Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
            Entity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("REGULAR_EMPLOYEE");
            Entity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("CONTRACT_EMPLOYEE");
            Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
            Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
            Entity projectManagerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("PROJECT_MANAGER");
            Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

            // entities check
            assertEquals(6, mapper.getDataBaseSchema().getEntities().size());
            assertEquals(4, mapper.getDataBaseSchema().getCanonicalRelationships().size());
            assertNotNull(employeeEntity);
            assertNotNull(regularEmployeeEntity);
            assertNotNull(contractEmployeeEntity);
            assertNotNull(countryEntity);
            assertNotNull(managerEntity);
            assertNull(residenceEntity);

            // attributes check
            assertEquals(4, employeeEntity.getAttributes().size());

            assertNotNull(employeeEntity.getAttributeByName("ID"));
            assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("NAME"));
            assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("MANAGER"));
            assertEquals("MANAGER", employeeEntity.getAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("MANAGER").getDataType());
            assertEquals(4, employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(2, regularEmployeeEntity.getAttributes().size());

            assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
            assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
            assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
            assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
            assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
            assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
            assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
            assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
            assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

            assertEquals(2, contractEmployeeEntity.getAttributes().size());

            assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
            assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
            assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
            assertEquals(1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
            assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
            assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
            assertEquals(2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
            assertEquals("CONTRACT_EMPLOYEE",
                    contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

            assertEquals(3, countryEntity.getAttributes().size());

            assertNotNull(countryEntity.getAttributeByName("ID"));
            assertEquals("ID", countryEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, countryEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("NAME"));
            assertEquals("NAME", countryEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, countryEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("CONTINENT"));
            assertEquals("CONTINENT", countryEntity.getAttributeByName("CONTINENT").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("CONTINENT").getDataType());
            assertEquals(3, countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName());

            assertEquals(2, managerEntity.getAttributes().size());

            assertNotNull(managerEntity.getAttributeByName("ID"));
            assertEquals("ID", managerEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, managerEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(managerEntity.getAttributeByName("NAME"));
            assertEquals("NAME", managerEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, managerEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertEquals(1, projectManagerEntity.getAttributes().size());

            assertNotNull(projectManagerEntity.getAttributeByName("PROJECT"));
            assertEquals("PROJECT", projectManagerEntity.getAttributeByName("PROJECT").getName());
            assertEquals("VARCHAR", projectManagerEntity.getAttributeByName("PROJECT").getDataType());
            assertEquals(1, projectManagerEntity.getAttributeByName("PROJECT").getOrdinalPosition());
            assertEquals("PROJECT_MANAGER", projectManagerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

            // inherited attributes check
            assertEquals(0, employeeEntity.getInheritedAttributes().size());

            assertEquals(4, regularEmployeeEntity.getInheritedAttributes().size());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("MANAGER"));
            assertEquals("MANAGER", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType());
            assertEquals(4, regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(4, contractEmployeeEntity.getInheritedAttributes().size());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("MANAGER"));
            assertEquals("MANAGER", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType());
            assertEquals(4, contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(2, projectManagerEntity.getInheritedAttributes().size());

            assertNotNull(projectManagerEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", projectManagerEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", projectManagerEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, projectManagerEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", projectManagerEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(projectManagerEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", projectManagerEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", projectManagerEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, projectManagerEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", projectManagerEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertEquals(0, countryEntity.getInheritedAttributes().size());
            assertEquals(0, managerEntity.getInheritedAttributes().size());

            // primary key check
            assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("EID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("REGULAR_EMPLOYEE",
                    regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("EID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("CONTRACT_EMPLOYEE",
                    contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            assertEquals(1, projectManagerEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("EID", projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("PROJECT_MANAGER",
                    projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            // relationship, primary and foreign key check
            assertEquals(1, regularEmployeeEntity.getOutCanonicalRelationships().size());
            assertEquals(1, contractEmployeeEntity.getOutCanonicalRelationships().size());
            assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
            assertEquals(1, projectManagerEntity.getOutCanonicalRelationships().size());
            assertEquals(0, managerEntity.getOutCanonicalRelationships().size());
            assertEquals(0, countryEntity.getOutCanonicalRelationships().size());
            assertEquals(0, regularEmployeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, contractEmployeeEntity.getInCanonicalRelationships().size());
            assertEquals(2, employeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, projectManagerEntity.getInCanonicalRelationships().size());
            assertEquals(2, managerEntity.getInCanonicalRelationships().size());
            assertEquals(0, countryEntity.getInCanonicalRelationships().size());

            assertEquals(1, regularEmployeeEntity.getForeignKeys().size());
            assertEquals(1, contractEmployeeEntity.getForeignKeys().size());
            assertEquals(1, employeeEntity.getForeignKeys().size());
            assertEquals(1, projectManagerEntity.getForeignKeys().size());
            assertEquals(0, managerEntity.getForeignKeys().size());
            assertEquals(0, countryEntity.getForeignKeys().size());

            Iterator<CanonicalRelationship> itEmp = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentEmpRel = itEmp.next();
            assertEquals("MANAGER", currentEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
            assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
            assertFalse(itEmp.hasNext());

            Iterator<CanonicalRelationship> itManager = managerEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentManRel = itManager.next();
            assertEquals(currentEmpRel, currentManRel);

            Iterator<CanonicalRelationship> itContEmp = contractEmployeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentContEmpRel = itContEmp.next();
            itEmp = employeeEntity.getInCanonicalRelationships().iterator();
            currentEmpRel = itEmp.next();
            assertEquals(currentContEmpRel, currentEmpRel);

            Iterator<CanonicalRelationship> itRegEmp = regularEmployeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRegEmpRel = itRegEmp.next();
            currentEmpRel = itEmp.next();
            assertEquals(currentRegEmpRel, currentEmpRel);

            // inherited relationships check
            assertEquals(1, regularEmployeeEntity.getInheritedOutCanonicalRelationships().size());
            assertEquals(1, contractEmployeeEntity.getInheritedOutCanonicalRelationships().size());
            assertEquals(0, employeeEntity.getInheritedOutCanonicalRelationships().size());

            itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
            itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
            currentRegEmpRel = itRegEmp.next();
            currentContEmpRel = itContEmp.next();
            assertEquals("MANAGER", currentRegEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
            assertEquals("MANAGER", currentContEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
            assertEquals(1, currentRegEmpRel.getFromColumns().size());
            assertEquals("MANAGER", currentRegEmpRel.getFromColumns().get(0).getName());
            assertEquals(managerEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
            assertEquals(1, currentContEmpRel.getFromColumns().size());
            assertEquals("MANAGER", currentContEmpRel.getFromColumns().get(0).getName());
            assertFalse(itRegEmp.hasNext());
            assertFalse(itContEmp.hasNext());

            // inheritance check
            assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
            assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
            assertNull(employeeEntity.getParentEntity());

            assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
            assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
            assertEquals(0, employeeEntity.getInheritanceLevel());

            // Hierarchical Bag check
            assertEquals(2, mapper.getDataBaseSchema().getHierarchicalBags().size());

            HierarchicalBag hierarchicalBag1 = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
            HierarchicalBag hierarchicalBag2 = mapper.getDataBaseSchema().getHierarchicalBags().get(1);
            assertEquals("table-per-type", hierarchicalBag1.getInheritancePattern());
            assertEquals("table-per-type", hierarchicalBag2.getInheritancePattern());

            assertEquals(2, hierarchicalBag1.getDepth2entities().size());

            assertEquals(1, hierarchicalBag1.getDepth2entities().get(0).size());
            Iterator<Entity> it = hierarchicalBag1.getDepth2entities().get(0).iterator();
            assertEquals("EMPLOYEE", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(2, hierarchicalBag1.getDepth2entities().get(1).size());
            it = hierarchicalBag1.getDepth2entities().get(1).iterator();
            assertEquals("REGULAR_EMPLOYEE", it.next().getName());
            assertEquals("CONTRACT_EMPLOYEE", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(hierarchicalBag1, employeeEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag1, regularEmployeeEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag1, contractEmployeeEntity.getHierarchicalBag());

            assertNotNull(hierarchicalBag1.getDiscriminatorColumn());

            assertEquals(2, hierarchicalBag2.getDepth2entities().size());

            assertEquals(1, hierarchicalBag2.getDepth2entities().get(0).size());
            it = hierarchicalBag2.getDepth2entities().get(0).iterator();
            assertEquals("MANAGER", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(1, hierarchicalBag2.getDepth2entities().get(1).size());
            it = hierarchicalBag2.getDepth2entities().get(1).iterator();
            assertEquals("PROJECT_MANAGER", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(hierarchicalBag2, managerEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag2, projectManagerEntity.getHierarchicalBag());

            assertNotNull(hierarchicalBag2.getDiscriminatorColumn());


            /*
             *  Testing built graph model
             */

            VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
            VertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
            VertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
            VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
            VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
            VertexType projectManagerVertexType = mapper.getGraphModel().getVertexTypeByName("ProjectManager");
            VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

            // vertices check
            assertEquals(6, mapper.getGraphModel().getVerticesType().size());
            assertNotNull(employeeVertexType);
            assertNotNull(regularEmployeeVertexType);
            assertNotNull(contractEmployeeVertexType);
            assertNotNull(countryVertexType);
            assertNotNull(managerVertexType);
            assertNotNull(projectManagerVertexType);
            assertNull(residenceVertexType);

            // properties check
            assertEquals(4, employeeVertexType.getProperties().size());

            assertNotNull(employeeVertexType.getPropertyByName("id"));
            assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("name"));
            assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("residence"));
            assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
            assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("manager"));
            assertEquals("manager", employeeVertexType.getPropertyByName("manager").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("manager").getOriginalType());
            assertEquals(4, employeeVertexType.getPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("manager").isFromPrimaryKey());

            assertEquals(2, regularEmployeeVertexType.getProperties().size());

            assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
            assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
            assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getOriginalType());
            assertEquals(1, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
            assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
            assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType());
            assertEquals(2, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

            assertEquals(2, contractEmployeeVertexType.getProperties().size());

            assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
            assertEquals("payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
            assertEquals("DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType());
            assertEquals(1, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
            assertEquals("contractDuration", contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType());
            assertEquals(2, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

            assertEquals(3, countryVertexType.getProperties().size());

            assertNotNull(countryVertexType.getPropertyByName("id"));
            assertEquals("id", countryVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, countryVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, countryVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("name"));
            assertEquals("name", countryVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, countryVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("continent"));
            assertEquals("continent", countryVertexType.getPropertyByName("continent").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("continent").getOriginalType());
            assertEquals(3, countryVertexType.getPropertyByName("continent").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("continent").isFromPrimaryKey());

            assertEquals(2, managerVertexType.getProperties().size());

            assertNotNull(managerVertexType.getPropertyByName("id"));
            assertEquals("id", managerVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, managerVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, managerVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(managerVertexType.getPropertyByName("name"));
            assertEquals("name", managerVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, managerVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, managerVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertEquals(1, projectManagerVertexType.getProperties().size());

            assertNotNull(projectManagerVertexType.getPropertyByName("project"));
            assertEquals("project", projectManagerVertexType.getPropertyByName("project").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getPropertyByName("project").getOriginalType());
            assertEquals(1, projectManagerVertexType.getPropertyByName("project").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getPropertyByName("project").isFromPrimaryKey());

            // inherited properties check
            assertEquals(0, employeeVertexType.getInheritedProperties().size());

            assertEquals(4, regularEmployeeVertexType.getInheritedProperties().size());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
            assertEquals("residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
            assertEquals(3, regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("manager"));
            assertEquals("manager", regularEmployeeVertexType.getInheritedPropertyByName("manager").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType());
            assertEquals(4, regularEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey());

            assertEquals(4, contractEmployeeVertexType.getInheritedProperties().size());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
            assertEquals("residence", contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
            assertEquals(3, contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("manager"));
            assertEquals("manager", contractEmployeeVertexType.getInheritedPropertyByName("manager").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType());
            assertEquals(4, contractEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey());

            assertEquals(2, projectManagerVertexType.getInheritedProperties().size());

            assertNotNull(projectManagerVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", projectManagerVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, projectManagerVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(projectManagerVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", projectManagerVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, projectManagerVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertEquals(0, countryVertexType.getInheritedProperties().size());
            assertEquals(0, managerVertexType.getInheritedProperties().size());

            // edges check

            assertEquals(1, mapper.getRelationship2edgeType().size());

            assertEquals(1, mapper.getGraphModel().getEdgesType().size());
            assertEquals("HasManager", mapper.getGraphModel().getEdgesType().get(0).getName());

            assertEquals(1, employeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", employeeVertexType.getOutEdgesType().get(0).getName());

            assertEquals(1, regularEmployeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", regularEmployeeVertexType.getOutEdgesType().get(0).getName());

            assertEquals(1, contractEmployeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", contractEmployeeVertexType.getOutEdgesType().get(0).getName());

            /*
             * Rules check
             */

            // Classes Mapping

            assertEquals(6, mapper.getVertexType2EVClassMappers().size());
            assertEquals(6, mapper.getEntity2EVClassMappers().size());

            assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
            EVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
            assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
            assertEquals(employeeClassMapper.getEntity(), employeeEntity);
            assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

            assertEquals(4, employeeClassMapper.getAttribute2property().size());
            assertEquals(4, employeeClassMapper.getProperty2attribute().size());
            assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", employeeClassMapper.getAttribute2property().get("NAME"));
            assertEquals("residence", employeeClassMapper.getAttribute2property().get("RESIDENCE"));
            assertEquals("manager", employeeClassMapper.getAttribute2property().get("MANAGER"));
            assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", employeeClassMapper.getProperty2attribute().get("name"));
            assertEquals("RESIDENCE", employeeClassMapper.getProperty2attribute().get("residence"));
            assertEquals("MANAGER", employeeClassMapper.getProperty2attribute().get("manager"));

            assertEquals(1, mapper.getEVClassMappersByVertex(regularEmployeeVertexType).size());
            EVClassMapper regularEmployeeClassMapper = mapper.getEVClassMappersByVertex(regularEmployeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(regularEmployeeEntity).size());
            assertEquals(regularEmployeeClassMapper, mapper.getEVClassMappersByEntity(regularEmployeeEntity).get(0));
            assertEquals(regularEmployeeClassMapper.getEntity(), regularEmployeeEntity);
            assertEquals(regularEmployeeClassMapper.getVertexType(), regularEmployeeVertexType);

            assertEquals(2, regularEmployeeClassMapper.getAttribute2property().size());
            assertEquals(2, regularEmployeeClassMapper.getProperty2attribute().size());
            assertEquals("salary", regularEmployeeClassMapper.getAttribute2property().get("SALARY"));
            assertEquals("bonus", regularEmployeeClassMapper.getAttribute2property().get("BONUS"));
            assertEquals("SALARY", regularEmployeeClassMapper.getProperty2attribute().get("salary"));
            assertEquals("BONUS", regularEmployeeClassMapper.getProperty2attribute().get("bonus"));

            assertEquals(1, mapper.getEVClassMappersByVertex(contractEmployeeVertexType).size());
            EVClassMapper contractEmployeeClassMapper = mapper.getEVClassMappersByVertex(contractEmployeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(contractEmployeeEntity).size());
            assertEquals(contractEmployeeClassMapper, mapper.getEVClassMappersByEntity(contractEmployeeEntity).get(0));
            assertEquals(contractEmployeeClassMapper.getEntity(), contractEmployeeEntity);
            assertEquals(contractEmployeeClassMapper.getVertexType(), contractEmployeeVertexType);

            assertEquals(2, contractEmployeeClassMapper.getAttribute2property().size());
            assertEquals(2, contractEmployeeClassMapper.getProperty2attribute().size());
            assertEquals("payPerHour", contractEmployeeClassMapper.getAttribute2property().get("PAY_PER_HOUR"));
            assertEquals("contractDuration", contractEmployeeClassMapper.getAttribute2property().get("CONTRACT_DURATION"));
            assertEquals("PAY_PER_HOUR", contractEmployeeClassMapper.getProperty2attribute().get("payPerHour"));
            assertEquals("CONTRACT_DURATION", contractEmployeeClassMapper.getProperty2attribute().get("contractDuration"));

            assertEquals(1, mapper.getEVClassMappersByVertex(countryVertexType).size());
            EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(countryEntity).size());
            assertEquals(countryClassMapper, mapper.getEVClassMappersByEntity(countryEntity).get(0));
            assertEquals(countryClassMapper.getEntity(), countryEntity);
            assertEquals(countryClassMapper.getVertexType(), countryVertexType);

            assertEquals(3, countryClassMapper.getAttribute2property().size());
            assertEquals(3, countryClassMapper.getProperty2attribute().size());
            assertEquals("id", countryClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", countryClassMapper.getAttribute2property().get("NAME"));
            assertEquals("continent", countryClassMapper.getAttribute2property().get("CONTINENT"));
            assertEquals("ID", countryClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", countryClassMapper.getProperty2attribute().get("name"));
            assertEquals("CONTINENT", countryClassMapper.getProperty2attribute().get("continent"));

            assertEquals(1, mapper.getEVClassMappersByVertex(managerVertexType).size());
            EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(managerEntity).size());
            assertEquals(managerClassMapper, mapper.getEVClassMappersByEntity(managerEntity).get(0));
            assertEquals(managerClassMapper.getEntity(), managerEntity);
            assertEquals(managerClassMapper.getVertexType(), managerVertexType);

            assertEquals(2, managerClassMapper.getAttribute2property().size());
            assertEquals(2, managerClassMapper.getProperty2attribute().size());
            assertEquals("id", managerClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", managerClassMapper.getAttribute2property().get("NAME"));
            assertEquals("ID", managerClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", managerClassMapper.getProperty2attribute().get("name"));

            assertEquals(1, mapper.getEVClassMappersByVertex(projectManagerVertexType).size());
            EVClassMapper projectManagerClassMapper = mapper.getEVClassMappersByVertex(projectManagerVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(projectManagerEntity).size());
            assertEquals(projectManagerClassMapper, mapper.getEVClassMappersByEntity(projectManagerEntity).get(0));
            assertEquals(projectManagerClassMapper.getEntity(), projectManagerEntity);
            assertEquals(projectManagerClassMapper.getVertexType(), projectManagerVertexType);

            assertEquals(1, projectManagerClassMapper.getAttribute2property().size());
            assertEquals(1, projectManagerClassMapper.getProperty2attribute().size());
            assertEquals("project", projectManagerClassMapper.getAttribute2property().get("PROJECT"));
            assertEquals("PROJECT", projectManagerClassMapper.getProperty2attribute().get("project"));

            // Relationships-Edges Mapping

            Iterator<CanonicalRelationship> itRelationships = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship hasManagerRelationship = itRelationships.next();
            assertFalse(itRelationships.hasNext());

            EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

            assertEquals(1, mapper.getRelationship2edgeType().size());
            assertEquals(hasManagerEdgeType, mapper.getRelationship2edgeType().get(hasManagerRelationship));

            assertEquals(1, mapper.getEdgeType2relationships().size());
            assertEquals(1, mapper.getEdgeType2relationships().get(hasManagerEdgeType).size());
            assertTrue(mapper.getEdgeType2relationships().get(hasManagerEdgeType).contains(hasManagerRelationship));

            // JoinVertexes-AggregatorEdges Mapping

            assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {

                // Dropping Source DB Schema and OrientGraph
                String dbDropping = "drop schema public cascade";
                st.execute(dbDropping);
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    @Test
    /*
     * Filtering out a table through include-tables (with Table per Concrete Type inheritance).
     */ public void filterOutThroughIncludeWithTablePerConcreteTypeInheritance() {

        Connection connection = null;
        Statement st = null;

        try {

            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
            st = connection.createStatement();
            st.execute(countryTableBuilding);

            String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
            st = connection.createStatement();
            st.execute(residenceTableBuilding);

            String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key (ID))";
            st.execute(managerTableBuilding);

            String projectManagerTableBuilding = "create memory table PROJECT_MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT varchar(256), primary key (ID))";
            st.execute(projectManagerTableBuilding);

            String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
                    + " NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID), "
                    + "foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
            st.execute(employeeTableBuilding);

            String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (ID varchar(256) not null, "
                    + "NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256),"
                    + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (ID))";
            st.execute(regularEmployeeTableBuilding);

            String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (ID varchar(256) not null, "
                    + "NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256),"
                    + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (ID))";
            st.execute(contractEmployeeTableBuilding);

            // Records Inserting

            String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values (" + "('C001','Italy','Europe'))";
            st.execute(countryFilling);

            String residenceFilling =
                    "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','C001')," + "('R002','Milan','C001'))";
            st.execute(residenceFilling);

            String managerFilling = "insert into MANAGER (ID,NAME) values (" + "('M001','Bill Right'))";
            st.execute(managerFilling);

            String projectManagerFilling =
                    "insert into PROJECT_MANAGER (ID,NAME,PROJECT) values (" + "('M001','Bill Right','New World'))";
            st.execute(projectManagerFilling);

            String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values (" + "('E001','John Black','R001',null),"
                    + "('E002','Andrew Brown','R001','M001')," + "('E003','Jack Johnson','R002',null))";
            st.execute(employeeFilling);

            String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,SALARY,BONUS) values ("
                    + "('E002','Andrew Brown','R001','M001','1000.00','10'))";
            st.execute(regularEmployeeFilling);

            String contractEmployeeFilling =
                    "insert into CONTRACT_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,PAY_PER_HOUR,CONTRACT_DURATION) values ("
                            + "('E003','Jack Johnson','R002',null,'50.00','6'))";
            st.execute(contractEmployeeFilling);

            List<String> includedTables = new ArrayList<String>();
            includedTables.add("COUNTRY");
            includedTables.add("MANAGER");
            includedTables.add("PROJECT_MANAGER");
            includedTables.add("EMPLOYEE");
            includedTables.add("REGULAR_EMPLOYEE");
            includedTables.add("CONTRACT_EMPLOYEE");

            this.mapper = new Hibernate2GraphMapper(dataSource, FilterTableMappingTest.XML_TABLE_PER_CONCRETE_CLASS,
                    includedTables, null, dbQueryEngine, dataTypeHandler, executionStrategy, nameResolver, statistics);
            mapper.buildSourceDatabaseSchema();
            mapper.buildGraphModel(new JavaConventionNameResolver());


            /*
             *  Testing context information
             */

            assertEquals(6, statistics.totalNumberOfEntities);
            assertEquals(6, statistics.builtEntities);
            assertEquals(1, statistics.totalNumberOfRelationships);
            assertEquals(1, statistics.builtRelationships);

            assertEquals(6, statistics.totalNumberOfModelVertices);
            assertEquals(6, statistics.builtModelVertexTypes);
            assertEquals(1, statistics.totalNumberOfModelEdges);
            assertEquals(1, statistics.builtModelEdgeTypes);


            /*
             *  Testing built source db schema
             */

            Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
            Entity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("REGULAR_EMPLOYEE");
            Entity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("CONTRACT_EMPLOYEE");
            Entity countryEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("COUNTRY");
            Entity managerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("MANAGER");
            Entity projectManagerEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("PROJECT_MANAGER");
            Entity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

            // entities check
            assertEquals(6, mapper.getDataBaseSchema().getEntities().size());
            assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
            assertNotNull(employeeEntity);
            assertNotNull(regularEmployeeEntity);
            assertNotNull(contractEmployeeEntity);
            assertNotNull(countryEntity);
            assertNotNull(managerEntity);
            assertNull(residenceEntity);

            // attributes check
            assertEquals(4, employeeEntity.getAttributes().size());

            assertNotNull(employeeEntity.getAttributeByName("ID"));
            assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("NAME"));
            assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(employeeEntity.getAttributeByName("MANAGER"));
            assertEquals("MANAGER", employeeEntity.getAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", employeeEntity.getAttributeByName("MANAGER").getDataType());
            assertEquals(4, employeeEntity.getAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(2, regularEmployeeEntity.getAttributes().size());

            assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
            assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
            assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
            assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
            assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
            assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
            assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
            assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
            assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

            assertEquals(2, contractEmployeeEntity.getAttributes().size());

            assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
            assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
            assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
            assertEquals(1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
            assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
            assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
            assertEquals(2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
            assertEquals("CONTRACT_EMPLOYEE",
                    contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

            assertEquals(3, countryEntity.getAttributes().size());

            assertNotNull(countryEntity.getAttributeByName("ID"));
            assertEquals("ID", countryEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, countryEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("NAME"));
            assertEquals("NAME", countryEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, countryEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(countryEntity.getAttributeByName("CONTINENT"));
            assertEquals("CONTINENT", countryEntity.getAttributeByName("CONTINENT").getName());
            assertEquals("VARCHAR", countryEntity.getAttributeByName("CONTINENT").getDataType());
            assertEquals(3, countryEntity.getAttributeByName("CONTINENT").getOrdinalPosition());
            assertEquals("COUNTRY", countryEntity.getAttributeByName("CONTINENT").getBelongingEntity().getName());

            assertEquals(2, managerEntity.getAttributes().size());

            assertNotNull(managerEntity.getAttributeByName("ID"));
            assertEquals("ID", managerEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, managerEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(managerEntity.getAttributeByName("NAME"));
            assertEquals("NAME", managerEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", managerEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, managerEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", managerEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertEquals(1, projectManagerEntity.getAttributes().size());

            assertNotNull(projectManagerEntity.getAttributeByName("PROJECT"));
            assertEquals("PROJECT", projectManagerEntity.getAttributeByName("PROJECT").getName());
            assertEquals("VARCHAR", projectManagerEntity.getAttributeByName("PROJECT").getDataType());
            assertEquals(1, projectManagerEntity.getAttributeByName("PROJECT").getOrdinalPosition());
            assertEquals("PROJECT_MANAGER", projectManagerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

            // inherited attributes check
            assertEquals(0, employeeEntity.getInheritedAttributes().size());

            assertEquals(4, regularEmployeeEntity.getInheritedAttributes().size());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("MANAGER"));
            assertEquals("MANAGER", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType());
            assertEquals(4, regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(4, contractEmployeeEntity.getInheritedAttributes().size());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
            assertEquals("RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
            assertEquals(3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

            assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("MANAGER"));
            assertEquals("MANAGER", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getDataType());
            assertEquals(4, contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getOrdinalPosition());
            assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("MANAGER").getBelongingEntity().getName());

            assertEquals(2, projectManagerEntity.getInheritedAttributes().size());

            assertNotNull(projectManagerEntity.getInheritedAttributeByName("ID"));
            assertEquals("ID", projectManagerEntity.getInheritedAttributeByName("ID").getName());
            assertEquals("VARCHAR", projectManagerEntity.getInheritedAttributeByName("ID").getDataType());
            assertEquals(1, projectManagerEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
            assertEquals("MANAGER", projectManagerEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(projectManagerEntity.getInheritedAttributeByName("NAME"));
            assertEquals("NAME", projectManagerEntity.getInheritedAttributeByName("NAME").getName());
            assertEquals("VARCHAR", projectManagerEntity.getInheritedAttributeByName("NAME").getDataType());
            assertEquals(2, projectManagerEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
            assertEquals("MANAGER", projectManagerEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

            assertEquals(0, countryEntity.getInheritedAttributes().size());
            assertEquals(0, managerEntity.getInheritedAttributes().size());

            // primary key check
            assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("ID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("REGULAR_EMPLOYEE",
                    regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("ID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("CONTRACT_EMPLOYEE",
                    contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            assertEquals(1, projectManagerEntity.getPrimaryKey().getInvolvedAttributes().size());
            assertEquals("ID", projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
            assertEquals("VARCHAR", projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
            assertEquals("PROJECT_MANAGER",
                    projectManagerEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

            // relationship, primary and foreign key check
            assertEquals(0, regularEmployeeEntity.getOutCanonicalRelationships().size());
            assertEquals(0, contractEmployeeEntity.getOutCanonicalRelationships().size());
            assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
            assertEquals(0, projectManagerEntity.getOutCanonicalRelationships().size());
            assertEquals(0, managerEntity.getOutCanonicalRelationships().size());
            assertEquals(0, countryEntity.getOutCanonicalRelationships().size());
            assertEquals(0, regularEmployeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, contractEmployeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, employeeEntity.getInCanonicalRelationships().size());
            assertEquals(0, projectManagerEntity.getInCanonicalRelationships().size());
            assertEquals(1, managerEntity.getInCanonicalRelationships().size());
            assertEquals(0, countryEntity.getInCanonicalRelationships().size());

            assertEquals(0, regularEmployeeEntity.getForeignKeys().size());
            assertEquals(0, contractEmployeeEntity.getForeignKeys().size());
            assertEquals(1, employeeEntity.getForeignKeys().size());
            assertEquals(0, projectManagerEntity.getForeignKeys().size());
            assertEquals(0, managerEntity.getForeignKeys().size());
            assertEquals(0, countryEntity.getForeignKeys().size());

            Iterator<CanonicalRelationship> itEmp = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentEmpRel = itEmp.next();
            assertEquals("MANAGER", currentEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
            assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
            assertFalse(itEmp.hasNext());

            Iterator<CanonicalRelationship> itManager = managerEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentManRel = itManager.next();
            assertEquals(currentEmpRel, currentManRel);

            // inherited relationships check
            assertEquals(1, regularEmployeeEntity.getInheritedOutCanonicalRelationships().size());
            assertEquals(1, contractEmployeeEntity.getInheritedOutCanonicalRelationships().size());
            assertEquals(0, employeeEntity.getInheritedOutCanonicalRelationships().size());

            Iterator<CanonicalRelationship> itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
            Iterator<CanonicalRelationship> itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRegEmpRel = itRegEmp.next();
            CanonicalRelationship currentContEmpRel = itContEmp.next();
            assertEquals("MANAGER", currentRegEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
            assertEquals("MANAGER", currentContEmpRel.getParentEntity().getName());
            assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
            assertEquals(managerEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
            assertEquals(1, currentRegEmpRel.getFromColumns().size());
            assertEquals("MANAGER", currentRegEmpRel.getFromColumns().get(0).getName());
            assertEquals(managerEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
            assertEquals(1, currentContEmpRel.getFromColumns().size());
            assertEquals("MANAGER", currentContEmpRel.getFromColumns().get(0).getName());
            assertFalse(itRegEmp.hasNext());
            assertFalse(itContEmp.hasNext());

            // inheritance check
            assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
            assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
            assertNull(employeeEntity.getParentEntity());

            assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
            assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
            assertEquals(0, employeeEntity.getInheritanceLevel());

            // Hierarchical Bag check
            assertEquals(2, mapper.getDataBaseSchema().getHierarchicalBags().size());

            HierarchicalBag hierarchicalBag1 = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
            HierarchicalBag hierarchicalBag2 = mapper.getDataBaseSchema().getHierarchicalBags().get(1);
            assertEquals("table-per-concrete-type", hierarchicalBag1.getInheritancePattern());
            assertEquals("table-per-concrete-type", hierarchicalBag2.getInheritancePattern());

            assertEquals(2, hierarchicalBag1.getDepth2entities().size());

            assertEquals(1, hierarchicalBag1.getDepth2entities().get(0).size());
            Iterator<Entity> it = hierarchicalBag1.getDepth2entities().get(0).iterator();
            assertEquals("EMPLOYEE", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(2, hierarchicalBag1.getDepth2entities().get(1).size());
            it = hierarchicalBag1.getDepth2entities().get(1).iterator();
            assertEquals("REGULAR_EMPLOYEE", it.next().getName());
            assertEquals("CONTRACT_EMPLOYEE", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(hierarchicalBag1, employeeEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag1, regularEmployeeEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag1, contractEmployeeEntity.getHierarchicalBag());

            assertNull(hierarchicalBag1.getDiscriminatorColumn());

            assertEquals(2, hierarchicalBag2.getDepth2entities().size());

            assertEquals(1, hierarchicalBag2.getDepth2entities().get(0).size());
            it = hierarchicalBag2.getDepth2entities().get(0).iterator();
            assertEquals("MANAGER", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(1, hierarchicalBag2.getDepth2entities().get(1).size());
            it = hierarchicalBag2.getDepth2entities().get(1).iterator();
            assertEquals("PROJECT_MANAGER", it.next().getName());
            assertTrue(!it.hasNext());

            assertEquals(hierarchicalBag2, managerEntity.getHierarchicalBag());
            assertEquals(hierarchicalBag2, projectManagerEntity.getHierarchicalBag());

            assertNull(hierarchicalBag2.getDiscriminatorColumn());


            /*
             *  Testing built graph model
             */

            VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
            VertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
            VertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
            VertexType countryVertexType = mapper.getGraphModel().getVertexTypeByName("Country");
            VertexType managerVertexType = mapper.getGraphModel().getVertexTypeByName("Manager");
            VertexType projectManagerVertexType = mapper.getGraphModel().getVertexTypeByName("ProjectManager");
            VertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

            // vertices check
            assertEquals(6, mapper.getGraphModel().getVerticesType().size());
            assertNotNull(employeeVertexType);
            assertNotNull(regularEmployeeVertexType);
            assertNotNull(contractEmployeeVertexType);
            assertNotNull(countryVertexType);
            assertNotNull(managerVertexType);
            assertNotNull(projectManagerVertexType);
            assertNull(residenceVertexType);

            // properties check
            assertEquals(4, employeeVertexType.getProperties().size());

            assertNotNull(employeeVertexType.getPropertyByName("id"));
            assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("name"));
            assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("residence"));
            assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
            assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(employeeVertexType.getPropertyByName("manager"));
            assertEquals("manager", employeeVertexType.getPropertyByName("manager").getName());
            assertEquals("VARCHAR", employeeVertexType.getPropertyByName("manager").getOriginalType());
            assertEquals(4, employeeVertexType.getPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, employeeVertexType.getPropertyByName("manager").isFromPrimaryKey());

            assertEquals(2, regularEmployeeVertexType.getProperties().size());

            assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
            assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
            assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getOriginalType());
            assertEquals(1, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
            assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
            assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType());
            assertEquals(2, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

            assertEquals(2, contractEmployeeVertexType.getProperties().size());

            assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
            assertEquals("payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
            assertEquals("DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType());
            assertEquals(1, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
            assertEquals("contractDuration", contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType());
            assertEquals(2, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

            assertEquals(3, countryVertexType.getProperties().size());

            assertNotNull(countryVertexType.getPropertyByName("id"));
            assertEquals("id", countryVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, countryVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, countryVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("name"));
            assertEquals("name", countryVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, countryVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertNotNull(countryVertexType.getPropertyByName("continent"));
            assertEquals("continent", countryVertexType.getPropertyByName("continent").getName());
            assertEquals("VARCHAR", countryVertexType.getPropertyByName("continent").getOriginalType());
            assertEquals(3, countryVertexType.getPropertyByName("continent").getOrdinalPosition());
            assertEquals(false, countryVertexType.getPropertyByName("continent").isFromPrimaryKey());

            assertEquals(2, managerVertexType.getProperties().size());

            assertNotNull(managerVertexType.getPropertyByName("id"));
            assertEquals("id", managerVertexType.getPropertyByName("id").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("id").getOriginalType());
            assertEquals(1, managerVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, managerVertexType.getPropertyByName("id").isFromPrimaryKey());

            assertNotNull(managerVertexType.getPropertyByName("name"));
            assertEquals("name", managerVertexType.getPropertyByName("name").getName());
            assertEquals("VARCHAR", managerVertexType.getPropertyByName("name").getOriginalType());
            assertEquals(2, managerVertexType.getPropertyByName("name").getOrdinalPosition());
            assertEquals(false, managerVertexType.getPropertyByName("name").isFromPrimaryKey());

            assertEquals(1, projectManagerVertexType.getProperties().size());

            assertNotNull(projectManagerVertexType.getPropertyByName("project"));
            assertEquals("project", projectManagerVertexType.getPropertyByName("project").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getPropertyByName("project").getOriginalType());
            assertEquals(1, projectManagerVertexType.getPropertyByName("project").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getPropertyByName("project").isFromPrimaryKey());

            // inherited properties check
            assertEquals(0, employeeVertexType.getInheritedProperties().size());

            assertEquals(4, regularEmployeeVertexType.getInheritedProperties().size());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(true, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
            assertEquals("residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
            assertEquals(3, regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("manager"));
            assertEquals("manager", regularEmployeeVertexType.getInheritedPropertyByName("manager").getName());
            assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType());
            assertEquals(4, regularEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey());

            assertEquals(4, contractEmployeeVertexType.getInheritedProperties().size());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(true, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
            assertEquals("residence", contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
            assertEquals(3, contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

            assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("manager"));
            assertEquals("manager", contractEmployeeVertexType.getInheritedPropertyByName("manager").getName());
            assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("manager").getOriginalType());
            assertEquals(4, contractEmployeeVertexType.getInheritedPropertyByName("manager").getOrdinalPosition());
            assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("manager").isFromPrimaryKey());

            assertEquals(2, projectManagerVertexType.getInheritedProperties().size());

            assertNotNull(projectManagerVertexType.getInheritedPropertyByName("id"));
            assertEquals("id", projectManagerVertexType.getInheritedPropertyByName("id").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getInheritedPropertyByName("id").getOriginalType());
            assertEquals(1, projectManagerVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
            assertEquals(true, projectManagerVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

            assertNotNull(projectManagerVertexType.getInheritedPropertyByName("name"));
            assertEquals("name", projectManagerVertexType.getInheritedPropertyByName("name").getName());
            assertEquals("VARCHAR", projectManagerVertexType.getInheritedPropertyByName("name").getOriginalType());
            assertEquals(2, projectManagerVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
            assertEquals(false, projectManagerVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

            assertEquals(0, countryVertexType.getInheritedProperties().size());
            assertEquals(0, managerVertexType.getInheritedProperties().size());

            // edges check

            assertEquals(1, mapper.getRelationship2edgeType().size());

            assertEquals(1, mapper.getGraphModel().getEdgesType().size());
            assertEquals("HasManager", mapper.getGraphModel().getEdgesType().get(0).getName());

            assertEquals(1, employeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", employeeVertexType.getOutEdgesType().get(0).getName());

            assertEquals(1, regularEmployeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", regularEmployeeVertexType.getOutEdgesType().get(0).getName());

            assertEquals(1, contractEmployeeVertexType.getOutEdgesType().size());
            assertEquals("HasManager", contractEmployeeVertexType.getOutEdgesType().get(0).getName());

            /*
             * Rules check
             */

            // Classes Mapping

            assertEquals(6, mapper.getVertexType2EVClassMappers().size());
            assertEquals(6, mapper.getEntity2EVClassMappers().size());

            assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
            EVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
            assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
            assertEquals(employeeClassMapper.getEntity(), employeeEntity);
            assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

            assertEquals(4, employeeClassMapper.getAttribute2property().size());
            assertEquals(4, employeeClassMapper.getProperty2attribute().size());
            assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", employeeClassMapper.getAttribute2property().get("NAME"));
            assertEquals("residence", employeeClassMapper.getAttribute2property().get("RESIDENCE"));
            assertEquals("manager", employeeClassMapper.getAttribute2property().get("MANAGER"));
            assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", employeeClassMapper.getProperty2attribute().get("name"));
            assertEquals("RESIDENCE", employeeClassMapper.getProperty2attribute().get("residence"));
            assertEquals("MANAGER", employeeClassMapper.getProperty2attribute().get("manager"));

            assertEquals(1, mapper.getEVClassMappersByVertex(regularEmployeeVertexType).size());
            EVClassMapper regularEmployeeClassMapper = mapper.getEVClassMappersByVertex(regularEmployeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(regularEmployeeEntity).size());
            assertEquals(regularEmployeeClassMapper, mapper.getEVClassMappersByEntity(regularEmployeeEntity).get(0));
            assertEquals(regularEmployeeClassMapper.getEntity(), regularEmployeeEntity);
            assertEquals(regularEmployeeClassMapper.getVertexType(), regularEmployeeVertexType);

            assertEquals(2, regularEmployeeClassMapper.getAttribute2property().size());
            assertEquals(2, regularEmployeeClassMapper.getProperty2attribute().size());
            assertEquals("salary", regularEmployeeClassMapper.getAttribute2property().get("SALARY"));
            assertEquals("bonus", regularEmployeeClassMapper.getAttribute2property().get("BONUS"));
            assertEquals("SALARY", regularEmployeeClassMapper.getProperty2attribute().get("salary"));
            assertEquals("BONUS", regularEmployeeClassMapper.getProperty2attribute().get("bonus"));

            assertEquals(1, mapper.getEVClassMappersByVertex(contractEmployeeVertexType).size());
            EVClassMapper contractEmployeeClassMapper = mapper.getEVClassMappersByVertex(contractEmployeeVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(contractEmployeeEntity).size());
            assertEquals(contractEmployeeClassMapper, mapper.getEVClassMappersByEntity(contractEmployeeEntity).get(0));
            assertEquals(contractEmployeeClassMapper.getEntity(), contractEmployeeEntity);
            assertEquals(contractEmployeeClassMapper.getVertexType(), contractEmployeeVertexType);

            assertEquals(2, contractEmployeeClassMapper.getAttribute2property().size());
            assertEquals(2, contractEmployeeClassMapper.getProperty2attribute().size());
            assertEquals("payPerHour", contractEmployeeClassMapper.getAttribute2property().get("PAY_PER_HOUR"));
            assertEquals("contractDuration", contractEmployeeClassMapper.getAttribute2property().get("CONTRACT_DURATION"));
            assertEquals("PAY_PER_HOUR", contractEmployeeClassMapper.getProperty2attribute().get("payPerHour"));
            assertEquals("CONTRACT_DURATION", contractEmployeeClassMapper.getProperty2attribute().get("contractDuration"));

            assertEquals(1, mapper.getEVClassMappersByVertex(countryVertexType).size());
            EVClassMapper countryClassMapper = mapper.getEVClassMappersByVertex(countryVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(countryEntity).size());
            assertEquals(countryClassMapper, mapper.getEVClassMappersByEntity(countryEntity).get(0));
            assertEquals(countryClassMapper.getEntity(), countryEntity);
            assertEquals(countryClassMapper.getVertexType(), countryVertexType);

            assertEquals(3, countryClassMapper.getAttribute2property().size());
            assertEquals(3, countryClassMapper.getProperty2attribute().size());
            assertEquals("id", countryClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", countryClassMapper.getAttribute2property().get("NAME"));
            assertEquals("continent", countryClassMapper.getAttribute2property().get("CONTINENT"));
            assertEquals("ID", countryClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", countryClassMapper.getProperty2attribute().get("name"));
            assertEquals("CONTINENT", countryClassMapper.getProperty2attribute().get("continent"));

            assertEquals(1, mapper.getEVClassMappersByVertex(managerVertexType).size());
            EVClassMapper managerClassMapper = mapper.getEVClassMappersByVertex(managerVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(managerEntity).size());
            assertEquals(managerClassMapper, mapper.getEVClassMappersByEntity(managerEntity).get(0));
            assertEquals(managerClassMapper.getEntity(), managerEntity);
            assertEquals(managerClassMapper.getVertexType(), managerVertexType);

            assertEquals(2, managerClassMapper.getAttribute2property().size());
            assertEquals(2, managerClassMapper.getProperty2attribute().size());
            assertEquals("id", managerClassMapper.getAttribute2property().get("ID"));
            assertEquals("name", managerClassMapper.getAttribute2property().get("NAME"));
            assertEquals("ID", managerClassMapper.getProperty2attribute().get("id"));
            assertEquals("NAME", managerClassMapper.getProperty2attribute().get("name"));

            assertEquals(1, mapper.getEVClassMappersByVertex(projectManagerVertexType).size());
            EVClassMapper projectManagerClassMapper = mapper.getEVClassMappersByVertex(projectManagerVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(projectManagerEntity).size());
            assertEquals(projectManagerClassMapper, mapper.getEVClassMappersByEntity(projectManagerEntity).get(0));
            assertEquals(projectManagerClassMapper.getEntity(), projectManagerEntity);
            assertEquals(projectManagerClassMapper.getVertexType(), projectManagerVertexType);

            assertEquals(1, projectManagerClassMapper.getAttribute2property().size());
            assertEquals(1, projectManagerClassMapper.getProperty2attribute().size());
            assertEquals("project", projectManagerClassMapper.getAttribute2property().get("PROJECT"));
            assertEquals("PROJECT", projectManagerClassMapper.getProperty2attribute().get("project"));

            // Relationships-Edges Mapping

            Iterator<CanonicalRelationship> itRelationships = employeeEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship hasManagerRelationship = itRelationships.next();
            assertFalse(itRelationships.hasNext());

            EdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

            assertEquals(1, mapper.getRelationship2edgeType().size());
            assertEquals(hasManagerEdgeType, mapper.getRelationship2edgeType().get(hasManagerRelationship));

            assertEquals(1, mapper.getEdgeType2relationships().size());
            assertEquals(1, mapper.getEdgeType2relationships().get(hasManagerEdgeType).size());
            assertTrue(mapper.getEdgeType2relationships().get(hasManagerEdgeType).contains(hasManagerRelationship));

            // JoinVertexes-AggregatorEdges Mapping

            assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {

                // Dropping Source DB Schema and OrientGraph
                String dbDropping = "drop schema public cascade";
                st.execute(dbDropping);
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }
    }

}
