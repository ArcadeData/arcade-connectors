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

package com.arcadeanalytics.provider.rdbms.jointableaggregation;

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
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.classmapper.EVClassMapper;
import com.arcadeanalytics.provider.rdbms.model.dbschema.CanonicalRelationship;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.EdgeType;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.VertexType;
import com.arcadeanalytics.provider.rdbms.nameresolver.JavaConventionNameResolver;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
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

class AggregationStrategyTest {

    private DBQueryEngine dbQueryEngine;
    private String driver = "org.hsqldb.jdbc.JDBCDriver";
    private String jurl = "jdbc:hsqldb:mem:mydb";
    private String username = "SA";
    private String password = "";
    private DataSourceInfo dataSource;

    private NameResolver nameResolver;
    private DBMSDataTypeHandler dataTypeHandler;
    private String executionStrategy;
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
                false
            );
        dbQueryEngine = new DBQueryEngine(dataSource, 300);
        executionStrategy = "not_specified";
        nameResolver = new JavaConventionNameResolver();
        dataTypeHandler = new HSQLDBDataTypeHandler();
        statistics = new Statistics();
    }

    /*
     * Aggregation Strategy Test: executing mapping
     */@Test
    void performMappingWithAggregationStrategy() {
        Connection connection = null;
        Statement st = null;

        try {
            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            // Tables Building

            String employeeTableBuilding =
                "create memory table EMPLOYEE (ID varchar(256) not null," +
                " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
            st = connection.createStatement();
            st.execute(employeeTableBuilding);

            String departmentTableBuilding = "create memory table DEPARTMENT (ID varchar(256) not null, NAME  varchar(256)," + " primary key (ID))";
            st.execute(departmentTableBuilding);

            String dept2empTableBuilding =
                "create memory table DEPT_EMP (DEPT_ID varchar(256) not null, EMP_ID  varchar(256), HIRING_YEAR varchar(256)," +
                " primary key (DEPT_ID,EMP_ID), foreign key (EMP_ID) references EMPLOYEE(ID), foreign key (DEPT_ID) references DEPARTMENT(ID))";
            st.execute(dept2empTableBuilding);

            String dept2managerTableBuilding =
                "create memory table DEPT_MANAGER (DEPT_ID varchar(256) not null, EMP_ID  varchar(256)," +
                " primary key (DEPT_ID,EMP_ID), foreign key (EMP_ID) references EMPLOYEE(ID), foreign key (DEPT_ID) references DEPARTMENT(ID))";
            st.execute(dept2managerTableBuilding);

            String branchTableBuilding =
                "create memory table BRANCH(BRANCH_ID varchar(256) not null, LOCATION  varchar(256)," +
                "DEPT varchar(256) not null, primary key (BRANCH_ID), foreign key (DEPT) references DEPARTMENT(ID))";
            st.execute(branchTableBuilding);

            ER2GraphMapper mapper = new ER2GraphMapper(dataSource, null, null, dbQueryEngine, dataTypeHandler, executionStrategy, nameResolver, statistics);
            mapper.buildSourceDatabaseSchema();
            mapper.buildGraphModel(new JavaConventionNameResolver());

            /*
             *  Testing context information
             */

            assertThat(statistics.totalNumberOfModelVertices).isEqualTo(5);
            assertThat(statistics.builtModelVertexTypes).isEqualTo(5);
            assertThat(statistics.totalNumberOfModelEdges).isEqualTo(2);
            assertThat(statistics.builtModelEdgeTypes).isEqualTo(2);

            /*
             *  Testing built graph model
             */
            VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
            VertexType departmentVertexType = mapper.getGraphModel().getVertexTypeByName("Department");
            VertexType deptEmpVertexType = mapper.getGraphModel().getVertexTypeByName("DeptEmp");
            VertexType deptManagerVertexType = mapper.getGraphModel().getVertexTypeByName("DeptManager");
            VertexType branchVertexType = mapper.getGraphModel().getVertexTypeByName("Branch");
            EdgeType deptEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasDept");
            EdgeType empEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasEmp");

            // vertices check
            assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(5);
            assertThat(employeeVertexType).isNotNull();
            assertThat(departmentVertexType).isNotNull();
            assertThat(deptEmpVertexType).isNotNull();
            assertThat(deptManagerVertexType).isNotNull();
            assertThat(branchVertexType).isNotNull();

            // edges check
            assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(2);
            assertThat(deptEdgeType).isNotNull();
            assertThat(empEdgeType).isNotNull();
            assertThat(deptEdgeType.getNumberRelationshipsRepresented()).isEqualTo(3);
            assertThat(empEdgeType.getNumberRelationshipsRepresented()).isEqualTo(2);

            /*
             * Rules check
             */

            // Classes Mapping

            assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(5);
            assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(5);

            Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
            assertThat(mapper.getEVClassMappersByVertex(employeeVertexType).size()).isEqualTo(1);
            EVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(employeeEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(employeeEntity).get(0)).isEqualTo(employeeClassMapper);
            assertThat(employeeEntity).isEqualTo(employeeClassMapper.getEntity());
            assertThat(employeeVertexType).isEqualTo(employeeClassMapper.getVertexType());

            assertThat(employeeClassMapper.getAttribute2property().size()).isEqualTo(3);
            assertThat(employeeClassMapper.getProperty2attribute().size()).isEqualTo(3);
            assertThat(employeeClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
            assertThat(employeeClassMapper.getAttribute2property().get("FIRST_NAME")).isEqualTo("firstName");
            assertThat(employeeClassMapper.getAttribute2property().get("LAST_NAME")).isEqualTo("lastName");
            assertThat(employeeClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
            assertThat(employeeClassMapper.getProperty2attribute().get("firstName")).isEqualTo("FIRST_NAME");
            assertThat(employeeClassMapper.getProperty2attribute().get("lastName")).isEqualTo("LAST_NAME");

            Entity departmentEntity = mapper.getDataBaseSchema().getEntityByName("DEPARTMENT");
            assertThat(mapper.getEVClassMappersByVertex(departmentVertexType).size()).isEqualTo(1);
            EVClassMapper departmentClassMapper = mapper.getEVClassMappersByVertex(departmentVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(departmentEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(departmentEntity).get(0)).isEqualTo(departmentClassMapper);
            assertThat(departmentEntity).isEqualTo(departmentClassMapper.getEntity());
            assertThat(departmentVertexType).isEqualTo(departmentClassMapper.getVertexType());

            assertThat(departmentClassMapper.getAttribute2property().size()).isEqualTo(2);
            assertThat(departmentClassMapper.getProperty2attribute().size()).isEqualTo(2);
            assertThat(departmentClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
            assertThat(departmentClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
            assertThat(departmentClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
            assertThat(departmentClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");

            Entity branchEntity = mapper.getDataBaseSchema().getEntityByName("BRANCH");
            assertThat(mapper.getEVClassMappersByVertex(branchVertexType).size()).isEqualTo(1);
            EVClassMapper branchClassMapper = mapper.getEVClassMappersByVertex(branchVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(branchEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(branchEntity).get(0)).isEqualTo(branchClassMapper);
            assertThat(branchEntity).isEqualTo(branchClassMapper.getEntity());
            assertThat(branchVertexType).isEqualTo(branchClassMapper.getVertexType());

            assertThat(branchClassMapper.getAttribute2property().size()).isEqualTo(3);
            assertThat(branchClassMapper.getProperty2attribute().size()).isEqualTo(3);
            assertThat(branchClassMapper.getAttribute2property().get("BRANCH_ID")).isEqualTo("branchId");
            assertThat(branchClassMapper.getAttribute2property().get("LOCATION")).isEqualTo("location");
            assertThat(branchClassMapper.getAttribute2property().get("DEPT")).isEqualTo("dept");
            assertThat(branchClassMapper.getProperty2attribute().get("branchId")).isEqualTo("BRANCH_ID");
            assertThat(branchClassMapper.getProperty2attribute().get("location")).isEqualTo("LOCATION");
            assertThat(branchClassMapper.getProperty2attribute().get("dept")).isEqualTo("DEPT");

            Entity deptEmpEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_EMP");
            assertThat(mapper.getEVClassMappersByVertex(deptEmpVertexType).size()).isEqualTo(1);
            EVClassMapper deptEmpClassMapper = mapper.getEVClassMappersByVertex(deptEmpVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(deptEmpEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(deptEmpEntity).get(0)).isEqualTo(deptEmpClassMapper);
            assertThat(deptEmpEntity).isEqualTo(deptEmpClassMapper.getEntity());
            assertThat(deptEmpVertexType).isEqualTo(deptEmpClassMapper.getVertexType());

            assertThat(deptEmpClassMapper.getAttribute2property().size()).isEqualTo(3);
            assertThat(deptEmpClassMapper.getProperty2attribute().size()).isEqualTo(3);
            assertThat(deptEmpClassMapper.getAttribute2property().get("DEPT_ID")).isEqualTo("deptId");
            assertThat(deptEmpClassMapper.getAttribute2property().get("EMP_ID")).isEqualTo("empId");
            assertThat(deptEmpClassMapper.getAttribute2property().get("HIRING_YEAR")).isEqualTo("hiringYear");
            assertThat(deptEmpClassMapper.getProperty2attribute().get("deptId")).isEqualTo("DEPT_ID");
            assertThat(deptEmpClassMapper.getProperty2attribute().get("empId")).isEqualTo("EMP_ID");
            assertThat(deptEmpClassMapper.getProperty2attribute().get("hiringYear")).isEqualTo("HIRING_YEAR");

            Entity deptMgrEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_MANAGER");
            assertThat(mapper.getEVClassMappersByVertex(deptManagerVertexType).size()).isEqualTo(1);
            EVClassMapper deptManagerClassMapper = mapper.getEVClassMappersByVertex(deptManagerVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(deptMgrEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(deptMgrEntity).get(0)).isEqualTo(deptManagerClassMapper);
            assertThat(deptMgrEntity).isEqualTo(deptManagerClassMapper.getEntity());
            assertThat(deptManagerVertexType).isEqualTo(deptManagerClassMapper.getVertexType());

            assertThat(deptManagerClassMapper.getAttribute2property().size()).isEqualTo(2);
            assertThat(deptManagerClassMapper.getProperty2attribute().size()).isEqualTo(2);
            assertThat(deptManagerClassMapper.getAttribute2property().get("DEPT_ID")).isEqualTo("deptId");
            assertThat(deptManagerClassMapper.getAttribute2property().get("EMP_ID")).isEqualTo("empId");
            assertThat(deptManagerClassMapper.getProperty2attribute().get("deptId")).isEqualTo("DEPT_ID");
            assertThat(deptManagerClassMapper.getProperty2attribute().get("empId")).isEqualTo("EMP_ID");

            // Relationships-Edges Mapping

            Iterator<CanonicalRelationship> it = deptEmpEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship hasDepartmentRelationship1 = it.next();
            CanonicalRelationship hasEmployeeRelationship1 = it.next();
            assertThat(it.hasNext()).isFalse();

            it = deptMgrEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship hasDepartmentRelationship2 = it.next();
            CanonicalRelationship hasEmployeeRelationship2 = it.next();
            assertThat(it.hasNext()).isFalse();

            it = branchEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship hasDepartmentRelationship3 = it.next();
            assertThat(it.hasNext()).isFalse();

            assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(5);
            assertThat(mapper.getRelationship2edgeType().get(hasDepartmentRelationship1)).isEqualTo(deptEdgeType);
            assertThat(mapper.getRelationship2edgeType().get(hasDepartmentRelationship2)).isEqualTo(deptEdgeType);
            assertThat(mapper.getRelationship2edgeType().get(hasDepartmentRelationship3)).isEqualTo(deptEdgeType);
            assertThat(mapper.getRelationship2edgeType().get(hasEmployeeRelationship1)).isEqualTo(empEdgeType);
            assertThat(mapper.getRelationship2edgeType().get(hasEmployeeRelationship2)).isEqualTo(empEdgeType);

            assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(2);
            assertThat(mapper.getEdgeType2relationships().get(deptEdgeType).size()).isEqualTo(3);
            assertThat(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship1)).isTrue();
            assertThat(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship2)).isTrue();
            assertThat(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship3)).isTrue();
            assertThat(mapper.getEdgeType2relationships().get(empEdgeType).size()).isEqualTo(2);
            assertThat(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship1)).isTrue();
            assertThat(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship2)).isTrue();

            assertThat(mapper.getJoinVertex2aggregatorEdges().size()).isEqualTo(0);

            /*
             * Aggregation of join tables
             */
            mapper.performMany2ManyAggregation();

            /*
             *  Testing context information
             */

            assertThat(statistics.totalNumberOfModelVertices).isEqualTo(3);
            assertThat(statistics.builtModelVertexTypes).isEqualTo(3);
            assertThat(statistics.totalNumberOfModelEdges).isEqualTo(3);
            assertThat(statistics.builtModelEdgeTypes).isEqualTo(3);

            /*
             *  Testing built graph model
             */

            employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
            departmentVertexType = mapper.getGraphModel().getVertexTypeByName("Department");
            branchVertexType = mapper.getGraphModel().getVertexTypeByName("Branch");
            deptEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasDept");
            EdgeType deptEmpEdgeType = mapper.getGraphModel().getEdgeTypeByName("DeptEmp");
            EdgeType deptManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("DeptManager");

            // vertices check
            assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(3);
            assertThat(employeeVertexType).isNotNull();
            assertThat(departmentVertexType).isNotNull();
            assertThat(mapper.getGraphModel().getVertexTypeByName("DeptEmp")).isNull();
            assertThat(mapper.getGraphModel().getVertexTypeByName("DeptManager")).isNull();
            assertThat(branchVertexType).isNotNull();

            // edges check
            assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(3);
            assertThat(deptEdgeType).isNotNull();
            assertThat(deptEmpEdgeType).isNotNull();
            assertThat(deptManagerEdgeType).isNotNull();
            assertThat(mapper.getGraphModel().getEdgeTypeByName("HasEmp")).isNull();
            assertThat(deptEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);
            assertThat(deptEmpEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);
            assertThat(deptManagerEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);

            assertThat(deptEmpEdgeType.getPropertyByName("hiringYear")).isNotNull();
            assertThat(deptEmpEdgeType.getPropertyByName("hiringYear").getOriginalType()).isEqualTo("VARCHAR");

            /*
             * Rules check
             */

            // Classes Mapping

            assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(5);
            assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(5);

            employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
            assertThat(mapper.getEVClassMappersByVertex(employeeVertexType).size()).isEqualTo(1);
            employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(employeeEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(employeeEntity).get(0)).isEqualTo(employeeClassMapper);
            assertThat(employeeEntity).isEqualTo(employeeClassMapper.getEntity());
            assertThat(employeeVertexType).isEqualTo(employeeClassMapper.getVertexType());

            assertThat(employeeClassMapper.getAttribute2property().size()).isEqualTo(3);
            assertThat(employeeClassMapper.getProperty2attribute().size()).isEqualTo(3);
            assertThat(employeeClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
            assertThat(employeeClassMapper.getAttribute2property().get("FIRST_NAME")).isEqualTo("firstName");
            assertThat(employeeClassMapper.getAttribute2property().get("LAST_NAME")).isEqualTo("lastName");
            assertThat(employeeClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
            assertThat(employeeClassMapper.getProperty2attribute().get("firstName")).isEqualTo("FIRST_NAME");
            assertThat(employeeClassMapper.getProperty2attribute().get("lastName")).isEqualTo("LAST_NAME");

            departmentEntity = mapper.getDataBaseSchema().getEntityByName("DEPARTMENT");
            assertThat(mapper.getEVClassMappersByVertex(departmentVertexType).size()).isEqualTo(1);
            departmentClassMapper = mapper.getEVClassMappersByVertex(departmentVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(departmentEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(departmentEntity).get(0)).isEqualTo(departmentClassMapper);
            assertThat(departmentEntity).isEqualTo(departmentClassMapper.getEntity());
            assertThat(departmentVertexType).isEqualTo(departmentClassMapper.getVertexType());

            assertThat(departmentClassMapper.getAttribute2property().size()).isEqualTo(2);
            assertThat(departmentClassMapper.getProperty2attribute().size()).isEqualTo(2);
            assertThat(departmentClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
            assertThat(departmentClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
            assertThat(departmentClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
            assertThat(departmentClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");

            branchEntity = mapper.getDataBaseSchema().getEntityByName("BRANCH");
            assertThat(mapper.getEVClassMappersByVertex(branchVertexType).size()).isEqualTo(1);
            branchClassMapper = mapper.getEVClassMappersByVertex(branchVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(branchEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(branchEntity).get(0)).isEqualTo(branchClassMapper);
            assertThat(branchEntity).isEqualTo(branchClassMapper.getEntity());
            assertThat(branchVertexType).isEqualTo(branchClassMapper.getVertexType());

            assertThat(branchClassMapper.getAttribute2property().size()).isEqualTo(3);
            assertThat(branchClassMapper.getProperty2attribute().size()).isEqualTo(3);
            assertThat(branchClassMapper.getAttribute2property().get("BRANCH_ID")).isEqualTo("branchId");
            assertThat(branchClassMapper.getAttribute2property().get("LOCATION")).isEqualTo("location");
            assertThat(branchClassMapper.getAttribute2property().get("DEPT")).isEqualTo("dept");
            assertThat(branchClassMapper.getProperty2attribute().get("branchId")).isEqualTo("BRANCH_ID");
            assertThat(branchClassMapper.getProperty2attribute().get("location")).isEqualTo("LOCATION");
            assertThat(branchClassMapper.getProperty2attribute().get("dept")).isEqualTo("DEPT");

            deptEmpEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_EMP");
            assertThat(mapper.getEVClassMappersByVertex(deptEmpVertexType).size()).isEqualTo(1);
            deptEmpClassMapper = mapper.getEVClassMappersByVertex(deptEmpVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(deptEmpEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(deptEmpEntity).get(0)).isEqualTo(deptEmpClassMapper);
            assertThat(deptEmpEntity).isEqualTo(deptEmpClassMapper.getEntity());
            assertThat(deptEmpVertexType).isEqualTo(deptEmpClassMapper.getVertexType());

            assertThat(deptEmpClassMapper.getAttribute2property().size()).isEqualTo(3);
            assertThat(deptEmpClassMapper.getProperty2attribute().size()).isEqualTo(3);
            assertThat(deptEmpClassMapper.getAttribute2property().get("DEPT_ID")).isEqualTo("deptId");
            assertThat(deptEmpClassMapper.getAttribute2property().get("EMP_ID")).isEqualTo("empId");
            assertThat(deptEmpClassMapper.getAttribute2property().get("HIRING_YEAR")).isEqualTo("hiringYear");
            assertThat(deptEmpClassMapper.getProperty2attribute().get("deptId")).isEqualTo("DEPT_ID");
            assertThat(deptEmpClassMapper.getProperty2attribute().get("empId")).isEqualTo("EMP_ID");
            assertThat(deptEmpClassMapper.getProperty2attribute().get("hiringYear")).isEqualTo("HIRING_YEAR");

            deptMgrEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_MANAGER");
            assertThat(mapper.getEVClassMappersByVertex(deptManagerVertexType).size()).isEqualTo(1);
            deptManagerClassMapper = mapper.getEVClassMappersByVertex(deptManagerVertexType).get(0);
            assertThat(mapper.getEVClassMappersByEntity(deptMgrEntity).size()).isEqualTo(1);
            assertThat(mapper.getEVClassMappersByEntity(deptMgrEntity).get(0)).isEqualTo(deptManagerClassMapper);
            assertThat(deptMgrEntity).isEqualTo(deptManagerClassMapper.getEntity());
            assertThat(deptManagerVertexType).isEqualTo(deptManagerClassMapper.getVertexType());

            assertThat(deptManagerClassMapper.getAttribute2property().size()).isEqualTo(2);
            assertThat(deptManagerClassMapper.getProperty2attribute().size()).isEqualTo(2);
            assertThat(deptManagerClassMapper.getAttribute2property().get("DEPT_ID")).isEqualTo("deptId");
            assertThat(deptManagerClassMapper.getAttribute2property().get("EMP_ID")).isEqualTo("empId");
            assertThat(deptManagerClassMapper.getProperty2attribute().get("deptId")).isEqualTo("DEPT_ID");
            assertThat(deptManagerClassMapper.getProperty2attribute().get("empId")).isEqualTo("EMP_ID");

            // Relationships-Edges Mapping

            it = deptEmpEntity.getOutCanonicalRelationships().iterator();
            hasDepartmentRelationship1 = it.next();
            hasEmployeeRelationship1 = it.next();
            assertThat(it.hasNext()).isFalse();

            it = deptMgrEntity.getOutCanonicalRelationships().iterator();
            hasDepartmentRelationship2 = it.next();
            hasEmployeeRelationship2 = it.next();
            assertThat(it.hasNext()).isFalse();

            it = branchEntity.getOutCanonicalRelationships().iterator();
            hasDepartmentRelationship3 = it.next();
            assertThat(it.hasNext()).isFalse();

            // fetching empEdgeType from the rules as was deleted from the graph model during the aggregation
            assertThat(empEdgeType.getName()).isEqualTo("HasEmp");
            assertThat(empEdgeType.getInVertexType()).isEqualTo(employeeVertexType);
            assertThat(empEdgeType.getAllProperties().size()).isEqualTo(0);

            assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(5);
            assertThat(mapper.getRelationship2edgeType().get(hasDepartmentRelationship1)).isEqualTo(deptEdgeType);
            assertThat(mapper.getRelationship2edgeType().get(hasDepartmentRelationship2)).isEqualTo(deptEdgeType);
            assertThat(mapper.getRelationship2edgeType().get(hasDepartmentRelationship3)).isEqualTo(deptEdgeType);
            assertThat(mapper.getRelationship2edgeType().get(hasEmployeeRelationship1)).isEqualTo(empEdgeType);
            assertThat(mapper.getRelationship2edgeType().get(hasEmployeeRelationship2)).isEqualTo(empEdgeType);

            assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(2);
            assertThat(mapper.getEdgeType2relationships().get(deptEdgeType).size()).isEqualTo(3);
            assertThat(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship1)).isTrue();
            assertThat(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship2)).isTrue();
            assertThat(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship3)).isTrue();
            assertThat(mapper.getEdgeType2relationships().get(empEdgeType).size()).isEqualTo(2);
            assertThat(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship1)).isTrue();
            assertThat(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship2)).isTrue();

            // JoinVertexes-AggregatorEdges Mapping

            assertThat(mapper.getJoinVertex2aggregatorEdges().size()).isEqualTo(2);
            assertThat(mapper.getJoinVertex2aggregatorEdges().containsKey(deptManagerVertexType)).isTrue();
            assertThat(mapper.getJoinVertex2aggregatorEdges().containsKey(deptEmpVertexType)).isTrue();
            assertThat(mapper.getJoinVertex2aggregatorEdges().get(deptManagerVertexType).getEdgeType()).isEqualTo(deptManagerEdgeType);
            assertThat(mapper.getJoinVertex2aggregatorEdges().get(deptManagerVertexType).getOutVertexClassName()).isEqualTo("Department");
            assertThat(mapper.getJoinVertex2aggregatorEdges().get(deptManagerVertexType).getInVertexClassName()).isEqualTo("Employee");
            assertThat(mapper.getJoinVertex2aggregatorEdges().get(deptEmpVertexType).getEdgeType()).isEqualTo(deptEmpEdgeType);
            assertThat(mapper.getJoinVertex2aggregatorEdges().get(deptEmpVertexType).getOutVertexClassName()).isEqualTo("Department");
            assertThat(mapper.getJoinVertex2aggregatorEdges().get(deptEmpVertexType).getInVertexClassName()).isEqualTo("Employee");
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
