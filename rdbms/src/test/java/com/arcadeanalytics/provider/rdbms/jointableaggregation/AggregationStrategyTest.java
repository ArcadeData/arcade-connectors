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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

public class AggregationStrategyTest {

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
  public void init() {
    this.dataSource =
      new DataSourceInfo(1L, "RDBMS_HSQL", "testDataSource", "desc", "mem", 1234, "mydb", username, password, false, "{}", false, false, "", 22, "", false);
    dbQueryEngine = new DBQueryEngine(dataSource, 300);
    executionStrategy = "not_specified";
    nameResolver = new JavaConventionNameResolver();
    dataTypeHandler = new HSQLDBDataTypeHandler();
    statistics = new Statistics();
  }

  @Test
  /*
   * Aggregation Strategy Test: executing mapping
   */public void performMappingWithAggregationStrategy() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      // Tables Building

      String employeeTableBuilding =
        "create memory table EMPLOYEE (ID varchar(256) not null," + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
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

      assertEquals(5, statistics.totalNumberOfModelVertices);
      assertEquals(5, statistics.builtModelVertexTypes);
      assertEquals(2, statistics.totalNumberOfModelEdges);
      assertEquals(2, statistics.builtModelEdgeTypes);

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
      assertEquals(5, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(departmentVertexType);
      assertNotNull(deptEmpVertexType);
      assertNotNull(deptManagerVertexType);
      assertNotNull(branchVertexType);

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(deptEdgeType);
      assertNotNull(empEdgeType);
      assertEquals(3, deptEdgeType.getNumberRelationshipsRepresented());
      assertEquals(2, empEdgeType.getNumberRelationshipsRepresented());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(5, mapper.getVertexType2EVClassMappers().size());
      assertEquals(5, mapper.getEntity2EVClassMappers().size());

      Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
      EVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
      assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
      assertEquals(employeeClassMapper.getEntity(), employeeEntity);
      assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

      assertEquals(3, employeeClassMapper.getAttribute2property().size());
      assertEquals(3, employeeClassMapper.getProperty2attribute().size());
      assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", employeeClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", employeeClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", employeeClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", employeeClassMapper.getProperty2attribute().get("lastName"));

      Entity departmentEntity = mapper.getDataBaseSchema().getEntityByName("DEPARTMENT");
      assertEquals(1, mapper.getEVClassMappersByVertex(departmentVertexType).size());
      EVClassMapper departmentClassMapper = mapper.getEVClassMappersByVertex(departmentVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(departmentEntity).size());
      assertEquals(departmentClassMapper, mapper.getEVClassMappersByEntity(departmentEntity).get(0));
      assertEquals(departmentClassMapper.getEntity(), departmentEntity);
      assertEquals(departmentClassMapper.getVertexType(), departmentVertexType);

      assertEquals(2, departmentClassMapper.getAttribute2property().size());
      assertEquals(2, departmentClassMapper.getProperty2attribute().size());
      assertEquals("id", departmentClassMapper.getAttribute2property().get("ID"));
      assertEquals("name", departmentClassMapper.getAttribute2property().get("NAME"));
      assertEquals("ID", departmentClassMapper.getProperty2attribute().get("id"));
      assertEquals("NAME", departmentClassMapper.getProperty2attribute().get("name"));

      Entity branchEntity = mapper.getDataBaseSchema().getEntityByName("BRANCH");
      assertEquals(1, mapper.getEVClassMappersByVertex(branchVertexType).size());
      EVClassMapper branchClassMapper = mapper.getEVClassMappersByVertex(branchVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(branchEntity).size());
      assertEquals(branchClassMapper, mapper.getEVClassMappersByEntity(branchEntity).get(0));
      assertEquals(branchClassMapper.getEntity(), branchEntity);
      assertEquals(branchClassMapper.getVertexType(), branchVertexType);

      assertEquals(3, branchClassMapper.getAttribute2property().size());
      assertEquals(3, branchClassMapper.getProperty2attribute().size());
      assertEquals("branchId", branchClassMapper.getAttribute2property().get("BRANCH_ID"));
      assertEquals("location", branchClassMapper.getAttribute2property().get("LOCATION"));
      assertEquals("dept", branchClassMapper.getAttribute2property().get("DEPT"));
      assertEquals("BRANCH_ID", branchClassMapper.getProperty2attribute().get("branchId"));
      assertEquals("LOCATION", branchClassMapper.getProperty2attribute().get("location"));
      assertEquals("DEPT", branchClassMapper.getProperty2attribute().get("dept"));

      Entity deptEmpEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_EMP");
      assertEquals(1, mapper.getEVClassMappersByVertex(deptEmpVertexType).size());
      EVClassMapper deptEmpClassMapper = mapper.getEVClassMappersByVertex(deptEmpVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(deptEmpEntity).size());
      assertEquals(deptEmpClassMapper, mapper.getEVClassMappersByEntity(deptEmpEntity).get(0));
      assertEquals(deptEmpClassMapper.getEntity(), deptEmpEntity);
      assertEquals(deptEmpClassMapper.getVertexType(), deptEmpVertexType);

      assertEquals(3, deptEmpClassMapper.getAttribute2property().size());
      assertEquals(3, deptEmpClassMapper.getProperty2attribute().size());
      assertEquals("deptId", deptEmpClassMapper.getAttribute2property().get("DEPT_ID"));
      assertEquals("empId", deptEmpClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("hiringYear", deptEmpClassMapper.getAttribute2property().get("HIRING_YEAR"));
      assertEquals("DEPT_ID", deptEmpClassMapper.getProperty2attribute().get("deptId"));
      assertEquals("EMP_ID", deptEmpClassMapper.getProperty2attribute().get("empId"));
      assertEquals("HIRING_YEAR", deptEmpClassMapper.getProperty2attribute().get("hiringYear"));

      Entity deptMgrEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_MANAGER");
      assertEquals(1, mapper.getEVClassMappersByVertex(deptManagerVertexType).size());
      EVClassMapper deptManagerClassMapper = mapper.getEVClassMappersByVertex(deptManagerVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(deptMgrEntity).size());
      assertEquals(deptManagerClassMapper, mapper.getEVClassMappersByEntity(deptMgrEntity).get(0));
      assertEquals(deptManagerClassMapper.getEntity(), deptMgrEntity);
      assertEquals(deptManagerClassMapper.getVertexType(), deptManagerVertexType);

      assertEquals(2, deptManagerClassMapper.getAttribute2property().size());
      assertEquals(2, deptManagerClassMapper.getProperty2attribute().size());
      assertEquals("deptId", deptManagerClassMapper.getAttribute2property().get("DEPT_ID"));
      assertEquals("empId", deptManagerClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("DEPT_ID", deptManagerClassMapper.getProperty2attribute().get("deptId"));
      assertEquals("EMP_ID", deptManagerClassMapper.getProperty2attribute().get("empId"));

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> it = deptEmpEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasDepartmentRelationship1 = it.next();
      CanonicalRelationship hasEmployeeRelationship1 = it.next();
      assertFalse(it.hasNext());

      it = deptMgrEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasDepartmentRelationship2 = it.next();
      CanonicalRelationship hasEmployeeRelationship2 = it.next();
      assertFalse(it.hasNext());

      it = branchEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasDepartmentRelationship3 = it.next();
      assertFalse(it.hasNext());

      assertEquals(5, mapper.getRelationship2edgeType().size());
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship1));
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship2));
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship3));
      assertEquals(empEdgeType, mapper.getRelationship2edgeType().get(hasEmployeeRelationship1));
      assertEquals(empEdgeType, mapper.getRelationship2edgeType().get(hasEmployeeRelationship2));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(3, mapper.getEdgeType2relationships().get(deptEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship1));
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship2));
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship3));
      assertEquals(2, mapper.getEdgeType2relationships().get(empEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship1));
      assertTrue(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship2));

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

      /*
       * Aggregation of join tables
       */
      mapper.performMany2ManyAggregation();

      /*
       *  Testing context information
       */

      assertEquals(3, statistics.totalNumberOfModelVertices);
      assertEquals(3, statistics.builtModelVertexTypes);
      assertEquals(3, statistics.totalNumberOfModelEdges);
      assertEquals(3, statistics.builtModelEdgeTypes);

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
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(departmentVertexType);
      assertNull(mapper.getGraphModel().getVertexTypeByName("DeptEmp"));
      assertNull(mapper.getGraphModel().getVertexTypeByName("DeptManager"));
      assertNotNull(branchVertexType);

      // edges check
      assertEquals(3, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(deptEdgeType);
      assertNotNull(deptEmpEdgeType);
      assertNotNull(deptManagerEdgeType);
      assertNull(mapper.getGraphModel().getEdgeTypeByName("HasEmp"));
      assertEquals(1, deptEdgeType.getNumberRelationshipsRepresented());
      assertEquals(1, deptEmpEdgeType.getNumberRelationshipsRepresented());
      assertEquals(1, deptManagerEdgeType.getNumberRelationshipsRepresented());

      assertNotNull(deptEmpEdgeType.getPropertyByName("hiringYear"));
      assertTrue(deptEmpEdgeType.getPropertyByName("hiringYear").getOriginalType().equals("VARCHAR"));

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(5, mapper.getVertexType2EVClassMappers().size());
      assertEquals(5, mapper.getEntity2EVClassMappers().size());

      employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
      employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
      assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
      assertEquals(employeeClassMapper.getEntity(), employeeEntity);
      assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

      assertEquals(3, employeeClassMapper.getAttribute2property().size());
      assertEquals(3, employeeClassMapper.getProperty2attribute().size());
      assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", employeeClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", employeeClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", employeeClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", employeeClassMapper.getProperty2attribute().get("lastName"));

      departmentEntity = mapper.getDataBaseSchema().getEntityByName("DEPARTMENT");
      assertEquals(1, mapper.getEVClassMappersByVertex(departmentVertexType).size());
      departmentClassMapper = mapper.getEVClassMappersByVertex(departmentVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(departmentEntity).size());
      assertEquals(departmentClassMapper, mapper.getEVClassMappersByEntity(departmentEntity).get(0));
      assertEquals(departmentClassMapper.getEntity(), departmentEntity);
      assertEquals(departmentClassMapper.getVertexType(), departmentVertexType);

      assertEquals(2, departmentClassMapper.getAttribute2property().size());
      assertEquals(2, departmentClassMapper.getProperty2attribute().size());
      assertEquals("id", departmentClassMapper.getAttribute2property().get("ID"));
      assertEquals("name", departmentClassMapper.getAttribute2property().get("NAME"));
      assertEquals("ID", departmentClassMapper.getProperty2attribute().get("id"));
      assertEquals("NAME", departmentClassMapper.getProperty2attribute().get("name"));

      branchEntity = mapper.getDataBaseSchema().getEntityByName("BRANCH");
      assertEquals(1, mapper.getEVClassMappersByVertex(branchVertexType).size());
      branchClassMapper = mapper.getEVClassMappersByVertex(branchVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(branchEntity).size());
      assertEquals(branchClassMapper, mapper.getEVClassMappersByEntity(branchEntity).get(0));
      assertEquals(branchClassMapper.getEntity(), branchEntity);
      assertEquals(branchClassMapper.getVertexType(), branchVertexType);

      assertEquals(3, branchClassMapper.getAttribute2property().size());
      assertEquals(3, branchClassMapper.getProperty2attribute().size());
      assertEquals("branchId", branchClassMapper.getAttribute2property().get("BRANCH_ID"));
      assertEquals("location", branchClassMapper.getAttribute2property().get("LOCATION"));
      assertEquals("dept", branchClassMapper.getAttribute2property().get("DEPT"));
      assertEquals("BRANCH_ID", branchClassMapper.getProperty2attribute().get("branchId"));
      assertEquals("LOCATION", branchClassMapper.getProperty2attribute().get("location"));
      assertEquals("DEPT", branchClassMapper.getProperty2attribute().get("dept"));

      deptEmpEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_EMP");
      assertEquals(1, mapper.getEVClassMappersByVertex(deptEmpVertexType).size());
      deptEmpClassMapper = mapper.getEVClassMappersByVertex(deptEmpVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(deptEmpEntity).size());
      assertEquals(deptEmpClassMapper, mapper.getEVClassMappersByEntity(deptEmpEntity).get(0));
      assertEquals(deptEmpClassMapper.getEntity(), deptEmpEntity);
      assertEquals(deptEmpClassMapper.getVertexType(), deptEmpVertexType);

      assertEquals(3, deptEmpClassMapper.getAttribute2property().size());
      assertEquals(3, deptEmpClassMapper.getProperty2attribute().size());
      assertEquals("deptId", deptEmpClassMapper.getAttribute2property().get("DEPT_ID"));
      assertEquals("empId", deptEmpClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("hiringYear", deptEmpClassMapper.getAttribute2property().get("HIRING_YEAR"));
      assertEquals("DEPT_ID", deptEmpClassMapper.getProperty2attribute().get("deptId"));
      assertEquals("EMP_ID", deptEmpClassMapper.getProperty2attribute().get("empId"));
      assertEquals("HIRING_YEAR", deptEmpClassMapper.getProperty2attribute().get("hiringYear"));

      deptMgrEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_MANAGER");
      assertEquals(1, mapper.getEVClassMappersByVertex(deptManagerVertexType).size());
      deptManagerClassMapper = mapper.getEVClassMappersByVertex(deptManagerVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(deptMgrEntity).size());
      assertEquals(deptManagerClassMapper, mapper.getEVClassMappersByEntity(deptMgrEntity).get(0));
      assertEquals(deptManagerClassMapper.getEntity(), deptMgrEntity);
      assertEquals(deptManagerClassMapper.getVertexType(), deptManagerVertexType);

      assertEquals(2, deptManagerClassMapper.getAttribute2property().size());
      assertEquals(2, deptManagerClassMapper.getProperty2attribute().size());
      assertEquals("deptId", deptManagerClassMapper.getAttribute2property().get("DEPT_ID"));
      assertEquals("empId", deptManagerClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("DEPT_ID", deptManagerClassMapper.getProperty2attribute().get("deptId"));
      assertEquals("EMP_ID", deptManagerClassMapper.getProperty2attribute().get("empId"));

      // Relationships-Edges Mapping

      it = deptEmpEntity.getOutCanonicalRelationships().iterator();
      hasDepartmentRelationship1 = it.next();
      hasEmployeeRelationship1 = it.next();
      assertFalse(it.hasNext());

      it = deptMgrEntity.getOutCanonicalRelationships().iterator();
      hasDepartmentRelationship2 = it.next();
      hasEmployeeRelationship2 = it.next();
      assertFalse(it.hasNext());

      it = branchEntity.getOutCanonicalRelationships().iterator();
      hasDepartmentRelationship3 = it.next();
      assertFalse(it.hasNext());

      // fetching empEdgeType from the rules as was deleted from the graph model during the aggregation
      assertEquals("HasEmp", empEdgeType.getName());
      assertEquals(employeeVertexType, empEdgeType.getInVertexType());
      assertEquals(0, empEdgeType.getAllProperties().size());

      assertEquals(5, mapper.getRelationship2edgeType().size());
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship1));
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship2));
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship3));
      assertEquals(empEdgeType, mapper.getRelationship2edgeType().get(hasEmployeeRelationship1));
      assertEquals(empEdgeType, mapper.getRelationship2edgeType().get(hasEmployeeRelationship2));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(3, mapper.getEdgeType2relationships().get(deptEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship1));
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship2));
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship3));
      assertEquals(2, mapper.getEdgeType2relationships().get(empEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship1));
      assertTrue(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship2));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(2, mapper.getJoinVertex2aggregatorEdges().size());
      assertTrue(mapper.getJoinVertex2aggregatorEdges().containsKey(deptManagerVertexType));
      assertTrue(mapper.getJoinVertex2aggregatorEdges().containsKey(deptEmpVertexType));
      assertEquals(deptManagerEdgeType, mapper.getJoinVertex2aggregatorEdges().get(deptManagerVertexType).getEdgeType());
      assertEquals("Department", mapper.getJoinVertex2aggregatorEdges().get(deptManagerVertexType).getOutVertexClassName());
      assertEquals("Employee", mapper.getJoinVertex2aggregatorEdges().get(deptManagerVertexType).getInVertexClassName());
      assertEquals(deptEmpEdgeType, mapper.getJoinVertex2aggregatorEdges().get(deptEmpVertexType).getEdgeType());
      assertEquals("Department", mapper.getJoinVertex2aggregatorEdges().get(deptEmpVertexType).getOutVertexClassName());
      assertEquals("Employee", mapper.getJoinVertex2aggregatorEdges().get(deptEmpVertexType).getInVertexClassName());
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
