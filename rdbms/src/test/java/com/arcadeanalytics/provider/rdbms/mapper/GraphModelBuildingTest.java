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

package com.arcadeanalytics.provider.rdbms.mapper;

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
class GraphModelBuildingTest {

  private ER2GraphMapper mapper;
  private DBQueryEngine dbQueryEngine;
  private String driver = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private DataSourceInfo dataSource;
  private Statistics statistics;
  private String executionStrategy;
  private NameResolver nameResolver;
  private HSQLDBDataTypeHandler dataTypeHandler;

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

    statistics = new Statistics();

    dbQueryEngine = new DBQueryEngine(dataSource, 300);
    executionStrategy = "not_specified";
    nameResolver = new JavaConventionNameResolver();
    dataTypeHandler = new HSQLDBDataTypeHandler();
  }

  /*
   *  Two tables Foreign and Parent with a simple primary key imported from the parent table.
   */

  @Test
  void buildGraphModelFromTwoTablesWithOneSimplePK() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding =
          "create memory table BOOK_AUTHOR (ID varchar(256) not null,"
              + " NAME varchar(256) not null, AGE integer not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding =
          "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256), AUTHOR_ID"
              + " varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references"
              + " BOOK_AUTHOR(ID))";
      st.execute(foreignTableBuilding);

      mapper =
          new ER2GraphMapper(
              dataSource,
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

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(2);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(2);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(1);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(1);

      /*
       *  Testing built graph model
       */
      VertexType authorVertexType = mapper.getGraphModel().getVertexTypeByName("BookAuthor");
      VertexType bookVertexType = mapper.getGraphModel().getVertexTypeByName("Book");
      EdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasAuthor");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(2);
      assertThat(authorVertexType).isNotNull();
      assertThat(bookVertexType).isNotNull();

      // properties check
      assertThat(authorVertexType.getProperties().size()).isEqualTo(3);

      assertThat(authorVertexType.getPropertyByName("id")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(authorVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(authorVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(authorVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(authorVertexType.getPropertyByName("name")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(authorVertexType.getPropertyByName("name").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(authorVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(authorVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(authorVertexType.getPropertyByName("age")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("age").getName()).isEqualTo("age");
      assertThat(authorVertexType.getPropertyByName("age").getOriginalType()).isEqualTo("INTEGER");
      assertThat(authorVertexType.getPropertyByName("age").getOrdinalPosition()).isEqualTo(3);
      assertThat(authorVertexType.getPropertyByName("age").isFromPrimaryKey()).isFalse();

      assertThat(bookVertexType.getProperties().size()).isEqualTo(3);

      assertThat(bookVertexType.getPropertyByName("id")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(bookVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(bookVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(bookVertexType.getPropertyByName("title")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("title").getName()).isEqualTo("title");
      assertThat(bookVertexType.getPropertyByName("title").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("title").getOrdinalPosition()).isEqualTo(2);
      assertThat(bookVertexType.getPropertyByName("title").isFromPrimaryKey()).isFalse();

      assertThat(bookVertexType.getPropertyByName("authorId")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("authorId").getName()).isEqualTo("authorId");
      assertThat(bookVertexType.getPropertyByName("authorId").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("authorId").getOrdinalPosition()).isEqualTo(3);
      assertThat(bookVertexType.getPropertyByName("authorId").isFromPrimaryKey()).isFalse();

      // edges check
      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(authorEdgeType).isNotNull();

      assertThat(authorEdgeType.getName()).isEqualTo("HasAuthor");
      assertThat(authorEdgeType.getProperties().size()).isEqualTo(0);
      assertThat(authorEdgeType.getInVertexType().getName()).isEqualTo("BookAuthor");
      assertThat(authorEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(2);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(2);

      Entity bookEntity = mapper.getDataBaseSchema().getEntityByName("BOOK");
      assertThat(mapper.getEVClassMappersByVertex(bookVertexType).size()).isEqualTo(1);
      EVClassMapper bookClassMapper = mapper.getEVClassMappersByVertex(bookVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(bookEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(bookEntity).get(0)).isEqualTo(bookClassMapper);
      assertThat(bookEntity).isEqualTo(bookClassMapper.getEntity());
      assertThat(bookVertexType).isEqualTo(bookClassMapper.getVertexType());

      assertThat(bookClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(bookClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(bookClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(bookClassMapper.getAttribute2property().get("TITLE")).isEqualTo("title");
      assertThat(bookClassMapper.getAttribute2property().get("AUTHOR_ID")).isEqualTo("authorId");
      assertThat(bookClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(bookClassMapper.getProperty2attribute().get("title")).isEqualTo("TITLE");
      assertThat(bookClassMapper.getProperty2attribute().get("authorId")).isEqualTo("AUTHOR_ID");

      Entity bookAuthorEntity = mapper.getDataBaseSchema().getEntityByName("BOOK_AUTHOR");
      assertThat(mapper.getEVClassMappersByVertex(authorVertexType).size()).isEqualTo(1);
      EVClassMapper bookAuthorClassMapper =
          mapper.getEVClassMappersByVertex(authorVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(bookAuthorEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(bookAuthorEntity).get(0))
          .isEqualTo(bookAuthorClassMapper);
      assertThat(bookAuthorEntity).isEqualTo(bookAuthorClassMapper.getEntity());
      assertThat(authorVertexType).isEqualTo(bookAuthorClassMapper.getVertexType());

      assertThat(bookAuthorClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(bookAuthorClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(bookAuthorClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(bookAuthorClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(bookAuthorClassMapper.getAttribute2property().get("AGE")).isEqualTo("age");
      assertThat(bookAuthorClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(bookAuthorClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(bookAuthorClassMapper.getProperty2attribute().get("age")).isEqualTo("AGE");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> it = bookEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasAuthorRelationship = it.next();
      assertThat(it.hasNext()).isFalse();

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);
      assertThat(mapper.getRelationship2edgeType().get(hasAuthorRelationship))
          .isEqualTo(authorEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(authorEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(authorEdgeType)
                  .contains(hasAuthorRelationship))
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
   *  Three tables and two relationships with two different simple primary keys imported .
   */

  @Test
  void buildGraphModelFromThreeTablesWithTwoSimplePK() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String authorTableBuilding =
          "create memory table AUTHOR (ID varchar(256) not null,"
              + " NAME varchar(256) not null, AGE integer not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(authorTableBuilding);

      String bookTableBuilding =
          "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256), AUTHOR_ID"
              + " varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references"
              + " AUTHOR(ID))";
      st.execute(bookTableBuilding);

      String itemTableBuilding =
          "create memory table ITEM (ID varchar(256) not null, BOOK_ID  varchar(256), PRICE"
              + " varchar(256) not null, primary key (ID), foreign key (BOOK_ID) references"
              + " BOOK(ID))";
      st.execute(itemTableBuilding);

      this.mapper =
          new ER2GraphMapper(
              dataSource,
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

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(3);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(3);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(2);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(2);

      /*
       *  Testing built graph model
       */
      VertexType authorVertexType = mapper.getGraphModel().getVertexTypeByName("Author");
      VertexType bookVertexType = mapper.getGraphModel().getVertexTypeByName("Book");
      VertexType itemVertexType = mapper.getGraphModel().getVertexTypeByName("Item");
      EdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasAuthor");
      EdgeType bookEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasBook");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(3);
      assertThat(authorVertexType).isNotNull();
      assertThat(bookVertexType).isNotNull();

      // properties check
      assertThat(authorVertexType.getProperties().size()).isEqualTo(3);

      assertThat(authorVertexType.getPropertyByName("id")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(authorVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(authorVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(authorVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(authorVertexType.getPropertyByName("name")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(authorVertexType.getPropertyByName("name").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(authorVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(authorVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(authorVertexType.getPropertyByName("age")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("age").getName()).isEqualTo("age");
      assertThat(authorVertexType.getPropertyByName("age").getOriginalType()).isEqualTo("INTEGER");
      assertThat(authorVertexType.getPropertyByName("age").getOrdinalPosition()).isEqualTo(3);
      assertThat(authorVertexType.getPropertyByName("age").isFromPrimaryKey()).isFalse();

      assertThat(bookVertexType.getProperties().size()).isEqualTo(3);

      assertThat(bookVertexType.getPropertyByName("id")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(bookVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(bookVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(bookVertexType.getPropertyByName("title")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("title").getName()).isEqualTo("title");
      assertThat(bookVertexType.getPropertyByName("title").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("title").getOrdinalPosition()).isEqualTo(2);
      assertThat(bookVertexType.getPropertyByName("title").isFromPrimaryKey()).isFalse();

      assertThat(bookVertexType.getPropertyByName("authorId")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("authorId").getName()).isEqualTo("authorId");
      assertThat(bookVertexType.getPropertyByName("authorId").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("authorId").getOrdinalPosition()).isEqualTo(3);
      assertThat(bookVertexType.getPropertyByName("authorId").isFromPrimaryKey()).isFalse();

      assertThat(itemVertexType.getProperties().size()).isEqualTo(3);

      assertThat(itemVertexType.getPropertyByName("id")).isNotNull();
      assertThat(itemVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(itemVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(itemVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(itemVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(itemVertexType.getPropertyByName("bookId")).isNotNull();
      assertThat(itemVertexType.getPropertyByName("bookId").getName()).isEqualTo("bookId");
      assertThat(itemVertexType.getPropertyByName("bookId").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(itemVertexType.getPropertyByName("bookId").getOrdinalPosition()).isEqualTo(2);
      assertThat(itemVertexType.getPropertyByName("bookId").isFromPrimaryKey()).isFalse();

      assertThat(itemVertexType.getPropertyByName("price")).isNotNull();
      assertThat(itemVertexType.getPropertyByName("price").getName()).isEqualTo("price");
      assertThat(itemVertexType.getPropertyByName("price").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(itemVertexType.getPropertyByName("price").getOrdinalPosition()).isEqualTo(3);
      assertThat(itemVertexType.getPropertyByName("price").isFromPrimaryKey()).isFalse();

      // edges check
      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(2);
      assertThat(authorEdgeType).isNotNull();
      assertThat(bookEdgeType).isNotNull();

      assertThat(authorEdgeType.getName()).isEqualTo("HasAuthor");
      assertThat(authorEdgeType.getProperties().size()).isEqualTo(0);
      assertThat(authorEdgeType.getInVertexType().getName()).isEqualTo("Author");
      assertThat(authorEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);

      assertThat(bookEdgeType.getName()).isEqualTo("HasBook");
      assertThat(bookEdgeType.getProperties().size()).isEqualTo(0);
      assertThat(bookEdgeType.getInVertexType().getName()).isEqualTo("Book");
      assertThat(bookEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(3);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(3);

      Entity bookEntity = mapper.getDataBaseSchema().getEntityByName("BOOK");
      assertThat(mapper.getEVClassMappersByVertex(bookVertexType).size()).isEqualTo(1);
      EVClassMapper bookClassMapper = mapper.getEVClassMappersByVertex(bookVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(bookEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(bookEntity).get(0)).isEqualTo(bookClassMapper);
      assertThat(bookEntity).isEqualTo(bookClassMapper.getEntity());
      assertThat(bookVertexType).isEqualTo(bookClassMapper.getVertexType());

      assertThat(bookClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(bookClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(bookClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(bookClassMapper.getAttribute2property().get("TITLE")).isEqualTo("title");
      assertThat(bookClassMapper.getAttribute2property().get("AUTHOR_ID")).isEqualTo("authorId");
      assertThat(bookClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(bookClassMapper.getProperty2attribute().get("title")).isEqualTo("TITLE");
      assertThat(bookClassMapper.getProperty2attribute().get("authorId")).isEqualTo("AUTHOR_ID");

      Entity authorEntity = mapper.getDataBaseSchema().getEntityByName("AUTHOR");
      assertThat(mapper.getEVClassMappersByVertex(authorVertexType).size()).isEqualTo(1);
      EVClassMapper authorClassMapper = mapper.getEVClassMappersByVertex(authorVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(authorEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(authorEntity).get(0))
          .isEqualTo(authorClassMapper);
      assertThat(authorEntity).isEqualTo(authorClassMapper.getEntity());
      assertThat(authorVertexType).isEqualTo(authorClassMapper.getVertexType());

      assertThat(authorClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(authorClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(authorClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(authorClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(authorClassMapper.getAttribute2property().get("AGE")).isEqualTo("age");
      assertThat(authorClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(authorClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(authorClassMapper.getProperty2attribute().get("age")).isEqualTo("AGE");

      Entity itemEntity = mapper.getDataBaseSchema().getEntityByName("ITEM");
      assertThat(mapper.getEVClassMappersByVertex(itemVertexType).size()).isEqualTo(1);
      EVClassMapper itemClassMapper = mapper.getEVClassMappersByVertex(itemVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(itemEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(itemEntity).get(0)).isEqualTo(itemClassMapper);
      assertThat(itemEntity).isEqualTo(itemClassMapper.getEntity());
      assertThat(itemVertexType).isEqualTo(itemClassMapper.getVertexType());

      assertThat(itemClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(itemClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(itemClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(itemClassMapper.getAttribute2property().get("BOOK_ID")).isEqualTo("bookId");
      assertThat(itemClassMapper.getAttribute2property().get("PRICE")).isEqualTo("price");
      assertThat(itemClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(itemClassMapper.getProperty2attribute().get("bookId")).isEqualTo("BOOK_ID");
      assertThat(itemClassMapper.getProperty2attribute().get("price")).isEqualTo("PRICE");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> it = bookEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasAuthorRelationship = it.next();
      assertThat(it.hasNext()).isFalse();
      it = itemEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasBookRelationship = it.next();
      assertThat(it.hasNext()).isFalse();

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(2);
      assertThat(mapper.getRelationship2edgeType().get(hasAuthorRelationship))
          .isEqualTo(authorEdgeType);
      assertThat(mapper.getRelationship2edgeType().get(hasBookRelationship))
          .isEqualTo(bookEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(2);
      assertThat(mapper.getEdgeType2relationships().get(authorEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(authorEdgeType)
                  .contains(hasAuthorRelationship))
          .isTrue();
      assertThat(mapper.getEdgeType2relationships().get(bookEdgeType).size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(bookEdgeType).contains(hasBookRelationship))
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
   *  Three tables and two relationships with a simple primary keys twice imported.
   */

  @Test
  void buildGraphModelFromThreeTablesWithOneSimplePKImportedTwice() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String authorTableBuilding =
          "create memory table AUTHOR (ID varchar(256) not null,"
              + " NAME varchar(256) not null, AGE integer not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(authorTableBuilding);

      String bookTableBuilding =
          "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256), AUTHOR_ID"
              + " varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references"
              + " AUTHOR(ID))";
      st.execute(bookTableBuilding);

      String articleTableBuilding =
          "create memory table ARTICLE (ID varchar(256) not null, TITLE  varchar(256), DATE  date,"
              + " AUTHOR_ID varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID)"
              + " references AUTHOR(ID))";
      st.execute(articleTableBuilding);

      this.mapper =
          new ER2GraphMapper(
              dataSource,
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

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(3);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(3);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(1);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(1);

      /*
       *  Testing built graph model
       */
      VertexType authorVertexType = mapper.getGraphModel().getVertexTypeByName("Author");
      VertexType bookVertexType = mapper.getGraphModel().getVertexTypeByName("Book");
      VertexType articleVertexType = mapper.getGraphModel().getVertexTypeByName("Article");
      EdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasAuthor");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(3);
      assertThat(authorVertexType).isNotNull();
      assertThat(bookVertexType).isNotNull();

      // properties check
      assertThat(authorVertexType.getProperties().size()).isEqualTo(3);

      assertThat(authorVertexType.getPropertyByName("id")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(authorVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(authorVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(authorVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(authorVertexType.getPropertyByName("name")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(authorVertexType.getPropertyByName("name").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(authorVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(authorVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(authorVertexType.getPropertyByName("age")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("age").getName()).isEqualTo("age");
      assertThat(authorVertexType.getPropertyByName("age").getOriginalType()).isEqualTo("INTEGER");
      assertThat(authorVertexType.getPropertyByName("age").getOrdinalPosition()).isEqualTo(3);
      assertThat(authorVertexType.getPropertyByName("age").isFromPrimaryKey()).isFalse();

      assertThat(bookVertexType.getProperties().size()).isEqualTo(3);

      assertThat(bookVertexType.getPropertyByName("id")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(bookVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(bookVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(bookVertexType.getPropertyByName("title")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("title").getName()).isEqualTo("title");
      assertThat(bookVertexType.getPropertyByName("title").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("title").getOrdinalPosition()).isEqualTo(2);
      assertThat(bookVertexType.getPropertyByName("title").isFromPrimaryKey()).isFalse();

      assertThat(bookVertexType.getPropertyByName("authorId")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("authorId").getName()).isEqualTo("authorId");
      assertThat(bookVertexType.getPropertyByName("authorId").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("authorId").getOrdinalPosition()).isEqualTo(3);
      assertThat(bookVertexType.getPropertyByName("authorId").isFromPrimaryKey()).isFalse();

      assertThat(articleVertexType.getProperties().size()).isEqualTo(4);

      assertThat(articleVertexType.getPropertyByName("id")).isNotNull();
      assertThat(articleVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(articleVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(articleVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(articleVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(articleVertexType.getPropertyByName("title")).isNotNull();
      assertThat(articleVertexType.getPropertyByName("title").getName()).isEqualTo("title");
      assertThat(articleVertexType.getPropertyByName("title").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(articleVertexType.getPropertyByName("title").getOrdinalPosition()).isEqualTo(2);
      assertThat(articleVertexType.getPropertyByName("title").isFromPrimaryKey()).isFalse();

      assertThat(articleVertexType.getPropertyByName("date")).isNotNull();
      assertThat(articleVertexType.getPropertyByName("date").getName()).isEqualTo("date");
      assertThat(articleVertexType.getPropertyByName("date").getOriginalType()).isEqualTo("DATE");
      assertThat(articleVertexType.getPropertyByName("date").getOrdinalPosition()).isEqualTo(3);
      assertThat(articleVertexType.getPropertyByName("date").isFromPrimaryKey()).isFalse();

      assertThat(articleVertexType.getPropertyByName("authorId")).isNotNull();
      assertThat(articleVertexType.getPropertyByName("authorId").getName()).isEqualTo("authorId");
      assertThat(articleVertexType.getPropertyByName("authorId").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(articleVertexType.getPropertyByName("authorId").getOrdinalPosition()).isEqualTo(4);
      assertThat(articleVertexType.getPropertyByName("authorId").isFromPrimaryKey()).isFalse();

      // edges check
      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(authorEdgeType).isNotNull();

      assertThat(authorEdgeType.getName()).isEqualTo("HasAuthor");
      assertThat(authorEdgeType.getProperties().size()).isEqualTo(0);
      assertThat(authorEdgeType.getInVertexType().getName()).isEqualTo("Author");
      assertThat(authorEdgeType.getNumberRelationshipsRepresented()).isEqualTo(2);

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(3);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(3);

      Entity bookEntity = mapper.getDataBaseSchema().getEntityByName("BOOK");
      assertThat(mapper.getEVClassMappersByVertex(bookVertexType).size()).isEqualTo(1);
      EVClassMapper bookClassMapper = mapper.getEVClassMappersByVertex(bookVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(bookEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(bookEntity).get(0)).isEqualTo(bookClassMapper);
      assertThat(bookEntity).isEqualTo(bookClassMapper.getEntity());
      assertThat(bookVertexType).isEqualTo(bookClassMapper.getVertexType());

      assertThat(bookClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(bookClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(bookClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(bookClassMapper.getAttribute2property().get("TITLE")).isEqualTo("title");
      assertThat(bookClassMapper.getAttribute2property().get("AUTHOR_ID")).isEqualTo("authorId");
      assertThat(bookClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(bookClassMapper.getProperty2attribute().get("title")).isEqualTo("TITLE");
      assertThat(bookClassMapper.getProperty2attribute().get("authorId")).isEqualTo("AUTHOR_ID");

      Entity authorEntity = mapper.getDataBaseSchema().getEntityByName("AUTHOR");
      assertThat(mapper.getEVClassMappersByVertex(authorVertexType).size()).isEqualTo(1);
      EVClassMapper authorClassMapper = mapper.getEVClassMappersByVertex(authorVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(authorEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(authorEntity).get(0))
          .isEqualTo(authorClassMapper);
      assertThat(authorEntity).isEqualTo(authorClassMapper.getEntity());
      assertThat(authorVertexType).isEqualTo(authorClassMapper.getVertexType());

      assertThat(authorClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(authorClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(authorClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(authorClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(authorClassMapper.getAttribute2property().get("AGE")).isEqualTo("age");
      assertThat(authorClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(authorClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(authorClassMapper.getProperty2attribute().get("age")).isEqualTo("AGE");

      Entity articleEntity = mapper.getDataBaseSchema().getEntityByName("ARTICLE");
      assertThat(mapper.getEVClassMappersByVertex(articleVertexType).size()).isEqualTo(1);
      EVClassMapper articleClassMapper = mapper.getEVClassMappersByVertex(articleVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(articleEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(articleEntity).get(0))
          .isEqualTo(articleClassMapper);
      assertThat(articleEntity).isEqualTo(articleClassMapper.getEntity());
      assertThat(articleVertexType).isEqualTo(articleClassMapper.getVertexType());

      assertThat(articleClassMapper.getAttribute2property().size()).isEqualTo(4);
      assertThat(articleClassMapper.getProperty2attribute().size()).isEqualTo(4);
      assertThat(articleClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(articleClassMapper.getAttribute2property().get("TITLE")).isEqualTo("title");
      assertThat(articleClassMapper.getAttribute2property().get("DATE")).isEqualTo("date");
      assertThat(articleClassMapper.getAttribute2property().get("AUTHOR_ID")).isEqualTo("authorId");
      assertThat(articleClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(articleClassMapper.getProperty2attribute().get("title")).isEqualTo("TITLE");
      assertThat(articleClassMapper.getProperty2attribute().get("date")).isEqualTo("DATE");
      assertThat(articleClassMapper.getProperty2attribute().get("authorId")).isEqualTo("AUTHOR_ID");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> it = bookEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasAuthorRelationship1 = it.next();
      assertThat(it.hasNext()).isFalse();
      it = articleEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasAuthorRelationship2 = it.next();
      assertThat(it.hasNext()).isFalse();

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(2);
      assertThat(mapper.getRelationship2edgeType().get(hasAuthorRelationship1))
          .isEqualTo(authorEdgeType);
      assertThat(mapper.getRelationship2edgeType().get(hasAuthorRelationship2))
          .isEqualTo(authorEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(authorEdgeType).size()).isEqualTo(2);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(authorEdgeType)
                  .contains(hasAuthorRelationship1))
          .isTrue();
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(authorEdgeType)
                  .contains(hasAuthorRelationship2))
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
   *  Two tables Foreign and Parent with a composite primary key imported from the parent table.
   */

  @Test
  void buildGraphModelFromThreeTablesWithOneCompositePK() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String authorTableBuilding =
          "create memory table AUTHOR (NAME varchar(256) not null,"
              + " SURNAME varchar(256) not null, AGE integer, primary key (NAME,SURNAME))";
      st = connection.createStatement();
      st.execute(authorTableBuilding);

      String bookTableBuilding =
          "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256), AUTHOR_NAME"
              + " varchar(256) not null, AUTHOR_SURNAME varchar(256) not null, primary key (ID),"
              + " foreign key (AUTHOR_NAME,AUTHOR_SURNAME) references AUTHOR(NAME,SURNAME))";
      st.execute(bookTableBuilding);

      this.mapper =
          new ER2GraphMapper(
              dataSource,
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

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(2);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(2);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(1);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(1);

      /*
       *  Testing built graph model
       */
      VertexType authorVertexType = mapper.getGraphModel().getVertexTypeByName("Author");
      VertexType bookVertexType = mapper.getGraphModel().getVertexTypeByName("Book");
      EdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("Book2Author");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(2);
      assertThat(authorVertexType).isNotNull();
      assertThat(bookVertexType).isNotNull();

      // properties check
      assertThat(authorVertexType.getProperties().size()).isEqualTo(3);

      assertThat(authorVertexType.getPropertyByName("name")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(authorVertexType.getPropertyByName("name").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(authorVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(1);
      assertThat(authorVertexType.getPropertyByName("name").isFromPrimaryKey()).isTrue();

      assertThat(authorVertexType.getPropertyByName("surname")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("surname").getName()).isEqualTo("surname");
      assertThat(authorVertexType.getPropertyByName("surname").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(authorVertexType.getPropertyByName("surname").getOrdinalPosition()).isEqualTo(2);
      assertThat(authorVertexType.getPropertyByName("surname").isFromPrimaryKey()).isTrue();

      assertThat(authorVertexType.getPropertyByName("age")).isNotNull();
      assertThat(authorVertexType.getPropertyByName("age").getName()).isEqualTo("age");
      assertThat(authorVertexType.getPropertyByName("age").getOriginalType()).isEqualTo("INTEGER");
      assertThat(authorVertexType.getPropertyByName("age").getOrdinalPosition()).isEqualTo(3);
      assertThat(authorVertexType.getPropertyByName("age").isFromPrimaryKey()).isFalse();

      assertThat(bookVertexType.getProperties().size()).isEqualTo(4);

      assertThat(bookVertexType.getPropertyByName("id")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(bookVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(bookVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(bookVertexType.getPropertyByName("title")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("title").getName()).isEqualTo("title");
      assertThat(bookVertexType.getPropertyByName("title").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("title").getOrdinalPosition()).isEqualTo(2);
      assertThat(bookVertexType.getPropertyByName("title").isFromPrimaryKey()).isFalse();

      assertThat(bookVertexType.getPropertyByName("authorName")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("authorName").getName()).isEqualTo("authorName");
      assertThat(bookVertexType.getPropertyByName("authorName").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("authorName").getOrdinalPosition()).isEqualTo(3);
      assertThat(bookVertexType.getPropertyByName("authorName").isFromPrimaryKey()).isFalse();

      assertThat(bookVertexType.getPropertyByName("authorSurname")).isNotNull();
      assertThat(bookVertexType.getPropertyByName("authorSurname").getName())
          .isEqualTo("authorSurname");
      assertThat(bookVertexType.getPropertyByName("authorSurname").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(bookVertexType.getPropertyByName("authorSurname").getOrdinalPosition())
          .isEqualTo(4);
      assertThat(bookVertexType.getPropertyByName("authorSurname").isFromPrimaryKey()).isFalse();

      // edges check
      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(1);
      assertThat(authorEdgeType).isNotNull();

      assertThat(authorEdgeType.getName()).isEqualTo("Book2Author");
      assertThat(authorEdgeType.getProperties().size()).isEqualTo(0);
      assertThat(authorEdgeType.getInVertexType().getName()).isEqualTo("Author");
      assertThat(authorEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(2);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(2);

      Entity bookEntity = mapper.getDataBaseSchema().getEntityByName("BOOK");
      assertThat(mapper.getEVClassMappersByVertex(bookVertexType).size()).isEqualTo(1);
      EVClassMapper bookClassMapper = mapper.getEVClassMappersByVertex(bookVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(bookEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(bookEntity).get(0)).isEqualTo(bookClassMapper);
      assertThat(bookEntity).isEqualTo(bookClassMapper.getEntity());
      assertThat(bookVertexType).isEqualTo(bookClassMapper.getVertexType());

      assertThat(bookClassMapper.getAttribute2property().size()).isEqualTo(4);
      assertThat(bookClassMapper.getProperty2attribute().size()).isEqualTo(4);
      assertThat(bookClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(bookClassMapper.getAttribute2property().get("TITLE")).isEqualTo("title");
      assertThat(bookClassMapper.getAttribute2property().get("AUTHOR_NAME"))
          .isEqualTo("authorName");
      assertThat(bookClassMapper.getAttribute2property().get("AUTHOR_SURNAME"))
          .isEqualTo("authorSurname");
      assertThat(bookClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(bookClassMapper.getProperty2attribute().get("title")).isEqualTo("TITLE");
      assertThat(bookClassMapper.getProperty2attribute().get("authorName"))
          .isEqualTo("AUTHOR_NAME");
      assertThat(bookClassMapper.getProperty2attribute().get("authorSurname"))
          .isEqualTo("AUTHOR_SURNAME");

      Entity authorEntity = mapper.getDataBaseSchema().getEntityByName("AUTHOR");
      assertThat(mapper.getEVClassMappersByVertex(authorVertexType).size()).isEqualTo(1);
      EVClassMapper authorClassMapper = mapper.getEVClassMappersByVertex(authorVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(authorEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(authorEntity).get(0))
          .isEqualTo(authorClassMapper);
      assertThat(authorEntity).isEqualTo(authorClassMapper.getEntity());
      assertThat(authorVertexType).isEqualTo(authorClassMapper.getVertexType());

      assertThat(authorClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(authorClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(authorClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(authorClassMapper.getAttribute2property().get("SURNAME")).isEqualTo("surname");
      assertThat(authorClassMapper.getAttribute2property().get("AGE")).isEqualTo("age");
      assertThat(authorClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(authorClassMapper.getProperty2attribute().get("surname")).isEqualTo("SURNAME");
      assertThat(authorClassMapper.getProperty2attribute().get("age")).isEqualTo("AGE");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> it = bookEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasAuthorRelationship = it.next();
      assertThat(it.hasNext()).isFalse();

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(1);
      assertThat(mapper.getRelationship2edgeType().get(hasAuthorRelationship))
          .isEqualTo(authorEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(authorEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(authorEdgeType)
                  .contains(hasAuthorRelationship))
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
   *  Three tables: 2 Parent and 1 join table which imports two different simple primary key.
   */

  @Test
  void buildGraphModelFromJoinTableAnd2ParentTables() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String filmTableBuilding =
          "create memory table FILM (ID varchar(256) not null,"
              + " TITLE varchar(256) not null, YEAR date, primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      String actorTableBuilding =
          "create memory table ACTOR (ID varchar(256) not null,"
              + " NAME varchar(256) not null, SURNAME varchar(256) not null, primary key (ID))";
      st.execute(actorTableBuilding);

      String film2actorTableBuilding =
          "create memory table FILM_ACTOR (FILM_ID varchar(256) not null,"
              + " ACTOR_ID varchar(256) not null, primary key (FILM_ID,ACTOR_ID),"
              + " foreign key (FILM_ID) references FILM(ID),"
              + " foreign key (ACTOR_ID) references ACTOR(ID))";
      st.execute(film2actorTableBuilding);

      this.mapper =
          new ER2GraphMapper(
              dataSource,
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

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(3);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(3);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(2);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(2);

      /*
       *  Testing built graph model
       */
      VertexType actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      VertexType filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      VertexType film2actorVertexType = mapper.getGraphModel().getVertexTypeByName("FilmActor");
      EdgeType actorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasActor");
      EdgeType filmEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasFilm");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(3);
      assertThat(actorVertexType).isNotNull();
      assertThat(filmVertexType).isNotNull();
      assertThat(film2actorVertexType).isNotNull();

      // properties check
      assertThat(actorVertexType.getProperties().size()).isEqualTo(3);

      assertThat(actorVertexType.getPropertyByName("id")).isNotNull();
      assertThat(actorVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(actorVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(actorVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(actorVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(actorVertexType.getPropertyByName("name")).isNotNull();
      assertThat(actorVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(actorVertexType.getPropertyByName("name").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(actorVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(2);
      assertThat(actorVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(actorVertexType.getPropertyByName("surname")).isNotNull();
      assertThat(actorVertexType.getPropertyByName("surname").getName()).isEqualTo("surname");
      assertThat(actorVertexType.getPropertyByName("surname").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(actorVertexType.getPropertyByName("surname").getOrdinalPosition()).isEqualTo(3);
      assertThat(actorVertexType.getPropertyByName("surname").isFromPrimaryKey()).isFalse();

      assertThat(filmVertexType.getProperties().size()).isEqualTo(3);

      assertThat(filmVertexType.getPropertyByName("id")).isNotNull();
      assertThat(filmVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(filmVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(filmVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(filmVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(filmVertexType.getPropertyByName("title")).isNotNull();
      assertThat(filmVertexType.getPropertyByName("title").getName()).isEqualTo("title");
      assertThat(filmVertexType.getPropertyByName("title").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(filmVertexType.getPropertyByName("title").getOrdinalPosition()).isEqualTo(2);
      assertThat(filmVertexType.getPropertyByName("title").isFromPrimaryKey()).isFalse();

      assertThat(filmVertexType.getPropertyByName("year")).isNotNull();
      assertThat(filmVertexType.getPropertyByName("year").getName()).isEqualTo("year");
      assertThat(filmVertexType.getPropertyByName("year").getOriginalType()).isEqualTo("DATE");
      assertThat(filmVertexType.getPropertyByName("year").getOrdinalPosition()).isEqualTo(3);
      assertThat(filmVertexType.getPropertyByName("year").isFromPrimaryKey()).isFalse();

      assertThat(film2actorVertexType.getProperties().size()).isEqualTo(2);

      assertThat(film2actorVertexType.getPropertyByName("filmId")).isNotNull();
      assertThat(film2actorVertexType.getPropertyByName("filmId").getName()).isEqualTo("filmId");
      assertThat(film2actorVertexType.getPropertyByName("filmId").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(film2actorVertexType.getPropertyByName("filmId").getOrdinalPosition())
          .isEqualTo(1);
      assertThat(film2actorVertexType.getPropertyByName("filmId").isFromPrimaryKey()).isTrue();

      assertThat(film2actorVertexType.getPropertyByName("actorId")).isNotNull();
      assertThat(film2actorVertexType.getPropertyByName("actorId").getName()).isEqualTo("actorId");
      assertThat(film2actorVertexType.getPropertyByName("actorId").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(film2actorVertexType.getPropertyByName("actorId").getOrdinalPosition())
          .isEqualTo(2);
      assertThat(film2actorVertexType.getPropertyByName("actorId").isFromPrimaryKey()).isTrue();

      // edges check
      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(2);
      assertThat(filmEdgeType).isNotNull();
      assertThat(actorEdgeType).isNotNull();

      assertThat(filmEdgeType.getName()).isEqualTo("HasFilm");
      assertThat(filmEdgeType.getProperties().size()).isEqualTo(0);
      assertThat(filmEdgeType.getInVertexType().getName()).isEqualTo("Film");
      assertThat(filmEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);

      assertThat(actorEdgeType.getName()).isEqualTo("HasActor");
      assertThat(actorEdgeType.getProperties().size()).isEqualTo(0);
      assertThat(actorEdgeType.getInVertexType().getName()).isEqualTo("Actor");
      assertThat(actorEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(3);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(3);

      Entity filmEntity = mapper.getDataBaseSchema().getEntityByName("FILM");
      assertThat(mapper.getEVClassMappersByVertex(filmVertexType).size()).isEqualTo(1);
      EVClassMapper filmClassMapper = mapper.getEVClassMappersByVertex(filmVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(filmEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(filmEntity).get(0)).isEqualTo(filmClassMapper);
      assertThat(filmEntity).isEqualTo(filmClassMapper.getEntity());
      assertThat(filmVertexType).isEqualTo(filmClassMapper.getVertexType());

      assertThat(filmClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(filmClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(filmClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(filmClassMapper.getAttribute2property().get("TITLE")).isEqualTo("title");
      assertThat(filmClassMapper.getAttribute2property().get("YEAR")).isEqualTo("year");
      assertThat(filmClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(filmClassMapper.getProperty2attribute().get("title")).isEqualTo("TITLE");
      assertThat(filmClassMapper.getProperty2attribute().get("year")).isEqualTo("YEAR");

      Entity actorEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR");
      assertThat(mapper.getEVClassMappersByVertex(actorVertexType).size()).isEqualTo(1);
      EVClassMapper actorClassMapper = mapper.getEVClassMappersByVertex(actorVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(actorEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(actorEntity).get(0)).isEqualTo(actorClassMapper);
      assertThat(actorEntity).isEqualTo(actorClassMapper.getEntity());
      assertThat(actorVertexType).isEqualTo(actorClassMapper.getVertexType());

      assertThat(actorClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(actorClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(actorClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(actorClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(actorClassMapper.getAttribute2property().get("SURNAME")).isEqualTo("surname");
      assertThat(actorClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(actorClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");
      assertThat(actorClassMapper.getProperty2attribute().get("surname")).isEqualTo("SURNAME");

      Entity filmActorEntity = mapper.getDataBaseSchema().getEntityByName("FILM_ACTOR");
      assertThat(mapper.getEVClassMappersByVertex(film2actorVertexType).size()).isEqualTo(1);
      EVClassMapper filmActorClassMapper =
          mapper.getEVClassMappersByVertex(film2actorVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(filmActorEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(filmActorEntity).get(0))
          .isEqualTo(filmActorClassMapper);
      assertThat(filmActorEntity).isEqualTo(filmActorClassMapper.getEntity());
      assertThat(film2actorVertexType).isEqualTo(filmActorClassMapper.getVertexType());

      assertThat(filmActorClassMapper.getAttribute2property().size()).isEqualTo(2);
      assertThat(filmActorClassMapper.getProperty2attribute().size()).isEqualTo(2);
      assertThat(filmActorClassMapper.getAttribute2property().get("FILM_ID")).isEqualTo("filmId");
      assertThat(filmActorClassMapper.getAttribute2property().get("ACTOR_ID")).isEqualTo("actorId");
      assertThat(filmActorClassMapper.getProperty2attribute().get("filmId")).isEqualTo("FILM_ID");
      assertThat(filmActorClassMapper.getProperty2attribute().get("actorId")).isEqualTo("ACTOR_ID");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> it =
          filmActorEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasActorRelationship = it.next();
      CanonicalRelationship hasFilmRelationship = it.next();
      assertThat(it.hasNext()).isFalse();

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(2);
      assertThat(mapper.getRelationship2edgeType().get(hasFilmRelationship))
          .isEqualTo(filmEdgeType);
      assertThat(mapper.getRelationship2edgeType().get(hasActorRelationship))
          .isEqualTo(actorEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(2);
      assertThat(mapper.getEdgeType2relationships().get(filmEdgeType).size()).isEqualTo(1);
      assertThat(mapper.getEdgeType2relationships().get(filmEdgeType).contains(hasFilmRelationship))
          .isTrue();
      assertThat(mapper.getEdgeType2relationships().get(actorEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper.getEdgeType2relationships().get(actorEdgeType).contains(hasActorRelationship))
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
   *  Two tables: 1 Foreign and 1 Parent (parent has an inner referential integrity).
   *  The primary key is imported both by the foreign table and the attribute of the parent table itself.
   */

  @Test
  void buildGraphModelFromTwoTablesWithOneSimplePKImportedTwice() {
    Connection connection = null;
    Statement st = null;

    try {
      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding =
          "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"
              + " MGR_ID varchar(256) not null, NAME varchar(256) not null, primary key (EMP_ID), "
              + " foreign key (MGR_ID) references EMPLOYEE(EMP_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding =
          "create memory table PROJECT (ID  varchar(256), TITLE varchar(256) not null,"
              + " PROJECT_MANAGER varchar(256) not null, primary key (ID), foreign key"
              + " (PROJECT_MANAGER) references EMPLOYEE(EMP_ID))";
      st.execute(foreignTableBuilding);

      this.mapper =
          new ER2GraphMapper(
              dataSource,
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

      assertThat(statistics.totalNumberOfModelVertices).isEqualTo(2);
      assertThat(statistics.builtModelVertexTypes).isEqualTo(2);
      assertThat(statistics.totalNumberOfModelEdges).isEqualTo(2);
      assertThat(statistics.builtModelEdgeTypes).isEqualTo(2);

      /*
       *  Testing built graph model
       */
      VertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      VertexType projectVertexType = mapper.getGraphModel().getVertexTypeByName("Project");
      EdgeType projectManagerEdgeType =
          mapper.getGraphModel().getEdgeTypeByName("HasProjectManager");
      EdgeType mgrEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasMgr");

      // vertices check
      assertThat(mapper.getGraphModel().getVerticesType().size()).isEqualTo(2);
      assertThat(employeeVertexType).isNotNull();
      assertThat(projectVertexType).isNotNull();

      // properties check
      assertThat(employeeVertexType.getProperties().size()).isEqualTo(3);

      assertThat(employeeVertexType.getPropertyByName("empId")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("empId").getName()).isEqualTo("empId");
      assertThat(employeeVertexType.getPropertyByName("empId").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("empId").getOrdinalPosition()).isEqualTo(1);
      assertThat(employeeVertexType.getPropertyByName("empId").isFromPrimaryKey()).isTrue();

      assertThat(employeeVertexType.getPropertyByName("mgrId")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("mgrId").getName()).isEqualTo("mgrId");
      assertThat(employeeVertexType.getPropertyByName("mgrId").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("mgrId").getOrdinalPosition()).isEqualTo(2);
      assertThat(employeeVertexType.getPropertyByName("mgrId").isFromPrimaryKey()).isFalse();

      assertThat(employeeVertexType.getPropertyByName("name")).isNotNull();
      assertThat(employeeVertexType.getPropertyByName("name").getName()).isEqualTo("name");
      assertThat(employeeVertexType.getPropertyByName("name").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(employeeVertexType.getPropertyByName("name").getOrdinalPosition()).isEqualTo(3);
      assertThat(employeeVertexType.getPropertyByName("name").isFromPrimaryKey()).isFalse();

      assertThat(projectVertexType.getProperties().size()).isEqualTo(3);

      assertThat(projectVertexType.getPropertyByName("id")).isNotNull();
      assertThat(projectVertexType.getPropertyByName("id").getName()).isEqualTo("id");
      assertThat(projectVertexType.getPropertyByName("id").getOriginalType()).isEqualTo("VARCHAR");
      assertThat(projectVertexType.getPropertyByName("id").getOrdinalPosition()).isEqualTo(1);
      assertThat(projectVertexType.getPropertyByName("id").isFromPrimaryKey()).isTrue();

      assertThat(projectVertexType.getPropertyByName("title")).isNotNull();
      assertThat(projectVertexType.getPropertyByName("title").getName()).isEqualTo("title");
      assertThat(projectVertexType.getPropertyByName("title").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectVertexType.getPropertyByName("title").getOrdinalPosition()).isEqualTo(2);
      assertThat(projectVertexType.getPropertyByName("title").isFromPrimaryKey()).isFalse();

      assertThat(projectVertexType.getPropertyByName("projectManager")).isNotNull();
      assertThat(projectVertexType.getPropertyByName("projectManager").getName())
          .isEqualTo("projectManager");
      assertThat(projectVertexType.getPropertyByName("projectManager").getOriginalType())
          .isEqualTo("VARCHAR");
      assertThat(projectVertexType.getPropertyByName("projectManager").getOrdinalPosition())
          .isEqualTo(3);
      assertThat(projectVertexType.getPropertyByName("projectManager").isFromPrimaryKey())
          .isFalse();

      // edges check
      assertThat(mapper.getGraphModel().getEdgesType().size()).isEqualTo(2);
      assertThat(mgrEdgeType).isNotNull();
      assertThat(projectManagerEdgeType).isNotNull();

      assertThat(mgrEdgeType.getName()).isEqualTo("HasMgr");
      assertThat(mgrEdgeType.getProperties().size()).isEqualTo(0);
      assertThat(mgrEdgeType.getInVertexType().getName()).isEqualTo("Employee");
      assertThat(mgrEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);

      assertThat(projectManagerEdgeType.getName()).isEqualTo("HasProjectManager");
      assertThat(projectManagerEdgeType.getProperties().size()).isEqualTo(0);
      assertThat(projectManagerEdgeType.getInVertexType().getName()).isEqualTo("Employee");
      assertThat(projectManagerEdgeType.getNumberRelationshipsRepresented()).isEqualTo(1);

      /*
       * Rules check
       */

      // Classes Mapping

      assertThat(mapper.getVertexType2EVClassMappers().size()).isEqualTo(2);
      assertThat(mapper.getEntity2EVClassMappers().size()).isEqualTo(2);

      Entity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      assertThat(mapper.getEVClassMappersByVertex(employeeVertexType).size()).isEqualTo(1);
      EVClassMapper employeeClassMapper =
          mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(employeeEntity).get(0))
          .isEqualTo(employeeClassMapper);
      assertThat(employeeEntity).isEqualTo(employeeClassMapper.getEntity());
      assertThat(employeeVertexType).isEqualTo(employeeClassMapper.getVertexType());

      assertThat(employeeClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(employeeClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(employeeClassMapper.getAttribute2property().get("EMP_ID")).isEqualTo("empId");
      assertThat(employeeClassMapper.getAttribute2property().get("MGR_ID")).isEqualTo("mgrId");
      assertThat(employeeClassMapper.getAttribute2property().get("NAME")).isEqualTo("name");
      assertThat(employeeClassMapper.getProperty2attribute().get("empId")).isEqualTo("EMP_ID");
      assertThat(employeeClassMapper.getProperty2attribute().get("mgrId")).isEqualTo("MGR_ID");
      assertThat(employeeClassMapper.getProperty2attribute().get("name")).isEqualTo("NAME");

      Entity projectEntity = mapper.getDataBaseSchema().getEntityByName("PROJECT");
      assertThat(mapper.getEVClassMappersByVertex(projectVertexType).size()).isEqualTo(1);
      EVClassMapper projectClassMapper = mapper.getEVClassMappersByVertex(projectVertexType).get(0);
      assertThat(mapper.getEVClassMappersByEntity(projectEntity).size()).isEqualTo(1);
      assertThat(mapper.getEVClassMappersByEntity(projectEntity).get(0))
          .isEqualTo(projectClassMapper);
      assertThat(projectEntity).isEqualTo(projectClassMapper.getEntity());
      assertThat(projectVertexType).isEqualTo(projectClassMapper.getVertexType());

      assertThat(projectClassMapper.getAttribute2property().size()).isEqualTo(3);
      assertThat(projectClassMapper.getProperty2attribute().size()).isEqualTo(3);
      assertThat(projectClassMapper.getAttribute2property().get("ID")).isEqualTo("id");
      assertThat(projectClassMapper.getAttribute2property().get("TITLE")).isEqualTo("title");
      assertThat(projectClassMapper.getAttribute2property().get("PROJECT_MANAGER"))
          .isEqualTo("projectManager");
      assertThat(projectClassMapper.getProperty2attribute().get("id")).isEqualTo("ID");
      assertThat(projectClassMapper.getProperty2attribute().get("title")).isEqualTo("TITLE");
      assertThat(projectClassMapper.getProperty2attribute().get("projectManager"))
          .isEqualTo("PROJECT_MANAGER");

      // Relationships-Edges Mapping

      Iterator<CanonicalRelationship> it = employeeEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasManagerRelationship = it.next();
      assertThat(it.hasNext()).isFalse();
      it = projectEntity.getOutCanonicalRelationships().iterator();
      CanonicalRelationship hasProjectManagerRelationship = it.next();
      assertThat(it.hasNext()).isFalse();

      assertThat(mapper.getRelationship2edgeType().size()).isEqualTo(2);
      assertThat(mapper.getRelationship2edgeType().get(hasManagerRelationship))
          .isEqualTo(mgrEdgeType);
      assertThat(mapper.getRelationship2edgeType().get(hasProjectManagerRelationship))
          .isEqualTo(projectManagerEdgeType);

      assertThat(mapper.getEdgeType2relationships().size()).isEqualTo(2);
      assertThat(mapper.getEdgeType2relationships().get(mgrEdgeType).size()).isEqualTo(1);
      assertThat(
              mapper.getEdgeType2relationships().get(mgrEdgeType).contains(hasManagerRelationship))
          .isTrue();
      assertThat(mapper.getEdgeType2relationships().get(projectManagerEdgeType).size())
          .isEqualTo(1);
      assertThat(
              mapper
                  .getEdgeType2relationships()
                  .get(projectManagerEdgeType)
                  .contains(hasProjectManagerRelationship))
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
