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
import com.arcadeanalytics.provider.rdbms.model.dbschema.CanonicalRelationship;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
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

class SourceSchemaBuildingTest {

    private ER2GraphMapper mapper;
    private DBQueryEngine dbQueryEngine;
    private String driver = "org.hsqldb.jdbc.JDBCDriver";
    private String jurl = "jdbc:hsqldb:mem:mydb";
    private String username = "SA";
    private String password = "";
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
                false
            );
        dbQueryEngine = new DBQueryEngine(dataSource, 300);
        executionStrategy = "not_specified";
        nameResolver = new JavaConventionNameResolver();
        dataTypeHandler = new HSQLDBDataTypeHandler();

        statistics = new Statistics();

        this.mapper = new ER2GraphMapper(dataSource, null, null, dbQueryEngine, dataTypeHandler, executionStrategy, nameResolver, statistics);
    }

    /*
     *  Two Foreign tables and one Parent with a simple primary key imported from the parent table.
     */

    @Test
    void buildSourceSchemaFromTwoTablesWithOneSimplePK() {
        Connection connection = null;
        Statement st = null;

        try {
            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String parentTableBuilding =
                "create memory table PARENT_AUTHOR (AUTHOR_ID varchar(256) not null," + " AUTHOR_NAME varchar(256) not null, primary key (AUTHOR_ID))";
            st = connection.createStatement();
            st.execute(parentTableBuilding);

            String foreignTableBuilding =
                "create memory table FOREIGN_BOOK (BOOK_ID varchar(256) not null, TITLE  varchar(256)," +
                " AUTHOR varchar(256) not null, primary key (BOOK_ID), foreign key (AUTHOR) references PARENT_AUTHOR(AUTHOR_ID))";
            st.execute(foreignTableBuilding);

            this.mapper.buildSourceDatabaseSchema();

            /*
             *  Testing context information
             */

            assertThat(statistics.totalNumberOfEntities).isEqualTo(2);
            assertThat(statistics.builtEntities).isEqualTo(2);
            assertThat(statistics.totalNumberOfRelationships).isEqualTo(1);
            assertThat(statistics.builtRelationships).isEqualTo(1);

            /*
             *  Testing built source db schema
             */

            Entity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_AUTHOR");
            Entity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_BOOK");

            // entities check
            assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(2);
            assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(1);
            assertThat(parentEntity).isNotNull();
            assertThat(foreignEntity).isNotNull();

            // attributes check
            assertThat(parentEntity.getAttributes().size()).isEqualTo(2);

            assertThat(parentEntity.getAttributeByName("AUTHOR_ID")).isNotNull();
            assertThat(parentEntity.getAttributeByName("AUTHOR_ID").getName()).isEqualTo("AUTHOR_ID");
            assertThat(parentEntity.getAttributeByName("AUTHOR_ID").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("AUTHOR_ID").getOrdinalPosition()).isEqualTo(1);
            assertThat(parentEntity.getAttributeByName("AUTHOR_ID").getBelongingEntity().getName()).isEqualTo("PARENT_AUTHOR");

            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME")).isNotNull();
            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME").getName()).isEqualTo("AUTHOR_NAME");
            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME").getOrdinalPosition()).isEqualTo(2);
            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME").getBelongingEntity().getName()).isEqualTo("PARENT_AUTHOR");

            assertThat(foreignEntity.getAttributes().size()).isEqualTo(3);

            assertThat(foreignEntity.getAttributeByName("BOOK_ID")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("BOOK_ID").getName()).isEqualTo("BOOK_ID");
            assertThat(foreignEntity.getAttributeByName("BOOK_ID").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("BOOK_ID").getOrdinalPosition()).isEqualTo(1);
            assertThat(foreignEntity.getAttributeByName("BOOK_ID").getBelongingEntity().getName()).isEqualTo("FOREIGN_BOOK");

            assertThat(foreignEntity.getAttributeByName("TITLE")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("TITLE").getName()).isEqualTo("TITLE");
            assertThat(foreignEntity.getAttributeByName("TITLE").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("TITLE").getOrdinalPosition()).isEqualTo(2);
            assertThat(foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName()).isEqualTo("FOREIGN_BOOK");

            assertThat(foreignEntity.getAttributeByName("AUTHOR")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("AUTHOR").getName()).isEqualTo("AUTHOR");
            assertThat(foreignEntity.getAttributeByName("AUTHOR").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("AUTHOR").getOrdinalPosition()).isEqualTo(3);
            assertThat(foreignEntity.getAttributeByName("AUTHOR").getBelongingEntity().getName()).isEqualTo("FOREIGN_BOOK");

            // relationship, primary and foreign key check
            assertThat(foreignEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
            assertThat(parentEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
            assertThat(foreignEntity.getInCanonicalRelationships().size()).isEqualTo(0);
            assertThat(parentEntity.getInCanonicalRelationships().size()).isEqualTo(1);
            assertThat(parentEntity.getForeignKeys().size()).isEqualTo(0);
            assertThat(foreignEntity.getForeignKeys().size()).isEqualTo(1);

            Iterator<CanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("PARENT_AUTHOR");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("FOREIGN_BOOK");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(parentEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(foreignEntity.getForeignKeys().get(0));

            Iterator<CanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship2 = it2.next();
            assertThat(currentRelationship2).isEqualTo(currentRelationship);

            assertThat(foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName()).isEqualTo("AUTHOR");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("AUTHOR_ID");

            assertThat(it.hasNext()).isFalse();
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
     *  Two Foreign tables and one Parent with a composite primary key imported from the parent table.
     */

    @Test
    void buildSourceSchemaFromThreeTablesWithOneCompositePK() {
        Connection connection = null;
        Statement st = null;

        try {
            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String parentTableBuilding =
                "create memory table PARENT_AUTHOR (AUTHOR_NAME varchar(256) not null," +
                " AUTHOR_SURNAME varchar(256) not null, AGE INTEGER, primary key (AUTHOR_NAME,AUTHOR_SURNAME))";
            st = connection.createStatement();
            st.execute(parentTableBuilding);

            String foreignTableBuilding =
                "create memory table FOREIGN_BOOK (TITLE  varchar(256)," +
                " AUTHOR_NAME varchar(256) not null, AUTHOR_SURNAME varchar(256) not null, primary key (TITLE)," +
                " foreign key (AUTHOR_NAME,AUTHOR_SURNAME) references PARENT_AUTHOR(AUTHOR_NAME,AUTHOR_SURNAME))";
            st.execute(foreignTableBuilding);

            mapper.buildSourceDatabaseSchema();

            /*
             *  Testing context information
             */

            assertThat(statistics.totalNumberOfEntities).isEqualTo(2);
            assertThat(statistics.builtEntities).isEqualTo(2);
            assertThat(statistics.totalNumberOfRelationships).isEqualTo(1);
            assertThat(statistics.builtRelationships).isEqualTo(1);

            /*
             *  Testing built source db schema
             */

            Entity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_AUTHOR");
            Entity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_BOOK");

            // entities check
            assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(2);
            assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(1);
            assertThat(parentEntity).isNotNull();
            assertThat(foreignEntity).isNotNull();

            // attributes check
            assertThat(parentEntity.getAttributes().size()).isEqualTo(3);

            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME")).isNotNull();
            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME").getName()).isEqualTo("AUTHOR_NAME");
            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME").getOrdinalPosition()).isEqualTo(1);
            assertThat(parentEntity.getAttributeByName("AUTHOR_NAME").getBelongingEntity().getName()).isEqualTo("PARENT_AUTHOR");

            assertThat(parentEntity.getAttributeByName("AUTHOR_SURNAME")).isNotNull();
            assertThat(parentEntity.getAttributeByName("AUTHOR_SURNAME").getName()).isEqualTo("AUTHOR_SURNAME");
            assertThat(parentEntity.getAttributeByName("AUTHOR_SURNAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("AUTHOR_SURNAME").getOrdinalPosition()).isEqualTo(2);
            assertThat(parentEntity.getAttributeByName("AUTHOR_SURNAME").getBelongingEntity().getName()).isEqualTo("PARENT_AUTHOR");

            assertThat(parentEntity.getAttributeByName("AGE")).isNotNull();
            assertThat(parentEntity.getAttributeByName("AGE").getName()).isEqualTo("AGE");
            assertThat(parentEntity.getAttributeByName("AGE").getDataType()).isEqualTo("INTEGER");
            assertThat(parentEntity.getAttributeByName("AGE").getOrdinalPosition()).isEqualTo(3);
            assertThat(parentEntity.getAttributeByName("AGE").getBelongingEntity().getName()).isEqualTo("PARENT_AUTHOR");

            assertThat(foreignEntity.getAttributes().size()).isEqualTo(3);

            assertThat(foreignEntity.getAttributeByName("TITLE")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("TITLE").getName()).isEqualTo("TITLE");
            assertThat(foreignEntity.getAttributeByName("TITLE").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("TITLE").getOrdinalPosition()).isEqualTo(1);
            assertThat(foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName()).isEqualTo("FOREIGN_BOOK");

            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME").getName()).isEqualTo("AUTHOR_NAME");
            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME").getOrdinalPosition()).isEqualTo(2);
            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME").getBelongingEntity().getName()).isEqualTo("FOREIGN_BOOK");

            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME").getName()).isEqualTo("AUTHOR_SURNAME");
            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME").getOrdinalPosition()).isEqualTo(3);
            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME").getBelongingEntity().getName()).isEqualTo("FOREIGN_BOOK");

            // relationship, primary and foreign key check
            assertThat(foreignEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
            assertThat(parentEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
            assertThat(foreignEntity.getInCanonicalRelationships().size()).isEqualTo(0);
            assertThat(parentEntity.getInCanonicalRelationships().size()).isEqualTo(1);
            assertThat(parentEntity.getForeignKeys().size()).isEqualTo(0);
            assertThat(foreignEntity.getForeignKeys().size()).isEqualTo(1);

            Iterator<CanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("PARENT_AUTHOR");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("FOREIGN_BOOK");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(parentEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(foreignEntity.getForeignKeys().get(0));

            Iterator<CanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship2 = it2.next();
            assertThat(currentRelationship2).isEqualTo(currentRelationship);

            assertThat(foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName()).isEqualTo("AUTHOR_NAME");
            assertThat(foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(1).getName()).isEqualTo("AUTHOR_SURNAME");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("AUTHOR_NAME");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(1).getName()).isEqualTo("AUTHOR_SURNAME");

            assertThat(it.hasNext()).isFalse();
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
     *  Two Foreign tables and one Parent with a simple primary key imported twice from the parent table.
     */

    @Test
    void buildSourceSchemaFromThreeTablesWithOneSimplePKImportedTwice() {
        Connection connection = null;
        Statement st = null;

        try {
            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String parentTableBuilding =
                "create memory table PARENT_PERSON (PERSON_ID varchar(256) not null," + " NAME varchar(256) not null, primary key (PERSON_ID))";
            st = connection.createStatement();
            st.execute(parentTableBuilding);

            String foreignTableBuilding =
                "create memory table FOREIGN_ARTICLE (TITLE  varchar(256)," +
                " AUTHOR varchar(256) not null, TRANSLATOR varchar(256) not null, primary key (TITLE)," +
                " foreign key (AUTHOR) references PARENT_PERSON(PERSON_ID)," +
                " foreign key (TRANSLATOR) references PARENT_PERSON(PERSON_ID))";
            st.execute(foreignTableBuilding);

            mapper.buildSourceDatabaseSchema();

            /*
             *  Testing context information
             */

            assertThat(statistics.totalNumberOfEntities).isEqualTo(2);
            assertThat(statistics.builtEntities).isEqualTo(2);
            assertThat(statistics.totalNumberOfRelationships).isEqualTo(2);
            assertThat(statistics.builtRelationships).isEqualTo(2);

            /*
             *  Testing built source db schema
             */

            Entity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_PERSON");
            Entity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_ARTICLE");

            // entities check
            assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(2);
            assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(2);
            assertThat(parentEntity).isNotNull();
            assertThat(foreignEntity).isNotNull();

            // attributes check
            assertThat(parentEntity.getAttributes().size()).isEqualTo(2);

            assertThat(parentEntity.getAttributeByName("PERSON_ID")).isNotNull();
            assertThat(parentEntity.getAttributeByName("PERSON_ID").getName()).isEqualTo("PERSON_ID");
            assertThat(parentEntity.getAttributeByName("PERSON_ID").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("PERSON_ID").getOrdinalPosition()).isEqualTo(1);
            assertThat(parentEntity.getAttributeByName("PERSON_ID").getBelongingEntity().getName()).isEqualTo("PARENT_PERSON");

            assertThat(parentEntity.getAttributeByName("NAME")).isNotNull();
            assertThat(parentEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
            assertThat(parentEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(2);
            assertThat(parentEntity.getAttributeByName("NAME").getBelongingEntity().getName()).isEqualTo("PARENT_PERSON");

            assertThat(foreignEntity.getAttributes().size()).isEqualTo(3);

            assertThat(foreignEntity.getAttributeByName("TITLE")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("TITLE").getName()).isEqualTo("TITLE");
            assertThat(foreignEntity.getAttributeByName("TITLE").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("TITLE").getOrdinalPosition()).isEqualTo(1);
            assertThat(foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName()).isEqualTo("FOREIGN_ARTICLE");

            assertThat(foreignEntity.getAttributeByName("AUTHOR")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("AUTHOR").getName()).isEqualTo("AUTHOR");
            assertThat(foreignEntity.getAttributeByName("AUTHOR").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("AUTHOR").getOrdinalPosition()).isEqualTo(2);
            assertThat(foreignEntity.getAttributeByName("AUTHOR").getBelongingEntity().getName()).isEqualTo("FOREIGN_ARTICLE");

            assertThat(foreignEntity.getAttributeByName("TRANSLATOR")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR").getName()).isEqualTo("TRANSLATOR");
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR").getOrdinalPosition()).isEqualTo(3);
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR").getBelongingEntity().getName()).isEqualTo("FOREIGN_ARTICLE");

            // relationship, primary and foreign key check
            assertThat(foreignEntity.getOutCanonicalRelationships().size()).isEqualTo(2);
            assertThat(parentEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
            assertThat(foreignEntity.getInCanonicalRelationships().size()).isEqualTo(0);
            assertThat(parentEntity.getInCanonicalRelationships().size()).isEqualTo(2);
            assertThat(parentEntity.getForeignKeys().size()).isEqualTo(0);
            assertThat(foreignEntity.getForeignKeys().size()).isEqualTo(2);

            // first relationship
            Iterator<CanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("PARENT_PERSON");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("FOREIGN_ARTICLE");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(parentEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(foreignEntity.getForeignKeys().get(0));

            Iterator<CanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship2 = it2.next();
            assertThat(currentRelationship2).isEqualTo(currentRelationship);

            assertThat(foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName()).isEqualTo("AUTHOR");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("PERSON_ID");

            // second relationship
            currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("PARENT_PERSON");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("FOREIGN_ARTICLE");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(parentEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(foreignEntity.getForeignKeys().get(1));

            currentRelationship2 = it2.next();
            assertThat(currentRelationship2).isEqualTo(currentRelationship);

            assertThat(foreignEntity.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName()).isEqualTo("TRANSLATOR");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("PERSON_ID");

            assertThat(it.hasNext()).isFalse();
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
     *  Two Foreign tables and one Parent with a composite primary key imported twice from the parent table.
     */

    @Test
    void buildSourceSchemaFromThreeTablesWithOneCompositePKImportedTwice() {
        Connection connection = null;
        Statement st = null;

        try {
            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String parentTableBuilding =
                "create memory table PARENT_PERSON (NAME varchar(256) not null," + " SURNAME varchar(256) not null, primary key (NAME,SURNAME))";
            st = connection.createStatement();
            st.execute(parentTableBuilding);

            String foreignTableBuilding =
                "create memory table FOREIGN_ARTICLE (TITLE  varchar(256)," +
                " AUTHOR_NAME varchar(256) not null, AUTHOR_SURNAME varchar(256) not null, TRANSLATOR_NAME varchar(256) not null," +
                " TRANSLATOR_SURNAME varchar(256) not null, primary key (TITLE)," +
                " foreign key (AUTHOR_NAME,AUTHOR_SURNAME) references PARENT_PERSON(NAME,SURNAME)," +
                " foreign key (TRANSLATOR_NAME,TRANSLATOR_SURNAME) references PARENT_PERSON(NAME,SURNAME))";
            st.execute(foreignTableBuilding);

            mapper.buildSourceDatabaseSchema();

            /*
             *  Testing context information
             */

            assertThat(statistics.totalNumberOfEntities).isEqualTo(2);
            assertThat(statistics.builtEntities).isEqualTo(2);
            assertThat(statistics.totalNumberOfRelationships).isEqualTo(2);
            assertThat(statistics.builtRelationships).isEqualTo(2);

            /*
             *  Testing built source db schema
             */

            Entity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_PERSON");
            Entity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_ARTICLE");

            // entities check
            assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(2);
            assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(2);
            assertThat(parentEntity).isNotNull();
            assertThat(foreignEntity).isNotNull();

            // attributes check
            assertThat(parentEntity.getAttributes().size()).isEqualTo(2);

            assertThat(parentEntity.getAttributeByName("NAME")).isNotNull();
            assertThat(parentEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
            assertThat(parentEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(1);
            assertThat(parentEntity.getAttributeByName("NAME").getBelongingEntity().getName()).isEqualTo("PARENT_PERSON");

            assertThat(parentEntity.getAttributeByName("SURNAME")).isNotNull();
            assertThat(parentEntity.getAttributeByName("SURNAME").getName()).isEqualTo("SURNAME");
            assertThat(parentEntity.getAttributeByName("SURNAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("SURNAME").getOrdinalPosition()).isEqualTo(2);
            assertThat(parentEntity.getAttributeByName("SURNAME").getBelongingEntity().getName()).isEqualTo("PARENT_PERSON");

            assertThat(foreignEntity.getAttributes().size()).isEqualTo(5);

            assertThat(foreignEntity.getAttributeByName("TITLE")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("TITLE").getName()).isEqualTo("TITLE");
            assertThat(foreignEntity.getAttributeByName("TITLE").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("TITLE").getOrdinalPosition()).isEqualTo(1);
            assertThat(foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName()).isEqualTo("FOREIGN_ARTICLE");

            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME").getName()).isEqualTo("AUTHOR_NAME");
            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME").getOrdinalPosition()).isEqualTo(2);
            assertThat(foreignEntity.getAttributeByName("AUTHOR_NAME").getBelongingEntity().getName()).isEqualTo("FOREIGN_ARTICLE");

            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME").getName()).isEqualTo("AUTHOR_SURNAME");
            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME").getOrdinalPosition()).isEqualTo(3);
            assertThat(foreignEntity.getAttributeByName("AUTHOR_SURNAME").getBelongingEntity().getName()).isEqualTo("FOREIGN_ARTICLE");

            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_NAME")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_NAME").getName()).isEqualTo("TRANSLATOR_NAME");
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_NAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_NAME").getOrdinalPosition()).isEqualTo(4);
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_NAME").getBelongingEntity().getName()).isEqualTo("FOREIGN_ARTICLE");

            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_SURNAME")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_SURNAME").getName()).isEqualTo("TRANSLATOR_SURNAME");
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_SURNAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_SURNAME").getOrdinalPosition()).isEqualTo(5);
            assertThat(foreignEntity.getAttributeByName("TRANSLATOR_SURNAME").getBelongingEntity().getName()).isEqualTo("FOREIGN_ARTICLE");

            // relationship, primary and foreign key check
            assertThat(foreignEntity.getOutCanonicalRelationships().size()).isEqualTo(2);
            assertThat(parentEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
            assertThat(foreignEntity.getInCanonicalRelationships().size()).isEqualTo(0);
            assertThat(parentEntity.getInCanonicalRelationships().size()).isEqualTo(2);
            assertThat(parentEntity.getForeignKeys().size()).isEqualTo(0);
            assertThat(foreignEntity.getForeignKeys().size()).isEqualTo(2);

            // first relationship
            Iterator<CanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("PARENT_PERSON");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("FOREIGN_ARTICLE");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(parentEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(foreignEntity.getForeignKeys().get(0));

            Iterator<CanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship2 = it2.next();
            assertThat(currentRelationship2).isEqualTo(currentRelationship);

            assertThat(foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName()).isEqualTo("AUTHOR_NAME");
            assertThat(foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(1).getName()).isEqualTo("AUTHOR_SURNAME");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("NAME");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(1).getName()).isEqualTo("SURNAME");

            // second relationship
            currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("PARENT_PERSON");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("FOREIGN_ARTICLE");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(parentEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(foreignEntity.getForeignKeys().get(1));
            assertThat(it.hasNext()).isFalse();

            currentRelationship2 = it2.next();
            assertThat(currentRelationship2).isEqualTo(currentRelationship);

            assertThat(foreignEntity.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName()).isEqualTo("TRANSLATOR_NAME");
            assertThat(foreignEntity.getForeignKeys().get(1).getInvolvedAttributes().get(1).getName()).isEqualTo("TRANSLATOR_SURNAME");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("NAME");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(1).getName()).isEqualTo("SURNAME");
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
     *  The primary key is imported both by the foreign table and from the first attribute of the parent table itself.
     */

    @Test
    void buildSourceSchemaFromTwoTablesWithOneSimplePKImportedTwice() {
        Connection connection = null;
        Statement st = null;

        try {
            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String parentTableBuilding =
                "create memory table PARENT_EMPLOYEE (EMP_ID varchar(256) not null," +
                " MGR_ID varchar(256) not null, NAME varchar(256) not null, primary key (EMP_ID), " +
                " foreign key (MGR_ID) references PARENT_EMPLOYEE(EMP_ID))";
            st = connection.createStatement();
            st.execute(parentTableBuilding);

            String foreignTableBuilding =
                "create memory table FOREIGN_PROJECT (PROJECT_ID  varchar(256)," +
                " TITLE varchar(256) not null, PROJECT_MANAGER varchar(256) not null, primary key (PROJECT_ID)," +
                " foreign key (PROJECT_MANAGER) references PARENT_EMPLOYEE(EMP_ID))";
            st.execute(foreignTableBuilding);

            mapper.buildSourceDatabaseSchema();

            /*
             *  Testing context information
             */

            assertThat(statistics.totalNumberOfEntities).isEqualTo(2);
            assertThat(statistics.builtEntities).isEqualTo(2);
            assertThat(statistics.totalNumberOfRelationships).isEqualTo(2);
            assertThat(statistics.builtRelationships).isEqualTo(2);

            /*
             *  Testing built source db schema
             */

            Entity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_EMPLOYEE");
            Entity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_PROJECT");

            // entities check
            assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(2);
            assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(2);
            assertThat(parentEntity).isNotNull();
            assertThat(foreignEntity).isNotNull();

            // attributes check
            assertThat(parentEntity.getAttributes().size()).isEqualTo(3);

            assertThat(parentEntity.getAttributeByName("EMP_ID")).isNotNull();
            assertThat(parentEntity.getAttributeByName("EMP_ID").getName()).isEqualTo("EMP_ID");
            assertThat(parentEntity.getAttributeByName("EMP_ID").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("EMP_ID").getOrdinalPosition()).isEqualTo(1);
            assertThat(parentEntity.getAttributeByName("EMP_ID").getBelongingEntity().getName()).isEqualTo("PARENT_EMPLOYEE");

            assertThat(parentEntity.getAttributeByName("MGR_ID")).isNotNull();
            assertThat(parentEntity.getAttributeByName("MGR_ID").getName()).isEqualTo("MGR_ID");
            assertThat(parentEntity.getAttributeByName("MGR_ID").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("MGR_ID").getOrdinalPosition()).isEqualTo(2);
            assertThat(parentEntity.getAttributeByName("MGR_ID").getBelongingEntity().getName()).isEqualTo("PARENT_EMPLOYEE");

            assertThat(parentEntity.getAttributeByName("NAME")).isNotNull();
            assertThat(parentEntity.getAttributeByName("NAME").getName()).isEqualTo("NAME");
            assertThat(parentEntity.getAttributeByName("NAME").getDataType()).isEqualTo("VARCHAR");
            assertThat(parentEntity.getAttributeByName("NAME").getOrdinalPosition()).isEqualTo(3);
            assertThat(parentEntity.getAttributeByName("NAME").getBelongingEntity().getName()).isEqualTo("PARENT_EMPLOYEE");

            assertThat(foreignEntity.getAttributes().size()).isEqualTo(3);

            assertThat(foreignEntity.getAttributeByName("PROJECT_ID")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("PROJECT_ID").getName()).isEqualTo("PROJECT_ID");
            assertThat(foreignEntity.getAttributeByName("PROJECT_ID").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("PROJECT_ID").getOrdinalPosition()).isEqualTo(1);
            assertThat(foreignEntity.getAttributeByName("PROJECT_ID").getBelongingEntity().getName()).isEqualTo("FOREIGN_PROJECT");

            assertThat(foreignEntity.getAttributeByName("TITLE")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("TITLE").getName()).isEqualTo("TITLE");
            assertThat(foreignEntity.getAttributeByName("TITLE").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("TITLE").getOrdinalPosition()).isEqualTo(2);
            assertThat(foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName()).isEqualTo("FOREIGN_PROJECT");

            assertThat(foreignEntity.getAttributeByName("PROJECT_MANAGER")).isNotNull();
            assertThat(foreignEntity.getAttributeByName("PROJECT_MANAGER").getName()).isEqualTo("PROJECT_MANAGER");
            assertThat(foreignEntity.getAttributeByName("PROJECT_MANAGER").getDataType()).isEqualTo("VARCHAR");
            assertThat(foreignEntity.getAttributeByName("PROJECT_MANAGER").getOrdinalPosition()).isEqualTo(3);
            assertThat(foreignEntity.getAttributeByName("PROJECT_MANAGER").getBelongingEntity().getName()).isEqualTo("FOREIGN_PROJECT");

            // relationship, primary and foreign key check
            assertThat(foreignEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
            assertThat(parentEntity.getOutCanonicalRelationships().size()).isEqualTo(1);
            assertThat(foreignEntity.getInCanonicalRelationships().size()).isEqualTo(0);
            assertThat(parentEntity.getInCanonicalRelationships().size()).isEqualTo(2);
            assertThat(parentEntity.getForeignKeys().size()).isEqualTo(1);
            assertThat(foreignEntity.getForeignKeys().size()).isEqualTo(1);

            // first relationship
            Iterator<CanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("PARENT_EMPLOYEE");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("FOREIGN_PROJECT");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(parentEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(foreignEntity.getForeignKeys().get(0));

            Iterator<CanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship2 = it2.next();
            assertThat(currentRelationship2).isEqualTo(currentRelationship);

            assertThat(foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName()).isEqualTo("PROJECT_MANAGER");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("EMP_ID");

            // second relationship
            it = parentEntity.getOutCanonicalRelationships().iterator();
            currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("PARENT_EMPLOYEE");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("PARENT_EMPLOYEE");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(parentEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(parentEntity.getForeignKeys().get(0));

            currentRelationship2 = it2.next();
            assertThat(currentRelationship2).isEqualTo(currentRelationship);

            assertThat(parentEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName()).isEqualTo("MGR_ID");
            assertThat(parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("EMP_ID");

            assertThat(it.hasNext()).isFalse();
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
     * Join table and 2 parent tables.
     */

    @Test
    void buildSourceSchemaFromJoinTableAnd2ParentTables() {
        Connection connection = null;
        Statement st = null;

        try {
            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String filmTableBuilding =
                "create memory table FILM (ID varchar(256) not null, TITLE varchar(256) not null," +
                " YEAR varchar(256) not null, DIRECTOR varchar(256) not null, primary key (ID))";
            st = connection.createStatement();
            st.execute(filmTableBuilding);

            String actorTableBuilding =
                "create memory table ACTOR (ID varchar(256) not null, NAME varchar(256) not null," + " SURNAME varchar(256) not null, primary key (ID))";
            st = connection.createStatement();
            st.execute(actorTableBuilding);

            String joinTableBuilding =
                "create memory table FILM2ACTOR (FILM_ID  varchar(256) not null," +
                " ACTOR_ID varchar(256) not null, SALARY varchar(256)," +
                " primary key (FILM_ID,ACTOR_ID)," +
                " foreign key (FILM_ID) references FILM(ID)," +
                " foreign key (ACTOR_ID) references ACTOR(ID))";
            st.execute(joinTableBuilding);

            connection.commit();

            mapper.buildSourceDatabaseSchema();

            /*
             *  Testing context information
             */

            assertThat(statistics.totalNumberOfEntities).isEqualTo(3);
            assertThat(statistics.builtEntities).isEqualTo(3);
            assertThat(statistics.totalNumberOfRelationships).isEqualTo(2);
            assertThat(statistics.builtRelationships).isEqualTo(2);

            /*
             *  Testing built source db schema
             */

            Entity filmEntity = mapper.getDataBaseSchema().getEntityByName("FILM");
            Entity actorEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR");
            Entity film2actor = mapper.getDataBaseSchema().getEntityByName("FILM2ACTOR");

            // entities check
            assertThat(mapper.getDataBaseSchema().getEntities().size()).isEqualTo(3);
            assertThat(mapper.getDataBaseSchema().getCanonicalRelationships().size()).isEqualTo(2);
            assertThat(filmEntity).isNotNull();
            assertThat(actorEntity).isNotNull();
            assertThat(film2actor).isNotNull();

            // attributes check
            assertThat(filmEntity.getAttributes().size()).isEqualTo(4);
            assertThat(actorEntity.getAttributes().size()).isEqualTo(3);
            assertThat(film2actor.getAttributes().size()).isEqualTo(3);

            // relationship, primary and foreign key check
            assertThat(filmEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
            assertThat(actorEntity.getOutCanonicalRelationships().size()).isEqualTo(0);
            assertThat(film2actor.getOutCanonicalRelationships().size()).isEqualTo(2);
            assertThat(filmEntity.getInCanonicalRelationships().size()).isEqualTo(1);
            assertThat(actorEntity.getInCanonicalRelationships().size()).isEqualTo(1);
            assertThat(film2actor.getInCanonicalRelationships().size()).isEqualTo(0);
            assertThat(filmEntity.getForeignKeys().size()).isEqualTo(0);
            assertThat(actorEntity.getForeignKeys().size()).isEqualTo(0);
            assertThat(film2actor.getForeignKeys().size()).isEqualTo(2);

            // first relationship
            Iterator<CanonicalRelationship> it = film2actor.getOutCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("ACTOR");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("FILM2ACTOR");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(actorEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(film2actor.getForeignKeys().get(0));

            Iterator<CanonicalRelationship> it2 = actorEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship2 = it2.next();
            assertThat(currentRelationship2).isEqualTo(currentRelationship);

            assertThat(film2actor.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName()).isEqualTo("ACTOR_ID");
            assertThat(actorEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("ID");

            // second relationship
            currentRelationship = it.next();
            assertThat(currentRelationship.getParentEntity().getName()).isEqualTo("FILM");
            assertThat(currentRelationship.getForeignEntity().getName()).isEqualTo("FILM2ACTOR");
            assertThat(currentRelationship.getPrimaryKey()).isEqualTo(filmEntity.getPrimaryKey());
            assertThat(currentRelationship.getForeignKey()).isEqualTo(film2actor.getForeignKeys().get(1));

            Iterator<CanonicalRelationship> it3 = filmEntity.getInCanonicalRelationships().iterator();
            CanonicalRelationship currentRelationship3 = it3.next();
            assertThat(currentRelationship3).isEqualTo(currentRelationship);

            assertThat(film2actor.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName()).isEqualTo("FILM_ID");
            assertThat(filmEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName()).isEqualTo("ID");

            assertThat(it.hasNext()).isFalse();
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
