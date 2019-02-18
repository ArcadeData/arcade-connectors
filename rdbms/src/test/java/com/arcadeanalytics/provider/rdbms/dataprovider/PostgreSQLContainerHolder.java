package com.arcadeanalytics.provider.rdbms.dataprovider;

import org.testcontainers.containers.PostgreSQLContainer;

public abstract class PostgreSQLContainerHolder {

    public static final PostgreSQLContainer container;

    static {
        container = new PostgreSQLContainer("arcadeanalytics/postgres-dvdrental")
                .withUsername("postgres")
                .withPassword("postgres");
        container.start();

        container.withDatabaseName("dvdrental");


    }
}
