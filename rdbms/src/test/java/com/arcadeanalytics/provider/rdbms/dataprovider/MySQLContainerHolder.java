package com.arcadeanalytics.provider.rdbms.dataprovider;

import org.testcontainers.containers.MySQLContainer;

public abstract class MySQLContainerHolder {

    public static final MySQLContainer container;

    static {
        container = new MySQLContainer("arcadeanalytics/mysql-sakila")
                .withUsername("test")
                .withPassword("test")
                .withDatabaseName("sakila");

        container.start();

        container.withDatabaseName("sakila");


    }
}
