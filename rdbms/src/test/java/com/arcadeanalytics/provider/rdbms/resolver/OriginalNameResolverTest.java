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

package com.arcadeanalytics.provider.rdbms.resolver;

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

import com.arcadeanalytics.provider.rdbms.nameresolver.OriginalConventionNameResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Gabriele Ponzi
 */

class OriginalNameResolverTest {

    private OriginalConventionNameResolver nameResolver;

    @BeforeEach
    void init() {
        this.nameResolver = new OriginalConventionNameResolver();
    }

    /*
     * Resolve Vertex Class Name (Original Class Convention)
     */

    @Test
    void resolveVertexClassNameWithOriginalNameConvention() {
        String candidateName = "";
        String newCandidateName = "";

        // No white space nor underscore

        candidateName = "testClass"; // NOT acceptable (one or more uppercase char, except the first one)
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("testClass");

        candidateName = "Testclass"; // acceptable (one or more uppercase char, the first one included)
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("Testclass");

        candidateName = "TestClass"; // acceptable (one or more uppercase char, except the first one)
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("TestClass");

        candidateName = "testclass"; // NOT acceptable (no uppercase chars)
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("testclass");

        candidateName = "TESTCLASS"; //  NOT acceptable (no lowercase chars)
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("TESTCLASS");

        // White space

        candidateName = "test Class"; //  NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("test_Class");

        candidateName = "Test class"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("Test_class");

        candidateName = "Test Class"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("Test_Class");

        candidateName = "test class"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("test_class");

        candidateName = "TEST CLASS"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("TEST_CLASS");

        // Underscore

        candidateName = "test_Class"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("test_Class");

        candidateName = "Test_class"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("Test_class");

        candidateName = "Test_Class"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("Test_Class");

        candidateName = "test_class"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("test_class");

        candidateName = "TEST_CLASS"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexName(candidateName);
        assertThat(newCandidateName).isEqualTo("TEST_CLASS");
    }

    /*
     * Resolve Vertex Property (Original Variable Convention)
     */

    @Test
    void resolveVertexPropertyNameWithOriginalNameConvention() {
        String candidateName = "";
        String newCandidateName = "";

        // No white space nor underscore

        candidateName = "testVariable"; // acceptable (one or more uppercase char, except the first one)
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("testVariable");

        candidateName = "Testvariable"; // NOT acceptable (one or more uppercase char, the first one included)
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("Testvariable");

        candidateName = "TestVariable"; // NOT acceptable (one or more uppercase char, except the first one)
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("TestVariable");

        candidateName = "testvariable"; // acceptable (no uppercase chars)
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("testvariable");

        candidateName = "TESTVARIABLE"; // NOT acceptable (no lowercase chars)
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("TESTVARIABLE");

        // White space

        candidateName = "test Variable"; //  NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("test_Variable");

        candidateName = "Test variable"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("Test_variable");

        candidateName = "Test Variable"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("Test_Variable");

        candidateName = "test variable"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("test_variable");

        candidateName = "TEST VARIABLE"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("TEST_VARIABLE");

        // Underscore

        candidateName = "test_Variable"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("test_Variable");

        candidateName = "Test_variable"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("Test_variable");

        candidateName = "Test_Variable"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("Test_Variable");

        candidateName = "test_variable"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("test_variable");

        candidateName = "TEST_VARIABLE"; // NOT acceptable
        newCandidateName = nameResolver.resolveVertexProperty(candidateName);
        assertThat(newCandidateName).isEqualTo("TEST_VARIABLE");
    }
}
