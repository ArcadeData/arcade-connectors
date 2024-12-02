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

import com.arcadeanalytics.provider.rdbms.nameresolver.JavaConventionNameResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Gabriele Ponzi
 */
class JavaNameResolverTest {

  private JavaConventionNameResolver nameResolver;

  @BeforeEach
  void init() {
    this.nameResolver = new JavaConventionNameResolver();
  }

  /*
   * Resolve Vertex Class Name (Java Class Convention)
   */

  @Test
  void resolveVertexClassNameWithJavaConvention() {
    String candidateName = "";
    String newCandidateName = "";

    // No white space nor underscore

    candidateName =
        "testClass"; // NOT acceptable (one or more uppercase char, except the first one)
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName = "Testclass"; // acceptable (one or more uppercase char, the first one included)
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isTrue();

    candidateName = "TestClass"; // acceptable (one or more uppercase char, except the first one)
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isTrue();

    candidateName = "testclass"; // NOT acceptable (no uppercase chars)
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("Testclass");

    candidateName = "TESTCLASS"; //  NOT acceptable (no lowercase chars)
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("Testclass");

    // White space

    candidateName = "test Class"; //  NOT acceptable
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName = "Test class"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName = "Test Class"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName = "test class"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName = "TEST CLASS"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    // Underscore

    candidateName = "test_Class"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName = "Test_class"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName = "Test_Class"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName = "test_class"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName = "TEST_CLASS"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");
  }

  /*
   * Resolve Vertex Class Name (Java Class Convention)
   * Test-Fix: Resolver supports names ending with '_'.
   */

  @Test
  void resolveVertexClassNameWithJavaConventionNamesEndingWithUnderscore() {
    String candidateName = "";
    String newCandidateName = "";

    // No white space nor underscore

    candidateName =
        "testClass_"; // NOT acceptable (one or more uppercase char, except the first one)
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.toJavaClassConvention(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");

    candidateName =
        "test_class_"; // NOT acceptable (one or more uppercase char, except the first one)
    assertThat(nameResolver.isCompliantToJavaClassConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.toJavaClassConvention(candidateName);
    assertThat(nameResolver.isCompliantToJavaClassConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("TestClass");
  }

  /*
   * Resolve Vertex Property (Java Variable Convention)
   */

  @Test
  void resolveVertexPropertyNameWithJavaConvention() {
    String candidateName = "";
    String newCandidateName = "";

    // No white space nor underscore

    candidateName = "testVariable"; // acceptable (one or more uppercase char, except the first one)
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isTrue();

    candidateName =
        "Testvariable"; // NOT acceptable (one or more uppercase char, the first one included)
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testvariable");

    candidateName =
        "TestVariable"; // NOT acceptable (one or more uppercase char, except the first one)
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    candidateName = "testvariable"; // acceptable (no uppercase chars)
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isTrue();

    candidateName = "TESTVARIABLE"; // NOT acceptable (no lowercase chars)
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testvariable");

    // White space

    candidateName = "test Variable"; //  NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    candidateName = "Test variable"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    candidateName = "Test Variable"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    candidateName = "test variable"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    candidateName = "TEST VARIABLE"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    // Underscore

    candidateName = "test_Variable"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    candidateName = "Test_variable"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    candidateName = "Test_Variable"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    candidateName = "test_variable"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");

    candidateName = "TEST_VARIABLE"; // NOT acceptable
    assertThat(nameResolver.isCompliantToJavaVariableConvention(candidateName)).isFalse();
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertThat(nameResolver.isCompliantToJavaVariableConvention(newCandidateName)).isTrue();
    assertThat(newCandidateName).isEqualTo("testVariable");
  }
}
