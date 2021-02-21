package com.arcadeanalytics.provider.rdbms.factory;

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

import com.arcadeanalytics.provider.rdbms.nameresolver.JavaConventionNameResolver;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.nameresolver.OriginalConventionNameResolver;

/**
 * Factory used to instantiate a specific NameResolver starting from its name.
 * If the name is not specified (null value) a JavaConventionNameResolver is instantiated.
 *
 * @author Gabriele Ponzi
 */

public class NameResolverFactory {

    public NameResolver buildNameResolver(String nameResolverConvention) {
        if (nameResolverConvention == null) {
            return new OriginalConventionNameResolver();
        } else {
            switch (nameResolverConvention) {
                case "java":
                    return new JavaConventionNameResolver();
                case "original":
                    return new OriginalConventionNameResolver();
                default:
                    return new OriginalConventionNameResolver();
            }
        }
    }
}
