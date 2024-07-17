/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.plugin.jdbc.dameng.v8.define;

import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.ConstructorInterceptPoint;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.InstanceMethodsInterceptPoint;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.ClassInstanceMethodsEnhancePluginDefine;
import org.apache.skywalking.apm.agent.core.plugin.match.ClassMatch;
import org.apache.skywalking.apm.plugin.jdbc.define.Constants;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.skywalking.apm.agent.core.plugin.match.MultiClassNameMatch.byMultiClassMatch;
import static org.apache.skywalking.apm.plugin.jdbc.dameng.v8.Constants.CREATE_PREPARED_STATEMENT_INTERCEPTOR;
import static org.apache.skywalking.apm.plugin.jdbc.dameng.v8.Constants.CREATE_CALLABLE_STATEMENT_INTERCEPTOR;
import static org.apache.skywalking.apm.plugin.jdbc.dameng.v8.Constants.CREATE_STATEMENT_INTERCEPTOR;
import static org.apache.skywalking.apm.plugin.jdbc.dameng.v8.Constants.SET_CATALOG_INTERCEPTOR;

public class ConnectionInstrumentation extends ClassInstanceMethodsEnhancePluginDefine {

    @Override
    protected ClassMatch enhanceClass() {
        return byMultiClassMatch("dm.jdbc.driver.DmdbConnection", "dm.jdbc.driver.DmdbConnection_bs");
    }

    @Override
    public ConstructorInterceptPoint[] getConstructorsInterceptPoints() {
        return new ConstructorInterceptPoint[0];
    }

    @Override
    public InstanceMethodsInterceptPoint[] getInstanceMethodsInterceptPoints() {
        return new InstanceMethodsInterceptPoint[]{
                new InstanceMethodsInterceptPoint() {
                    @Override
                    public ElementMatcher<MethodDescription> getMethodsMatcher() {
                        return named(Constants.PREPARE_STATEMENT_METHOD_NAME);
                    }

                    @Override
                    public String getMethodsInterceptor() {
                        return CREATE_PREPARED_STATEMENT_INTERCEPTOR;
                    }

                    @Override
                    public boolean isOverrideArgs() {
                        return false;
                    }
                },
                //prepareCallMethodName
                new InstanceMethodsInterceptPoint() {
                    @Override
                    public ElementMatcher<MethodDescription> getMethodsMatcher() {
                        return named(Constants.PREPARE_CALL_METHOD_NAME);
                    }

                    @Override
                    public String getMethodsInterceptor() {
                        return CREATE_CALLABLE_STATEMENT_INTERCEPTOR;
                    }

                    @Override
                    public boolean isOverrideArgs() {
                        return false;
                    }
                },

                new InstanceMethodsInterceptPoint() {
                    @Override
                    public ElementMatcher<MethodDescription> getMethodsMatcher() {
                        return named(Constants.CREATE_STATEMENT_METHOD_NAME).and(takesArguments(2));
                    }

                    @Override
                    public String getMethodsInterceptor() {
                        return CREATE_STATEMENT_INTERCEPTOR;
                    }

                    @Override
                    public boolean isOverrideArgs() {
                        return false;
                    }
                },

                new InstanceMethodsInterceptPoint() {
                    @Override
                    public ElementMatcher<MethodDescription> getMethodsMatcher() {
                        return named(Constants.COMMIT_METHOD_NAME).or(named(Constants.ROLLBACK_METHOD_NAME))
                                .or(named(Constants.CLOSE_METHOD_NAME))
                                .or(named(Constants.RELEASE_SAVE_POINT_METHOD_NAME));
                    }

                    @Override
                    public String getMethodsInterceptor() {
                        return Constants.SERVICE_METHOD_INTERCEPT_CLASS;
                    }

                    @Override
                    public boolean isOverrideArgs() {
                        return false;
                    }
                },

                new InstanceMethodsInterceptPoint() {
                    @Override
                    public ElementMatcher<MethodDescription> getMethodsMatcher() {
                        return named("setCatalog");
                    }

                    @Override
                    public String getMethodsInterceptor() {
                        return SET_CATALOG_INTERCEPTOR;
                    }

                    @Override
                    public boolean isOverrideArgs() {
                        return false;
                    }
                }
        };
    }
}