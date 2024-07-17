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

package org.apache.skywalking.apm.plugin.jdbc.dameng.v8;

import dm.jdbc.driver.DmdbConnection;
import dm.jdbc.driver.DmdbConnection_bs;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceConstructorInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.OfficialComponent;
import org.apache.skywalking.apm.plugin.jdbc.define.StatementEnhanceInfos;
import org.apache.skywalking.apm.plugin.jdbc.trace.ConnectionInfo;

import java.lang.reflect.Method;

public class CreatePreparedStatementInterceptor implements InstanceConstructorInterceptor, InstanceMethodsAroundInterceptor {

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {

    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Object ret) throws Throwable {
        if (ret instanceof EnhancedInstance) {
            if (objInst != null) {
                ConnectionInfo connectionInfo = (ConnectionInfo) objInst.getSkyWalkingDynamicField();
                if (connectionInfo == null) {
                    if (objInst instanceof DmdbConnection) {
                        DmdbConnection connection = (DmdbConnection) objInst ;
                        connectionInfo = new ConnectionInfo(new OfficialComponent(Constants.COMPONENT_ID, Constants.DB_TYPE), Constants.DB_TYPE, connection.getHostName(), Integer.parseInt(connection.getHostPort()), connection.do_getSchema()) ;
                    } else if (objInst instanceof DmdbConnection_bs) {
                        DmdbConnection_bs connection = (DmdbConnection_bs) objInst ;
                        connectionInfo = new ConnectionInfo(new OfficialComponent(Constants.COMPONENT_ID, Constants.DB_TYPE), Constants.DB_TYPE,  connection.getHost(), connection.getPort(), connection.getCurrentDBName()) ;
                    }
                }
                ((EnhancedInstance) ret).setSkyWalkingDynamicField(new StatementEnhanceInfos(connectionInfo, (String) allArguments[0], "PreparedStatement"));
            }
        }
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Throwable t) {

    }

    @Override
    public void onConstruct(EnhancedInstance objInst, Object[] allArguments) throws Throwable {

    }
}
