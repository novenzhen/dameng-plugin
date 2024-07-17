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

import dm.jdbc.driver.DmdbStatement;
import dm.jdbc.driver.DmdbStatement_bs;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.OfficialComponent;
import org.apache.skywalking.apm.plugin.jdbc.SqlBodyUtil;
import org.apache.skywalking.apm.plugin.jdbc.define.StatementEnhanceInfos;
import org.apache.skywalking.apm.plugin.jdbc.trace.ConnectionInfo;

import java.lang.reflect.Method;

public class StatementExecuteMethodsInterceptor implements InstanceMethodsAroundInterceptor {
    @Override
    public final void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                                   Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {
        StatementEnhanceInfos cacheObject = (StatementEnhanceInfos) objInst.getSkyWalkingDynamicField();
        ConnectionInfo connectInfo = null ;
        if (cacheObject != null && cacheObject.getConnectionInfo() != null) {
            connectInfo = cacheObject.getConnectionInfo() ;
        } else {
            if (objInst instanceof DmdbStatement) {
                DmdbStatement dmdbStatement = (DmdbStatement) objInst ;
                connectInfo = new ConnectionInfo(new OfficialComponent(Constants.COMPONENT_ID, Constants.DB_TYPE), Constants.DB_TYPE, dmdbStatement.do_getConnection().getHostName(), Integer.parseInt(dmdbStatement.do_getConnection().getHostPort()), dmdbStatement.do_getConnection().do_getSchema()) ;
            } else if (objInst instanceof DmdbStatement_bs) {
                DmdbStatement_bs dmdbStatement = (DmdbStatement_bs) objInst ;
                connectInfo = new ConnectionInfo(new OfficialComponent(Constants.COMPONENT_ID, Constants.DB_TYPE), Constants.DB_TYPE, dmdbStatement.getConnection_bs().getHost(), dmdbStatement.getConnection_bs().getPort(), dmdbStatement.getConnection_bs().getCurrentDBName()) ;
            }
            cacheObject = new StatementEnhanceInfos(connectInfo, cacheObject == null ? (String) allArguments[0] : cacheObject.getSql(), "Statement");
            objInst.setSkyWalkingDynamicField(cacheObject);
        }
            if (connectInfo != null) {

                AbstractSpan span = ContextManager.createExitSpan(buildOperationName(connectInfo, method.getName(), cacheObject
                        .getStatementName()), connectInfo.getDatabasePeer());
                Tags.DB_TYPE.set(span, connectInfo.getDBType());
                Tags.DB_INSTANCE.set(span, connectInfo.getDatabaseName());

                String sql = "";
                if (allArguments.length > 0) {
                    sql = (String) allArguments[0];
                    sql = SqlBodyUtil.limitSqlBodySize(sql);
                }
                Tags.DB_STATEMENT.set(span, sql);
                span.setComponent(connectInfo.getComponent());

                SpanLayer.asDB(span);
            }
    }

    @Override
    public final Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                                    Class<?>[] argumentsTypes, Object ret) throws Throwable {
        StatementEnhanceInfos cacheObject = (StatementEnhanceInfos) objInst.getSkyWalkingDynamicField();
        if (cacheObject.getConnectionInfo() != null) {
            ContextManager.stopSpan();
        }
        return ret;
    }

    @Override
    public final void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
                                            Class<?>[] argumentsTypes, Throwable t) {
        StatementEnhanceInfos cacheObject = (StatementEnhanceInfos) objInst.getSkyWalkingDynamicField();
        if (cacheObject.getConnectionInfo() != null) {
            ContextManager.activeSpan().log(t);
        }
    }

    private String buildOperationName(ConnectionInfo connectionInfo, String methodName, String statementName) {
        return connectionInfo.getDBType() + "/JDBI/" + statementName + "/" + methodName;
    }
}
