// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.catalog.Column;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.sql.ast.StatementBase;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ArrowFlightSqlQueryConnectContext extends ConnectContext {
    private final ArrowFlightSqlConnectContext parentConnection;

    private StatementBase statement; 

    private String query;

    private CompletableFuture<Coordinator> coordinatorFuture;

    private boolean returnResultFromFE; 

    private ArrowSchemaRootWrapper resultWrapper; 

    public ArrowFlightSqlQueryConnectContext(ArrowFlightSqlConnectContext parent, UUID queryId, String query) {
        super(); 

        this.parentConnection = parent; 
        this.query = query; 
        this.statement = null;
        this.coordinatorFuture = new CompletableFuture<>(); 
        this.returnResultFromFE = true; 

        this.setSessionVariable(this.parentConnection.getSessionVariable());
        this.setGlobalStateMgr(parent.getGlobalStateMgr());
        this.setDatabase(this.parentConnection.getDatabase());
        this.setRemoteIP(this.parentConnection.getRemoteIP());

        // authentication
        this.setQualifiedUser(this.parentConnection.getQualifiedUser());
        this.setCurrentUserIdentity(this.parentConnection.getCurrentUserIdentity());
        this.setDistinguishedName(parent.getDistinguishedName());
        this.setSecurityIntegration(parent.getSecurityIntegration());
        this.setGroups(parent.getGroups());
        this.setCurrentRoleIds(parent.getCurrentRoleIds());
        this.setAuthPlugin(parent.getAuthPlugin());
        this.setAuthToken(parent.getAuthToken());

        this.setQueryId(queryId); 
        this.setExecutionId(UUIDUtil.toTUniqueId(this.getQueryId()));
    }

    public ArrowFlightSqlConnectContext getParentConnection() {
        return parentConnection; 
    }
    
    public String getFlightQueryId() {
        return DebugUtil.printId(queryId); 
    }

    public StatementBase getStatement() {
        return statement;
    }
    
    public void setStatement(StatementBase statement) {
        this.statement = statement;
    }
    
    public Coordinator waitForDeploymentFinished(long timeoutMs)
            throws ExecutionException, InterruptedException, TimeoutException, CancellationException {
        return coordinatorFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void setDeploymentFinished(Coordinator coordinator) {
        this.coordinatorFuture.complete(coordinator);
    }

    public void setDeployFailed(Throwable e) {
        this.coordinatorFuture.completeExceptionally(e);
    }

    public void setResult(ArrowSchemaRootWrapper wrapper) {
        this.resultWrapper = wrapper;
    }

    public VectorSchemaRoot getResult() {
        return resultWrapper != null ? resultWrapper.getSchemaRoot() : null;
    }

    public String getQuery() {
        return query;
    }

    public boolean returnFromFE() {
        return returnResultFromFE;
    }

    public void setReturnResultFromFE(boolean returnResultFromFE) {
        this.returnResultFromFE = returnResultFromFE;
    }
    
    public boolean isFromFECoordinator() {
        Coordinator coordinator = coordinatorFuture.getNow(null);
        return !(coordinator instanceof DefaultCoordinator);
    }
   
    public void cancelQuery() {
        if (executor != null) {
            executor.cancel("Arrow Flight SQL client disconnected"); 
        }
    }

    public void cancelQuery(String reason) {
        if (executor != null) {
            executor.cancel(reason);
        }
        
        if (coordinatorFuture != null && coordinatorFuture.isDone()) {
            try {
                Coordinator coordinator = coordinatorFuture.getNow(null);
                if (coordinator != null) {
                    coordinator.cancel(reason);
                }
            } catch (Exception e) {
                // Do nothing.
            }
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (resultWrapper != null) {
            resultWrapper.close();
            resultWrapper = null; 
        }
    }

    public void addShowResult(ShowResultSet showResultSet) {
        List<Field> schemaFields = new ArrayList<>();
        List<FieldVector> dataFields = new ArrayList<>();
        List<List<String>> resultData = showResultSet.getResultRows();
        ShowResultSetMetaData metaData = showResultSet.getMetaData();
        
        BufferAllocator allocator = parentConnection.getAllocator();

        for (Column col : metaData.getColumns()) {
            schemaFields.add(new Field(col.getName(), FieldType.nullable(new Utf8()), null));
            VarCharVector varCharVector = ArrowUtil.createVarCharVector(col.getName(), allocator, resultData.size());
            dataFields.add(varCharVector);
        }

        for (int i = 0; i < resultData.size(); i++) {
            List<String> row = resultData.get(i);
            for (int j = 0; j < row.size(); j++) {
                String item = row.get(j);
                if (item == null || item.equals(FeConstants.NULL_STRING)) {
                    dataFields.get(j).setNull(i);
                } else {
                    ((VarCharVector) dataFields.get(j)).setSafe(i, item.getBytes());
                }
            }
        }

        VectorSchemaRoot root = new VectorSchemaRoot(schemaFields, dataFields);
        root.setRowCount(resultData.size());
        this.resultWrapper = new ArrowSchemaRootWrapper(root);
    }

    public void setEmptyResultIfNotExist() {
        if (this.resultWrapper == null) {
            VectorSchemaRoot schemaRoot = ArrowUtil.createSingleSchemaRoot("StatusResult", "0");
            this.resultWrapper = new ArrowSchemaRootWrapper(schemaRoot);
        }
    }

    public String getArrowFlightSqlToken() {
        return parentConnection.getArrowFlightSqlToken();
    }
}
