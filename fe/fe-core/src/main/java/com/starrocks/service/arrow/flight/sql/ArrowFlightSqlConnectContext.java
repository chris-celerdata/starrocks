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

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.ExecuteEnv;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

// one connection will create one ArrowFlightSqlConnectContext
// This context can handle MULTIPLE concurrent queries
public class ArrowFlightSqlConnectContext extends ConnectContext {
    private final BufferAllocator allocator;

    private final String arrowFlightSqlToken;

    // Store multiple query contexts - one per concurrent query
    private final ConcurrentHashMap<UUID, ArrowFlightSqlQueryConnectContext> queryContexts = new ConcurrentHashMap<>();

    public ArrowFlightSqlConnectContext(String arrowFlightSqlToken) {
        super();
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.arrowFlightSqlToken = arrowFlightSqlToken;
    }

    /**
     * Create a new query context for a query
     */
    public ArrowFlightSqlQueryConnectContext createQueryContext(String query) {
        UUID newQueryId = UUIDUtil.genUUID();
        ArrowFlightSqlQueryConnectContext queryContext = new ArrowFlightSqlQueryConnectContext(this, newQueryId, query);
        queryContexts.put(newQueryId, queryContext);
        return queryContext;
    }

    /**
     * Get an existing query context
     */
    public ArrowFlightSqlQueryConnectContext getQueryContext(String queryId) {
        return queryContexts.get(UUID.fromString(queryId));
    }
    public ArrowFlightSqlQueryConnectContext getQueryContext(UUID queryId) {
        return queryContexts.get(queryId);
    }

    /**
     * Remove and cleanup a query context
     */
    public void removeQueryContext(UUID queryId) {
        ArrowFlightSqlQueryConnectContext queryContext = queryContexts.remove(queryId);
        if (queryContext != null) {
            queryContext.cleanup();
        }
    }
    public void removeQueryContext(String queryId) {
        this.removeQueryContext(UUID.fromString(queryId));
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    public String getArrowFlightSqlToken() {
        return arrowFlightSqlToken;
    }

    public void removeResult(String queryId) {
        this.removeQueryContext(queryId);
    }

    /**
     * Cancel all queries and close the connection
     */
    @Override
    public void kill(boolean isKillConnection, String cancelledMessage) {
        // Cancel all active queries
        for (ArrowFlightSqlQueryConnectContext queryContext : queryContexts.values()) {
            queryContext.cancelQuery(cancelledMessage);
        }

        if (isKillConnection) {
            isKilled = true;
            
            // Cleanup all query contexts
            for (UUID queryId : new ArrayList<>(queryContexts.keySet())) {
                removeQueryContext(queryId);
            }
            
            this.cleanup();
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(this);
        }

        if (allocator != null) {
            allocator.close();
        }
    }
}