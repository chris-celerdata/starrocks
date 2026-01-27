// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ArrowFlightSqlSessionManagerTest {

    private ArrowFlightSqlSessionManager sessionManager;
    private final String mockToken = "mock-token-123";
    private final UUID mockUUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    private final TUniqueId mockTUniqueId = new TUniqueId(1L, 2L);
    private ConnectScheduler mockScheduler;
    private ArrowFlightSqlConnectContext mockContext;

    @BeforeEach
    public void setUp() {
        sessionManager = new ArrowFlightSqlSessionManager();
        mockScheduler = mock(ConnectScheduler.class);
        mockContext = mock(ArrowFlightSqlConnectContext.class);
    }

    private void mockGlobalStateMgr(MockedStatic<GlobalStateMgr> mockedGlobalState) {
        GlobalStateMgr mockGlobalState = mock(GlobalStateMgr.class);
        VariableMgr mockVariableMgr = mock(VariableMgr.class);
        SessionVariable mockSessionVariable = mock(SessionVariable.class);
        com.starrocks.authorization.AuthorizationMgr mockAuthMgr = mock(com.starrocks.authorization.AuthorizationMgr.class);

        mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
        when(mockGlobalState.getVariableMgr()).thenReturn(mockVariableMgr);
        when(mockVariableMgr.newSessionVariable()).thenReturn(mockSessionVariable);
        when(mockGlobalState.getAuthorizationMgr()).thenReturn(mockAuthMgr);
        try {
            when(mockAuthMgr.getDefaultRoleIdsByUser(any())).thenReturn(Set.of(1L, 2L, 3L));
        } catch (PrivilegeException e) {
            throw new RuntimeException(e);
        }
    }

    private void mockAuthentication(MockedStatic<AuthenticationHandler> mockedAuth) {
        mockedAuth.when(() -> AuthenticationHandler.authenticate(any(), any(), any(), any()))
                .thenAnswer(invocation -> {
                    ConnectContext ctx = invocation.getArgument(0);
                    ctx.setCurrentUserIdentity(null);
                    ctx.setQualifiedUser("testUser");
                    return null;
                });
    }

    @Test
    public void testInitializeSession_success() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            assertNotNull(token);
        }
    }

    @Test
    public void testInitializeSession_registerConnectionFail() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(false, "register failed"));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            assertThrows(FlightRuntimeException.class, () -> sessionManager
                    .initializeSession("testUser", "127.0.0.1", "testPassword"));
        }
    }

    @Test
    public void testValidateToken_success() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            assertDoesNotThrow(() -> sessionManager.validateToken(token));
        }
    }

    @Test
    public void testValidateToken_emptyOrNull() {
        assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(null));
        assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(""));
    }

    @Test
    public void testValidateToken_invalidToken() {
        assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken("non-exist-token"));
    }

    @Test
    public void testCloseSession() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
                MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
                MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
                MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            sessionManager.validateToken(token);

            sessionManager.closeSession(token);
            assertThrows(IllegalArgumentException.class, () -> sessionManager.validateToken(token));
        }
    }

    @Test
    public void testValidateAndGetConnectContext_success() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class)) {
            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getArrowFlightSqlConnectContext(mockToken)).thenReturn(mockContext);

            ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(mockToken);
            assertNotNull(ctx);
        }
    }

    @Test
    public void testValidateAndGetConnectContext_notFound() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class)) {
            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getArrowFlightSqlConnectContext(mockToken)).thenReturn(null);

            assertThrows(FlightRuntimeException.class, () -> sessionManager.validateAndGetConnectContext(mockToken));
        }
    }

    // ==================== Token Format Tests for Multi-FE Proxy ====================

    @Test
    public void testExtractFeHost_validTokenWithHost() {
        // Token format: "FE_HOST:UUID"
        String token = "fe1.example.com:123e4567-e89b-12d3-a456-426614174000";
        String feHost = ArrowFlightSqlSessionManager.extractFeHost(token);
        assertEquals("fe1.example.com", feHost);
    }

    @Test
    public void testExtractFeHost_tokenWithIpAddress() {
        String token = "10.0.1.107:123e4567-e89b-12d3-a456-426614174000";
        String feHost = ArrowFlightSqlSessionManager.extractFeHost(token);
        assertEquals("10.0.1.107", feHost);
    }

    @Test
    public void testExtractFeHost_legacyTokenWithoutHost() {
        // Legacy token format (no host prefix): "UUID" - contains colons but not as host separator
        // UUID format: 123e4567-e89b-12d3-a456-426614174000 (no colons)
        String token = "123e4567-e89b-12d3-a456-426614174000";
        String feHost = ArrowFlightSqlSessionManager.extractFeHost(token);
        // UUID without colon returns null (indexOf(':') returns -1)
        assertNull(feHost);
    }

    @Test
    public void testExtractFeHost_emptyToken() {
        assertNull(ArrowFlightSqlSessionManager.extractFeHost(""));
        assertNull(ArrowFlightSqlSessionManager.extractFeHost(null));
    }

    @Test
    public void testExtractFeHost_tokenWithoutColon() {
        // Token without colon should return null (no host prefix)
        String token = "simpletoken";
        String feHost = ArrowFlightSqlSessionManager.extractFeHost(token);
        assertNull(feHost);
    }

    @Test
    public void testIsLocalToken_proxyDisabled() {
        // When proxy is disabled, all tokens are considered local
        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class)) {
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);

            assertTrue(sessionManager.isLocalToken("any-token"));
            assertTrue(sessionManager.isLocalToken("fe1.example.com:some-uuid"));
        }
    }

    @Test
    public void testIsLocalToken_proxyEnabled_localToken() {
        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
             MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class)) {

            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            GlobalStateMgr mockGlobalState = mock(GlobalStateMgr.class);
            NodeMgr mockNodeMgr = mock(NodeMgr.class);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
            when(mockGlobalState.getNodeMgr()).thenReturn(mockNodeMgr);
            when(mockNodeMgr.getSelfNode()).thenReturn(Pair.create("10.0.1.107", 9010));

            // Token from this FE
            String localToken = "10.0.1.107:123e4567-e89b-12d3-a456-426614174000";
            assertTrue(sessionManager.isLocalToken(localToken));
        }
    }

    @Test
    public void testIsLocalToken_proxyEnabled_remoteToken() {
        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
             MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class)) {

            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            GlobalStateMgr mockGlobalState = mock(GlobalStateMgr.class);
            NodeMgr mockNodeMgr = mock(NodeMgr.class);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
            when(mockGlobalState.getNodeMgr()).thenReturn(mockNodeMgr);
            when(mockNodeMgr.getSelfNode()).thenReturn(Pair.create("10.0.1.107", 9010));

            // Token from different FE
            String remoteToken = "10.0.6.7:123e4567-e89b-12d3-a456-426614174000";
            assertFalse(sessionManager.isLocalToken(remoteToken));
        }
    }

    @Test
    public void testIsLocalToken_proxyEnabled_legacyToken() {
        try (MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
             MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class)) {

            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            GlobalStateMgr mockGlobalState = mock(GlobalStateMgr.class);
            NodeMgr mockNodeMgr = mock(NodeMgr.class);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
            when(mockGlobalState.getNodeMgr()).thenReturn(mockNodeMgr);
            when(mockNodeMgr.getSelfNode()).thenReturn(Pair.create("10.0.1.107", 9010));

            // Legacy token without host prefix - should be treated as local
            String legacyToken = "simpletoken";
            assertTrue(sessionManager.isLocalToken(legacyToken));
        }
    }

    @Test
    public void testInitializeSession_proxyEnabled_tokenIncludesFeHost() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
             MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
             MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
             MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
             MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            GlobalStateMgr mockGlobalState = mock(GlobalStateMgr.class);
            NodeMgr mockNodeMgr = mock(NodeMgr.class);
            VariableMgr mockVariableMgr = mock(VariableMgr.class);
            SessionVariable mockSessionVariable = mock(SessionVariable.class);
            com.starrocks.authorization.AuthorizationMgr mockAuthMgr =
                    mock(com.starrocks.authorization.AuthorizationMgr.class);

            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(mockGlobalState);
            when(mockGlobalState.getNodeMgr()).thenReturn(mockNodeMgr);
            when(mockNodeMgr.getSelfNode()).thenReturn(Pair.create("10.0.1.107", 9010));
            when(mockGlobalState.getVariableMgr()).thenReturn(mockVariableMgr);
            when(mockVariableMgr.newSessionVariable()).thenReturn(mockSessionVariable);
            when(mockGlobalState.getAuthorizationMgr()).thenReturn(mockAuthMgr);
            try {
                when(mockAuthMgr.getDefaultRoleIdsByUser(any())).thenReturn(Set.of(1L, 2L, 3L));
            } catch (PrivilegeException e) {
                throw new RuntimeException(e);
            }

            String token = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            assertNotNull(token);
            // Token should start with FE host
            assertTrue(token.startsWith("10.0.1.107:"), "Token should include FE host prefix");
            // Token should contain UUID
            assertTrue(token.contains(mockUUID.toString()), "Token should contain UUID");
        }
    }

    @Test
    public void testInitializeSession_proxyDisabled_tokenIsPlainUuid() {
        try (MockedStatic<ExecuteEnv> mockedEnv = mockStatic(ExecuteEnv.class);
             MockedStatic<UUIDUtil> mockedUUID = mockStatic(UUIDUtil.class);
             MockedStatic<GlobalStateMgr> mockedGlobalState = mockStatic(GlobalStateMgr.class);
             MockedStatic<GlobalVariable> mockedGlobalVar = mockStatic(GlobalVariable.class);
             MockedStatic<AuthenticationHandler> mockedAuth = mockStatic(AuthenticationHandler.class)) {

            mockAuthentication(mockedAuth);
            mockedGlobalVar.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);

            ExecuteEnv mockEnv = mock(ExecuteEnv.class);
            mockedEnv.when(ExecuteEnv::getInstance).thenReturn(mockEnv);
            when(mockEnv.getScheduler()).thenReturn(mockScheduler);
            when(mockScheduler.getNextConnectionId()).thenReturn(123);
            when(mockScheduler.registerConnection(any())).thenReturn(Pair.create(true, ""));

            mockedUUID.when(UUIDUtil::genUUID).thenReturn(mockUUID);
            mockedUUID.when(() -> UUIDUtil.toTUniqueId(mockUUID)).thenReturn(mockTUniqueId);

            mockGlobalStateMgr(mockedGlobalState);

            String token = sessionManager.initializeSession("testUser", "127.0.0.1", "testPassword");
            assertNotNull(token);
            // Token should be plain UUID without host prefix
            assertEquals(mockUUID.toString(), token, "Token should be plain UUID when proxy is disabled");
        }
    }
}
