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

import com.starrocks.common.Config;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArrowFlightSqlServiceTest {
    @Test
    public void testDisable() {
        ArrowFlightSqlService service = new ArrowFlightSqlService(-1);
        service.start();
        service.stop();
    }

    /**
     * Mock {@link FlightServer.Builder#build()}.
     */
    @Test
    public void testEnable(@Mocked FlightServer server) throws IOException, InterruptedException {
        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer build() {
                return server;
            }
        };

        new Expectations() {
            {
                server.start();
                result = server;
                times = 1;
            }

            {
                server.shutdown();
                times = 1;
            }

            {
                server.awaitTermination();
                times = 1;
            }

            {
                server.awaitTermination(anyLong, TimeUnit.SECONDS);
                result = true;
                times = 1;
            }
        };

        ArrowFlightSqlService service = new ArrowFlightSqlService(1234);
        service.start();
        service.stop();
    }


    @Test
    public void testSslDisabledUsesInsecureLocation(@Mocked FlightServer server) throws Exception {
        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer build() {
                return server;
            }
        };

        ArrowFlightSqlService service = new ArrowFlightSqlService(1234);

        Field f = ArrowFlightSqlService.class.getDeclaredField("location");
        f.setAccessible(true);
        Location location = (Location) f.get(service);

        assertFalse(location.getUri().getScheme().contains("tls"),
                "Insecure mode must not use a TLS URI scheme, got: " + location.getUri().getScheme());
    }

    @Test
    public void testSslEnabledUsesTlsLocationAndCallsUseTls(
            @Mocked FlightServer server,
            @Mocked KeyStore ks,
            @Mocked Certificate cert,
            @Mocked PrivateKey key) throws Exception {

        File tempKeystore = File.createTempFile("dummy", ".jks");

        Config.arrow_flight_ssl_enabled = true;
        Config.ssl_keystore_location = tempKeystore.getAbsolutePath();
        Config.ssl_keystore_password = "pass";
        Config.ssl_key_password = "pass";

        try {
            new Expectations() {
                {
                    KeyStore.getInstance("JKS");
                    result = ks;
                    ks.aliases();
                    result = Collections.enumeration(List.of("alias"));
                    result = Collections.enumeration(List.of("alias"));
                    ks.getCertificate("alias");
                    result = cert;
                    cert.getEncoded();
                    result = new byte[] {1, 2, 3};
                    ks.getKey("alias", (char[]) any);
                    result = key;
                    key.getEncoded();
                    result = new byte[] {4, 5, 6};
                }
            };

            new MockUp<FlightServer.Builder>() {
                @Mock
                public FlightServer build() {
                    return server;
                }
            };

            ArrowFlightSqlService service = new ArrowFlightSqlService(1234);

            Field f = ArrowFlightSqlService.class.getDeclaredField("location");
            f.setAccessible(true);
            Location location = (Location) f.get(service);

            assertTrue(location.getUri().getScheme().contains("tls"),
                    "TLS mode must use a TLS URI scheme, got: " + location.getUri().getScheme());
        } finally {
            Config.arrow_flight_ssl_enabled = false;
            Config.ssl_keystore_location = "";
            Config.ssl_keystore_password = "";
            Config.ssl_key_password = "";
            tempKeystore.delete();
        }
    }

    @Test
    public void testStartExitsWhenKeystoreLocationEmpty(@Mocked FlightServer server) {
        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer build() {
                return server;
            }
        };

        ArrowFlightSqlService service = new ArrowFlightSqlService(1234);

        Config.arrow_flight_ssl_enabled = true;
        Config.ssl_keystore_location = "";

        new MockUp<System>() {
            @Mock
            public void exit(int value) {
                throw new RuntimeException(String.valueOf(value));
            }
        };

        try {
            RuntimeException ex = assertThrows(RuntimeException.class, service::start);
            assertEquals("-1", ex.getMessage());
        } finally {
            Config.arrow_flight_ssl_enabled = false;
        }
    }

    @Test
    public void testStartExitsWhenKeystoreFileNotFound(@Mocked FlightServer server) {
        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer build() {
                return server;
            }
        };

        ArrowFlightSqlService service = new ArrowFlightSqlService(1234);

        Config.arrow_flight_ssl_enabled = true;
        Config.ssl_keystore_location = "/nonexistent/path/keystore.jks";

        new MockUp<System>() {
            @Mock
            public void exit(int value) {
                throw new RuntimeException(String.valueOf(value));
            }
        };

        try {
            RuntimeException ex = assertThrows(RuntimeException.class, service::start);
            assertEquals("-1", ex.getMessage());
        } finally {
            Config.arrow_flight_ssl_enabled = false;
            Config.ssl_keystore_location = "";
        }
    }

    @Test
    public void testSslEnabledWithPemFiles(@Mocked FlightServer server) throws Exception {
        File tempCert = File.createTempFile("cert", ".pem");
        File tempKey = File.createTempFile("key", ".pem");

        Config.arrow_flight_ssl_enabled = true;
        Config.arrow_flight_ssl_cert_file = tempCert.getAbsolutePath();
        Config.arrow_flight_ssl_key_file = tempKey.getAbsolutePath();

        try {
            new MockUp<FlightServer.Builder>() {
                @Mock
                public FlightServer build() {
                    return server;
                }
            };

            ArrowFlightSqlService service = new ArrowFlightSqlService(1234);

            Field f = ArrowFlightSqlService.class.getDeclaredField("location");
            f.setAccessible(true);
            Location location = (Location) f.get(service);

            assertTrue(location.getUri().getScheme().contains("tls"),
                    "Option B TLS must use a TLS URI scheme, got: " + location.getUri().getScheme());
        } finally {
            Config.arrow_flight_ssl_enabled = false;
            Config.arrow_flight_ssl_cert_file = "";
            Config.arrow_flight_ssl_key_file = "";
            tempCert.delete();
            tempKey.delete();
        }
    }

    @Test
    public void testStartExitsWhenPemKeyFileMissing(@Mocked FlightServer server) throws Exception {
        File tempCert = File.createTempFile("cert", ".pem");

        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer build() {
                return server;
            }
        };

        ArrowFlightSqlService service = new ArrowFlightSqlService(1234);

        Config.arrow_flight_ssl_enabled = true;
        Config.arrow_flight_ssl_cert_file = tempCert.getAbsolutePath();
        Config.arrow_flight_ssl_key_file = "";

        new MockUp<System>() {
            @Mock
            public void exit(int value) {
                throw new RuntimeException(String.valueOf(value));
            }
        };

        try {
            RuntimeException ex = assertThrows(RuntimeException.class, service::start);
            assertEquals("-1", ex.getMessage());
        } finally {
            Config.arrow_flight_ssl_enabled = false;
            Config.arrow_flight_ssl_cert_file = "";
            Config.arrow_flight_ssl_key_file = "";
            tempCert.delete();
        }
    }

    @Test
    public void testStartExitsWhenPemCertFileNotFound(@Mocked FlightServer server) {
        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer build() {
                return server;
            }
        };

        ArrowFlightSqlService service = new ArrowFlightSqlService(1234);

        Config.arrow_flight_ssl_enabled = true;
        Config.arrow_flight_ssl_cert_file = "/nonexistent/cert.pem";
        Config.arrow_flight_ssl_key_file = "/nonexistent/key.pem";

        new MockUp<System>() {
            @Mock
            public void exit(int value) {
                throw new RuntimeException(String.valueOf(value));
            }
        };

        try {
            RuntimeException ex = assertThrows(RuntimeException.class, service::start);
            assertEquals("-1", ex.getMessage());
        } finally {
            Config.arrow_flight_ssl_enabled = false;
            Config.arrow_flight_ssl_cert_file = "";
            Config.arrow_flight_ssl_key_file = "";
        }
    }

    @Test
    public void testToPemFormat() throws Exception {
        // Use a disabled service instance (port = -1) purely as a handle to
        // invoke the private helper via reflection.
        ArrowFlightSqlService service = new ArrowFlightSqlService(-1);

        Method toPem = ArrowFlightSqlService.class
                .getDeclaredMethod("toPem", String.class, byte[].class);
        toPem.setAccessible(true);

        byte[] der = new byte[] {0x30, 0x01, 0x02, 0x03};

        // --- CERTIFICATE ---
        byte[] pemBytes = (byte[]) toPem.invoke(service, "CERTIFICATE", der);
        String pem = new String(pemBytes, StandardCharsets.UTF_8);

        assertTrue(pem.startsWith("-----BEGIN CERTIFICATE-----\n"), "Missing PEM certificate header");
        assertTrue(pem.endsWith("\n-----END CERTIFICATE-----\n"), "Missing PEM certificate footer");

        String body = pem
                .replace("-----BEGIN CERTIFICATE-----\n", "")
                .replace("\n-----END CERTIFICATE-----\n", "");
        assertArrayEquals(der, Base64.getMimeDecoder().decode(body),
                "PEM body must base64-decode back to the original DER bytes");

        // --- PRIVATE KEY ---
        byte[] keyPemBytes = (byte[]) toPem.invoke(service, "PRIVATE KEY", der);
        String keyPem = new String(keyPemBytes, StandardCharsets.UTF_8);

        assertTrue(keyPem.startsWith("-----BEGIN PRIVATE KEY-----\n"), "Missing PEM private-key header");
        assertTrue(keyPem.endsWith("\n-----END PRIVATE KEY-----\n"), "Missing PEM private-key footer");
    }
}
