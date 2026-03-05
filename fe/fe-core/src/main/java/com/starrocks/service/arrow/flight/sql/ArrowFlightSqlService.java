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

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.service.FrontendOptions;
import com.starrocks.service.arrow.flight.sql.auth2.ArrowFlightSqlAuthenticator;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class ArrowFlightSqlService {

    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlService.class);

    private final Location location;

    // Arrow Flight server encapsulation class, based on gRPC implementation
    private final FlightServer flightServer;

    private final Location feEndpoint;

    private final ArrowFlightSqlServiceImpl producer;

    protected volatile boolean running;

    public ArrowFlightSqlService(int port) {
        // Disable Arrow Flight SQL feature if port is not set to a positive value.
        if (port <= 0) {
            this.location = null;
            this.flightServer = null;
            this.feEndpoint = null;
            this.producer = null;
            return;
        }

        // Memory allocator: A memory allocator is needed to manage memory.
        BufferAllocator allocator = new RootAllocator();
        boolean useSsl = Config.arrow_flight_ssl_enabled;

        this.location = useSsl ? Location.forGrpcTls("0.0.0.0", port)
                : Location.forGrpcInsecure("0.0.0.0", port);
        this.feEndpoint = useSsl ? Location.forGrpcTls(FrontendOptions.getLocalHostAddress(), port)
                : Location.forGrpcInsecure(FrontendOptions.getLocalHostAddress(), port);

        ArrowFlightSqlSessionManager sessionManager = new ArrowFlightSqlSessionManager();
        ArrowFlightSqlAuthenticator authenticator = new ArrowFlightSqlAuthenticator(sessionManager);

        // Request handler: Processing client SQL requests.
        this.producer = new ArrowFlightSqlServiceImpl(sessionManager, feEndpoint);

        // Constructs the server object of the Arrow Flight SQL.
        FlightServer.Builder builder = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(authenticator);

        if (useSsl) {
            try {
                if (!Strings.isNullOrEmpty(Config.arrow_flight_ssl_cert_file)) {
                    // read PEM cert/key files directly
                    builder.useTls(new File(Config.arrow_flight_ssl_cert_file),
                            new File(Config.arrow_flight_ssl_key_file));
                } else {
                    // JKS keystore → in-memory PEM conversion
                    builder.useTls(extractCertPem(), extractPrivateKeyPem());
                }
            } catch (Exception e) {
                LOG.error("[ARROW] Failed to load TLS certificate: {}", e.getMessage(), e);
                System.exit(-1);
            }
        }
        this.flightServer = builder.build();
    }

    public void start() {
        if (running) {
            return;
        }

        if (location == null) {
            LOG.info("[ARROW] Arrow Flight SQL server is disabled. You can modify `arrow_flight_port` in `fe.conf` " +
                    "to a positive value to enable it.");
            return;
        }

        if (Config.arrow_flight_ssl_enabled) {
            if (!Strings.isNullOrEmpty(Config.arrow_flight_ssl_cert_file)) {
                if (Strings.isNullOrEmpty(Config.arrow_flight_ssl_key_file)) {
                    LOG.error("[ARROW] arrow_flight_ssl_cert_file is set but arrow_flight_ssl_key_file is not.");
                    System.exit(-1);
                }
                if (!new File(Config.arrow_flight_ssl_cert_file).exists()) {
                    LOG.error("[ARROW] SSL cert file not found: {}", Config.arrow_flight_ssl_cert_file);
                    System.exit(-1);
                }
                if (!new File(Config.arrow_flight_ssl_key_file).exists()) {
                    LOG.error("[ARROW] SSL key file not found: {}", Config.arrow_flight_ssl_key_file);
                    System.exit(-1);
                }
            } else {
                if (Strings.isNullOrEmpty(Config.ssl_keystore_location)) {
                    LOG.error("[ARROW] arrow_flight_ssl_enabled=true but neither arrow_flight_ssl_cert_file " +
                            "nor ssl_keystore_location is configured.");
                    System.exit(-1);
                }
                if (!new File(Config.ssl_keystore_location).exists()) {
                    LOG.error("[ARROW] Keystore file not found: {}", Config.ssl_keystore_location);
                    System.exit(-1);
                }
            }
        }

        try {
            flightServer.start();
            running = true;
            LOG.info("[ARROW] Arrow Flight SQL server start [location={}] [feEndpoint={}].", location, feEndpoint);
            flightServer.awaitTermination();
        } catch (InterruptedException e) {
            LOG.warn("[ARROW] Interrupted while stopping Arrow Flight SQL server", e);
            Thread.currentThread().interrupt();
            System.exit(-1);
        } catch (Exception e) {
            LOG.error("[ARROW] Failed to start Arrow Flight SQL server on {}:{}. Its port might be occupied. You can " +
                            "modify `arrow_flight_port` in `fe.conf` to an unused port or set it to -1 to disable it." +
                            (Config.arrow_flight_ssl_enabled ?
                                    " Check arrow_flight_ssl_cert_file/arrow_flight_ssl_key_file " +
                                            "or ssl_keystore_location and related SSL settings." : ""),
                    location.getUri().getHost(),
                    location.getUri().getPort(), e);
            System.exit(-1);
        }
    }

    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        try {
            LOG.info("[ARROW] Stopping Arrow Flight SQL server .");
            flightServer.shutdown();
            flightServer.awaitTermination(1, TimeUnit.SECONDS);
            producer.close();
        } catch (InterruptedException e) {
            LOG.warn("[ARROW] Interrupted while stopping Arrow Flight SQL server", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("[ARROW] Error while stopping Arrow Flight SQL server", e);
        }
    }

    /** Loads the JKS keystore using the shared ssl_keystore_* config. */
    private KeyStore loadKeyStore() throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(Config.ssl_keystore_location)) {
            ks.load(fis, Config.ssl_keystore_password.toCharArray());
        }
        return ks;
    }

    // Extracts the first certificate from the JKS keystore as an in-memory PEM stream.
    private InputStream extractCertPem() throws Exception {
        KeyStore ks = loadKeyStore();
        String alias = ks.aliases().nextElement();
        return new ByteArrayInputStream(toPem("CERTIFICATE", ks.getCertificate(alias).getEncoded()));
    }

    // Extracts the private key from the JKS keystore as an in-memory PKCS8 PEM stream.
    private InputStream extractPrivateKeyPem() throws Exception {
        KeyStore ks = loadKeyStore();
        String alias = ks.aliases().nextElement();
        PrivateKey key = (PrivateKey) ks.getKey(alias, Config.ssl_key_password.toCharArray());
        return new ByteArrayInputStream(toPem("PRIVATE KEY", key.getEncoded()));
    }

    private byte[] toPem(String type, byte[] der) {
        String header = "-----BEGIN " + type + "-----\n";
        String footer = "\n-----END " + type + "-----\n";
        String body = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(der);
        return (header + body + footer).getBytes(StandardCharsets.UTF_8);
    }

}
