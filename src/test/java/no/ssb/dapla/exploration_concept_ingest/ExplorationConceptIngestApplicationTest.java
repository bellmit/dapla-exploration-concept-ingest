/*
 * Copyright (c) 2018, 2020 Oracle and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.ssb.dapla.exploration_concept_ingest;

import io.helidon.config.Config;
import io.helidon.media.common.DefaultMediaSupport;
import io.helidon.media.jackson.common.JacksonSupport;
import io.helidon.webclient.WebClient;
import io.helidon.webserver.WebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static io.helidon.config.ConfigSources.classpath;

public class ExplorationConceptIngestApplicationTest {

    private static final Logger LOG = LoggerFactory.getLogger(ExplorationConceptIngestApplicationTest.class);

    static {
        ExplorationConceptIngestApplication.initLogging();
    }

    private static WebServer webServer;

    @BeforeAll
    public static void startTheServer() {
        Config config = Config
                .builder(classpath("application-dev.yaml"),
                        classpath("application.yaml"))
                .metaConfig()
                .build();
        long webServerStart = System.currentTimeMillis();
        webServer = new ExplorationConceptIngestApplication(config).get(WebServer.class);
        webServer.start().toCompletableFuture()
                .thenAccept(webServer -> {
                    long duration = System.currentTimeMillis() - webServerStart;
                    LOG.info("Server started in {} ms, listening at port {}", duration, webServer.port());
                })
                .orTimeout(5, TimeUnit.SECONDS)
                .join();
    }

    @AfterAll
    public static void stopServer() {
        if (webServer != null) {
            webServer.shutdown()
                    .toCompletableFuture()
                    .orTimeout(10, TimeUnit.SECONDS)
                    .join();
        }
    }

    @Test
    public void testHelloWorld() {
        WebClient webClient = WebClient.builder()
                .baseUri("http://localhost:" + webServer.port())
                .addMediaSupport(DefaultMediaSupport.create(true))
                .addMediaSupport(JacksonSupport.create())
                .build();

        webClient.put()
                .path("/pipe/trigger")
                .submit()
                .thenAccept(response -> Assertions.assertEquals(200, response.status().code()))
                .exceptionally(throwable -> {
                    Assertions.fail(throwable);
                    return null;
                })
                .toCompletableFuture()
                .orTimeout(60, TimeUnit.SECONDS)
                .join();
    }
}
