/*
 * Copyright (c) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
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

package no.ssb.dapla.gsim_concept_ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.huxhorn.sulky.ulid.ULID;
import io.helidon.common.http.Http;
import io.helidon.common.http.MediaType;
import io.helidon.config.Config;
import io.helidon.media.common.DefaultMediaSupport;
import io.helidon.media.jackson.common.JacksonSupport;
import io.helidon.webclient.WebClient;
import io.helidon.webclient.WebClientRequestBuilder;
import io.helidon.webclient.WebClientResponse;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Optional.ofNullable;

public class GsimConceptIngestService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(GsimConceptIngestService.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Config config;

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    GsimConceptIngestService(Config config) {
        this.config = config;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules
                .get("/trigger", this::getRevisionHandler)
                .put("/trigger", this::putRevisionHandler)
        ;
    }

    private void getRevisionHandler(ServerRequest request, ServerResponse response) {
        response.headers().contentType(MediaType.APPLICATION_JSON);
        response.status(200).send("[]");
    }

    private void putRevisionHandler(ServerRequest request, ServerResponse response) {
        instanceByType.computeIfAbsent(RawdataClient.class, k -> {
            String provider = config.get("pipe.source.provider.name").asString().get();
            Map<String, String> providerConfig = config.get("pipe.source.provider.config").detach().asMap().get();
            return ProviderConfigurator.configure(providerConfig, provider, RawdataClientInitializer.class);
        });

        instanceByType.computeIfAbsent(WebClient.class, k -> {
            Config targetConfig = this.config.get("pipe.target");
            String scheme = targetConfig.get("scheme").asString().get();
            String host = targetConfig.get("host").asString().get();
            int port = targetConfig.get("port").asInt().get();
            URI ldsBaseUri;
            try {
                ldsBaseUri = new URI(scheme, null, host, port, null, null, null);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }

            return WebClient.builder()
                    .addMediaSupport(DefaultMediaSupport.create(true))
                    .addMediaSupport(JacksonSupport.create())
                    .baseUri(ldsBaseUri)
                    .build();
        });

        instanceByType.computeIfAbsent(Pipe.class, k -> {
            Pipe pipe = new Pipe();
            Thread thread = new Thread(pipe);
            instanceByType.put(Thread.class, thread);
            thread.start();
            return pipe;
        });

        response.headers().contentType(MediaType.APPLICATION_JSON);
        response.status(200).send("[]");
    }

    private Optional<String> getLatestSourceIdFromTarget() {
        Config targetConfig = this.config.get("pipe.target");
        String source = targetConfig.get("source").asString().get();
        WebClient webClient = (WebClient) instanceByType.get(WebClient.class);
        WebClientRequestBuilder builder = webClient.get();
        builder.headers().add("Origin", "localhost");
        WebClientResponse response = builder
                .path("source/" + source)
                .submit()
                .toCompletableFuture()
                .join();
        if (!Http.ResponseStatus.Family.SUCCESSFUL.equals(response.status().family())) {
            throw new RuntimeException("Got http status code " + response.status() + " with reason: " + response.status().reasonPhrase());
        }
        JsonNode body = response.content().as(JsonNode.class).toCompletableFuture().join();
        return ofNullable(body.get("lastSourceId").textValue());
    }

    private void sendMessageToTarget(RawdataMessage message) {
        System.out.printf("Going to send the following message to target: %n%s%n", message);
    }

    class Pipe implements Runnable {
        @Override
        public void run() {
            try {
                String topic = config.get("pipe.source.topic").asString().get();
                ULID.Value previousUlid = getLatestSourceIdFromTarget().map(ULID::parseULID).orElse(null);
                try (RawdataConsumer consumer = ((RawdataClient) instanceByType.get(RawdataClient.class)).consumer(topic, previousUlid, false)) {
                    RawdataMessage message = consumer.receive(3, TimeUnit.SECONDS);
                    while (message != null) {
                        sendMessageToTarget(message);
                        message = consumer.receive(3, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } finally {
                instanceByType.remove(Pipe.class);
            }
        }
    }

    public void close() {
        RawdataClient rawdataClient = (RawdataClient) instanceByType.get(RawdataClient.class);
        if (rawdataClient != null) {
            try {
                rawdataClient.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
