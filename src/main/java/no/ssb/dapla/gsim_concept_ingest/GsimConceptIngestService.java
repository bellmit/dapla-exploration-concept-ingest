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
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.ofNullable;

public class GsimConceptIngestService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(GsimConceptIngestService.class);

    private final ObjectMapper msgPackMapper = new ObjectMapper(new MessagePackFactory());
    private final Config config;

    private final AtomicBoolean waitLoopAllowed = new AtomicBoolean(false);
    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    GsimConceptIngestService(Config config) {
        this.config = config;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules
                .get("/trigger", this::getRevisionHandler)
                .put("/trigger", this::putRevisionHandler)
                .delete("/trigger", this::deleteRevisionHandler)
        ;
    }

    void getRevisionHandler(ServerRequest request, ServerResponse response) {
        response.headers().contentType(MediaType.APPLICATION_JSON);
        response.status(200).send("[]");
    }

    void putRevisionHandler(ServerRequest request, ServerResponse response) {
        triggerStart();

        response.headers().contentType(MediaType.APPLICATION_JSON);
        response.status(200).send("[]");
    }

    void deleteRevisionHandler(ServerRequest request, ServerResponse response) {
        triggerStop();
        response.status(200).send();
    }

    public void triggerStop() {
        waitLoopAllowed.set(false);
    }

    public void triggerStart() {
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

        waitLoopAllowed.set(true);
    }

    Optional<String> getLatestSourceIdFromTarget() {
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

    void sendMessageToTarget(RawdataMessage message) throws IOException {
        JsonNode meta = msgPackMapper.readTree(message.get("meta"));

        String method = meta.get("method").textValue();
        String sourceNamespace = meta.get("namespace").textValue();
        String entity = meta.get("entity").textValue();
        String id = meta.get("id").textValue();
        String versionStr = meta.get("version").textValue();

        String namespace = config.get("pipe.target.namespace").asString().get();
        String source = config.get("pipe.target.source").asString().get();
        String sourceId = message.ulid().toString();

        WebClient webClient = (WebClient) instanceByType.get(WebClient.class);

        String path = String.format("%s/%s/%s", namespace, entity, id);

        if ("PUT".equals(method)) {
            JsonNode data = msgPackMapper.readTree(message.get("data"));
            WebClientRequestBuilder builder = webClient.put();
            builder.headers().add("Origin", "localhost");
            WebClientResponse response = builder
                    .path(path)
                    .queryParam("timestamp", versionStr)
                    .queryParam("source", source)
                    .queryParam("sourceId", sourceId)
                    .submit(data)
                    .toCompletableFuture()
                    .join();
            if (!Http.ResponseStatus.Family.SUCCESSFUL.equals(response.status().family())) {
                throw new RuntimeException("Got http status code " + response.status() + " with reason: " + response.status().reasonPhrase());
            }
        } else if ("DELETE".equals(method)) {
            WebClientRequestBuilder builder = webClient.delete();
            builder.headers().add("Origin", "localhost");
            WebClientResponse response = builder
                    .path(path)
                    .queryParam("timestamp", versionStr)
                    .queryParam("source", source)
                    .queryParam("sourceId", sourceId)
                    .submit()
                    .toCompletableFuture()
                    .join();
            if (!Http.ResponseStatus.Family.SUCCESSFUL.equals(response.status().family())) {
                throw new RuntimeException("Got http status code " + response.status() + " with reason: " + response.status().reasonPhrase());
            }
        } else {
            throw new RuntimeException("Unsupported method: " + method);
        }
    }

    class Pipe implements Runnable {
        @Override
        public void run() {
            try {
                String topic = config.get("pipe.source.topic").asString().get();
                ULID.Value previousUlid = getLatestSourceIdFromTarget().map(ULID::parseULID).orElse(null);
                try (RawdataConsumer consumer = ((RawdataClient) instanceByType.get(RawdataClient.class)).consumer(topic, previousUlid, false)) {
                    while (waitLoopAllowed.get()) {
                        RawdataMessage message = consumer.receive(3, TimeUnit.SECONDS);
                        if (message != null) {
                            sendMessageToTarget(message);
                        }
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
