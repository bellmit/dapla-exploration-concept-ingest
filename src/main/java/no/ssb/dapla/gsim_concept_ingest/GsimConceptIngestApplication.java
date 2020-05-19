package no.ssb.dapla.gsim_concept_ingest;


import ch.qos.logback.classic.util.ContextInitializer;
import io.helidon.config.Config;
import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.media.jackson.server.JacksonSupport;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebTracingConfig;
import io.helidon.webserver.accesslog.AccessLogSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.LogManager;

public class GsimConceptIngestApplication {

    private static final Logger LOG;

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        LOG = LoggerFactory.getLogger(GsimConceptIngestApplication.class);
    }

    public static void initLogging() {
    }

    /**
     * Application main entry point.
     *
     * @param args command line arguments.
     */
    public static void main(final String[] args) {
        GsimConceptIngestApplication app = new GsimConceptIngestApplication(Config.create());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.get(GsimConceptIngestService.class).close();
            app.get(WebServer.class).shutdown().toCompletableFuture().join();
            LOG.info("Shutdown complete.");
        }));

        // Try to start the server. If successful, print some info and arrange to
        // print a message at shutdown. If unsuccessful, print the exception.
        app.get(WebServer.class).start()
                .thenAccept(ws -> {
                    LOG.info("WebServer running at port " + ws.port());
                })
                .exceptionally(t -> {
                    LOG.error("Startup failed", t);
                    return null;
                })
                .toCompletableFuture()
                .join();

        if (app.get(Config.class).get("pipe.trigger.autostart").asBoolean().orElse(false)) {
            LOG.info("Pipe triggered automatically.");
            app.get(GsimConceptIngestService.class).triggerStart();
        }
    }

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    GsimConceptIngestApplication(Config config) {
        put(Config.class, config);

        HealthSupport health = HealthSupport.builder()
                .addLiveness(HealthChecks.healthChecks())   // Adds a convenient set of checks
                .build();
        MetricsSupport metrics = MetricsSupport.create();

        GsimConceptIngestService conceptToGsimLdsService = new GsimConceptIngestService(config);
        put(GsimConceptIngestService.class, conceptToGsimLdsService);

        WebServer server = WebServer.create(ServerConfiguration.create(config.get("server")), Routing.builder()
                .register(AccessLogSupport.create(config.get("server.access-log")))
                .register(WebTracingConfig.create(config.get("tracing")))
                .register(JacksonSupport.create())
                .register(health)  // "/health"
                .register(metrics) // "/metrics"
                .register("/pipe", conceptToGsimLdsService)
                .build());
        put(WebServer.class, server);
    }

    public <T> T put(Class<T> clazz, T instance) {
        return (T) instanceByType.put(clazz, instance);
    }

    public <T> T get(Class<T> clazz) {
        return (T) instanceByType.get(clazz);
    }
}
