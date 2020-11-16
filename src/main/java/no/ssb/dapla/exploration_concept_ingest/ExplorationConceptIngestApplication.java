package no.ssb.dapla.exploration_concept_ingest;


import ch.qos.logback.classic.util.ContextInitializer;
import io.helidon.config.Config;
import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.media.jackson.JacksonSupport;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebTracingConfig;
import io.helidon.webserver.accesslog.AccessLogSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.LogManager;

import static io.helidon.config.ConfigSources.classpath;
import static io.helidon.config.ConfigSources.file;

public class ExplorationConceptIngestApplication {

    private static final Logger LOG;

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        LOG = LoggerFactory.getLogger(ExplorationConceptIngestApplication.class);
    }

    public static void initLogging() {
    }

    public static Config createDefaultConfig() {
        Config.Builder builder = Config.builder();
        String overrideFile = System.getenv("HELIDON_CONFIG_FILE");
        if (overrideFile != null) {
            builder.addSource(file(overrideFile).optional());
        }
        builder.addSource(file("conf/application.yaml").optional());
        builder.addSource(classpath("application.yaml"));
        return builder.build();
    }

    /**
     * Application main entry point.
     *
     * @param args command line arguments.
     */
    public static void main(final String[] args) throws InterruptedException {
        ExplorationConceptIngestApplication app = new ExplorationConceptIngestApplication(createDefaultConfig());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.get(ExplorationConceptIngestService.class).close();
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

        if (app.get(Config.class).get("pipe.trigger.autostart.enabled").asBoolean().orElse(false)) {
            int delaySeconds = app.get(Config.class).get("pipe.trigger.autostart.delaysec").asInt().orElse(0);
            if (delaySeconds > 0) {
                Thread.sleep(1000 * Math.min(3600, delaySeconds));
            }
            LOG.info("Pipe triggered automatically.");
            app.get(ExplorationConceptIngestService.class).triggerStart();
        }
    }

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    ExplorationConceptIngestApplication(Config config) {
        put(Config.class, config);

        HealthSupport health = HealthSupport.builder()
                .addLiveness(HealthChecks.healthChecks())   // Adds a convenient set of checks
                .build();
        MetricsSupport metrics = MetricsSupport.create();

        ExplorationConceptIngestService conceptToExplorationLdsService = new ExplorationConceptIngestService(config);
        put(ExplorationConceptIngestService.class, conceptToExplorationLdsService);

        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("server.access-log")))
                .register(WebTracingConfig.builder()
                        .config(config.get("tracing"))
                        .build())
                .register(health)  // "/health"
                .register(metrics) // "/metrics"
                .register("/pipe", conceptToExplorationLdsService)
                .build();

        WebServer server = WebServer.builder()
                .config(config.get("server"))
                .addMediaSupport(JacksonSupport.create())
                .routing(routing)
                .build();
        put(WebServer.class, server);
    }

    public <T> T put(Class<T> clazz, T instance) {
        return (T) instanceByType.put(clazz, instance);
    }

    public <T> T get(Class<T> clazz) {
        return (T) instanceByType.get(clazz);
    }
}
