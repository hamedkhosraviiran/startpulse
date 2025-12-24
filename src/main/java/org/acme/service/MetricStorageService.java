package org.acme.service;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;

@ApplicationScoped
public class MetricStorageService {
    @Inject
    EntityManager entityManager;

    @ConfigProperty(name = "metrics.retention.days", defaultValue = "30")
    int retentionDays;

    @ConfigProperty(name = "metrics.batch.size", defaultValue = "1000")
    int batchSize;

    private static final Logger LOG = Logger.getLogger(MetricStorageService.class);

    @Transactional
    public Uni<Integer> storeMetrics(List<ParsedMetric> metrics, String host) {
        return Uni.createFrom().item(() -> {
            if (metrics.isEmpty()) {
                LOG.debug("No metrics to store");
                return 0;
            }

            List<NginxMetric> dbMetrics = NginxMetric.fromParsedMetrics(metrics, host);
            int savedCount = 0;

            // Batch persist for performance
            for (int i = 0; i < dbMetrics.size(); i++) {
                NginxMetric metric = dbMetrics.get(i);
                entityManager.persist(metric);
                savedCount++;

                // Flush and clear periodically
                if (i % batchSize == 0 && i > 0) {
                    entityManager.flush();
                    entityManager.clear();
                }
            }

            // Final flush
            entityManager.flush();
            entityManager.clear();

            LOG.debugf("Stored %d metric data points for host %s", savedCount, host);

            // Periodic cleanup (1% chance)
            if (Math.random() < 0.01) {
                int deleted = cleanOldMetrics();
                LOG.debugf("Cleaned up %d old metrics", deleted);
            }

            return savedCount;
        });
    }

    @Transactional
    public int cleanOldMetrics() {
        Instant cutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS);

        return entityManager.createQuery(
                        "DELETE FROM NginxMetric m WHERE m.timestamp < :cutoff")
                .setParameter("cutoff", cutoff)
                .executeUpdate();
    }

    // Query methods
    public Uni<List<CounterMetric>> getRequestCounts(Instant from, Instant to, String host) {
        return Uni.createFrom().item(() ->
                entityManager.createQuery(
                                "SELECT NEW com.example.CounterMetric(" +
                                        "  m.metricName, m.metricValue, m.labels, m.timestamp" +
                                        ") FROM NginxMetric m " +
                                        "WHERE m.metricName = 'nginx_http_requests_total' " +
                                        "  AND m.timestamp BETWEEN :from AND :to " +
                                        "  AND (:host IS NULL OR m.host = :host) " +
                                        "ORDER BY m.timestamp", CounterMetric.class)
                        .setParameter("from", from)
                        .setParameter("to", to)
                        .setParameter("host", host)
                        .getResultList()
        );
    }

    public Uni<Double> getAverageResponseTime(Instant from, Instant to, String host) {
        return Uni.createFrom().item(() -> {
            Object result = entityManager.createQuery(
                            "SELECT AVG(m.metricValue) FROM NginxMetric m " +
                                    "WHERE m.metricName = 'nginx_upstream_response_time_seconds_sum' " +
                                    "  AND m.timestamp BETWEEN :from AND :to " +
                                    "  AND (:host IS NULL OR m.host = :host)")
                    .setParameter("from", from)
                    .setParameter("to", to)
                    .setParameter("host", host)
                    .getSingleResult();

            return result != null ? (Double) result : 0.0;
        });
    }
}
