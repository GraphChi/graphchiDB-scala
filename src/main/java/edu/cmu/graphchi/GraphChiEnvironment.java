package edu.cmu.graphchi;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

/**
 * Static environment. Should probably use dependency injection like Spring.
 * @author Aapo Kyrola
 */
public class GraphChiEnvironment {

    public static MetricRegistry metrics = new MetricRegistry();


    public static void reportMetrics() {
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.report();
    }
}
