package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalMetrics(
    serviceName: String,
    accountName: String,
    parallelRequests: Int,
    private val timeoutPolicy: PaymentTimeoutPolicy,
    private val meterRegistry: MeterRegistry,
) {
    private val inFlightRequests = AtomicInteger()
    private val tags = Tags.of(
        "service", serviceName,
        "account", accountName,
        "accountName", accountName,
    )

    private val successfulResponses = Counter.builder("payments.external.responses")
        .tags(tags.and("result", "success"))
        .register(meterRegistry)
    private val failedResponses = Counter.builder("payments.external.responses")
        .tags(tags.and("result", "failure"))
        .register(meterRegistry)
    private val semaphoreWaitTimer = Timer.builder("payments.external.semaphore.wait")
        .tags(tags)
        .description("Time spent waiting for an external request permit")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(meterRegistry)

    private val providerRetries = Counter.builder("payments.provider.retries")
        .description("Provider HTTP attempts that were retries")
        .tags(tags)
        .register(meterRegistry)

    private val requestDurationTimers = listOf(
        "success",
        "failure",
        "provider_timeout",
        "client_timeout",
        "error",
    ).associateWith { outcome ->
        Timer.builder("payments.provider.request.duration")
            .description("Provider HTTP attempt duration including response reading and decoding")
            .tags(tags.and("outcome", outcome))
            .publishPercentiles(0.5, 0.9, 0.95, 0.99)
            .register(meterRegistry)
    }

    init {
        Gauge.builder("payments.external.in.flight") { inFlightRequests.get().toDouble() }
            .tags(tags)
            .register(meterRegistry)
        Gauge.builder("payments.external.parallel.limit") { parallelRequests.toDouble() }
            .tags(tags)
            .register(meterRegistry)
        Gauge.builder("payments.provider.timeout.seconds", timeoutPolicy) { it.serverMillis / 1_000.0 }
            .description("Configured per-attempt provider timeout in seconds")
            .tags(tags.and("source", "provider"))
            .strongReference(true)
            .register(meterRegistry)
        Gauge.builder("payments.provider.timeout.seconds", timeoutPolicy) { it.callMillis / 1_000.0 }
            .description("Configured per-attempt client call timeout in seconds")
            .tags(tags.and("source", "client"))
            .strongReference(true)
            .register(meterRegistry)
    }

    fun startSemaphoreWait(): Timer.Sample = Timer.start(meterRegistry)

    fun recordSemaphoreAcquired(sample: Timer.Sample) {
        sample.stop(semaphoreWaitTimer)
    }

    fun startHttpAttempt(isRetry: Boolean): Long {
        if (isRetry) providerRetries.increment()
        inFlightRequests.incrementAndGet()
        return System.nanoTime()
    }

    fun finishHttpAttempt(startedAtNanos: Long, outcome: String) {
        inFlightRequests.decrementAndGet()
        val timer = requestDurationTimers[outcome] ?: requestDurationTimers.getValue("error")
        timer.record(System.nanoTime() - startedAtNanos, TimeUnit.NANOSECONDS)
    }

    fun responseReceived(success: Boolean) {
        if (success) successfulResponses.increment() else failedResponses.increment()
    }
}
