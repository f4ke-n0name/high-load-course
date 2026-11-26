package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalMetrics(
    serviceName: String,
    accountName: String,
    parallelRequests: Int,
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

    private val retryOutcomes = listOf(
        "success",
        "failure",
        "provider_timeout",
        "client_timeout",
        "error",
    ).associateWith { outcome ->
        Counter.builder("payments.provider.retry.outcomes")
            .description("Outcome of provider HTTP attempts that were retries")
            .tags(tags.and("outcome", outcome))
            .register(meterRegistry)
    }

    private val attemptOutcomes = listOf(
        "success",
        "failure",
        "provider_timeout",
        "client_timeout",
        "error",
    ).associateWith { outcome ->
        Counter.builder("payments.provider.attempts")
            .description("Provider HTTP attempts by outcome")
            .tags(tags.and("outcome", outcome))
            .register(meterRegistry)
    }

    init {
        Gauge.builder("payments.external.in.flight") { inFlightRequests.get().toDouble() }
            .tags(tags)
            .register(meterRegistry)
        Gauge.builder("payments.external.parallel.limit") { parallelRequests.toDouble() }
            .tags(tags)
            .register(meterRegistry)
    }

    fun startSemaphoreWait(): Timer.Sample = Timer.start(meterRegistry)

    fun recordSemaphoreAcquired(sample: Timer.Sample) {
        sample.stop(semaphoreWaitTimer)
    }

    fun startHttpAttempt(isRetry: Boolean) {
        if (isRetry) providerRetries.increment()
        inFlightRequests.incrementAndGet()
    }

    fun finishHttpAttempt(outcome: String, isRetry: Boolean) {
        inFlightRequests.decrementAndGet()
        val attemptOutcome = attemptOutcomes[outcome] ?: attemptOutcomes.getValue("error")
        attemptOutcome.increment()
        if (isRetry) {
            val retryOutcome = retryOutcomes[outcome] ?: retryOutcomes.getValue("error")
            retryOutcome.increment()
        }
    }

    fun responseReceived(success: Boolean) {
        if (success) successfulResponses.increment() else failedResponses.increment()
    }
}
