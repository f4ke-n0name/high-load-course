package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalMetrics(
    accountName: String,
    parallelRequests: Int,
    private val meterRegistry: MeterRegistry,
) {
    private val inFlightRequests = AtomicInteger()
    private val successfulResponses = Counter.builder("payments.external.responses")
        .tag("account", accountName)
        .tag("result", "success")
        .register(meterRegistry)
    private val failedResponses = Counter.builder("payments.external.responses")
        .tag("account", accountName)
        .tag("result", "failure")
        .register(meterRegistry)
    private val semaphoreWaitTimer = Timer.builder("payments.external.semaphore.wait")
        .tag("account", accountName)
        .description("Time spent waiting for an external request permit")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(meterRegistry)

    init {
        Gauge.builder("payments.external.in.flight") { inFlightRequests.get().toDouble() }
            .tag("account", accountName)
            .register(meterRegistry)
        Gauge.builder("payments.external.parallel.limit") { parallelRequests.toDouble() }
            .tag("account", accountName)
            .register(meterRegistry)
    }

    fun startSemaphoreWait(): Timer.Sample = Timer.start(meterRegistry)

    fun recordSemaphoreAcquired(sample: Timer.Sample) {
        sample.stop(semaphoreWaitTimer)
    }

    fun requestStarted() {
        inFlightRequests.incrementAndGet()
    }

    fun requestFinished() {
        inFlightRequests.decrementAndGet()
    }

    fun responseReceived(success: Boolean) {
        if (success) successfulResponses.increment() else failedResponses.increment()
    }
}
