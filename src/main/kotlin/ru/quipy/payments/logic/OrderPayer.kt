package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import jakarta.annotation.PreDestroy
import java.util.concurrent.TimeUnit


@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val meterRegistry: MeterRegistry,
    @Value("\${payment.executor.threads:12}") private val workersAmount: Int,
    @Value("\${payment.executor.queue-capacity:10}") private val queueCapacity: Int,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private val paymentExecutor by lazy {
        require(workersAmount > 0 && queueCapacity > 0) { "Executor threads and queue capacity must be positive" }
        ThreadPoolExecutor(
            workersAmount,
            workersAmount,
            0L,
            TimeUnit.MILLISECONDS,
            ArrayBlockingQueue(queueCapacity),
            NamedThreadFactory("payment-submission-executor"),
            ThreadPoolExecutor.AbortPolicy()
        ).also { executor ->
            ExecutorServiceMetrics.monitor(meterRegistry, executor, "payment-submission-executor")
        }
    }

    private val admissionWait = Timer.builder("payments.admission.wait")
        .description("Time spent accepting a payment into the executor")
        .register(meterRegistry)
    private val admissionAccepted = Counter.builder("payments.admission.accepted")
        .register(meterRegistry)
    private val admissionStarted = Counter.builder("payments.admission.started")
        .register(meterRegistry)
    private val admissionRejected = Counter.builder("payments.admission.rejected")
        .register(meterRegistry)

    init {
        logger.info("Payment executor: workers={}, queue={}", workersAmount, queueCapacity)
        Gauge.builder("payments.admission.queue.size", paymentExecutor.queue) { it.size.toDouble() }
            .register(meterRegistry)
        Gauge.builder("payments.admission.queue.capacity", paymentExecutor.queue) {
            (it.size + it.remainingCapacity()).toDouble()
        }.register(meterRegistry)
        Gauge.builder("payments.admission.executor.active", paymentExecutor) { it.activeCount.toDouble() }
            .register(meterRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()
        val admissionSample = Timer.start(meterRegistry)
        try {
            if (deadline <= createdAt) {
                admissionRejected.increment()
                return null
            }
            paymentExecutor.submit {
                admissionStarted.increment()
                val createdEvent = paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
            admissionAccepted.increment()
        } catch (e: RejectedExecutionException) {
            admissionRejected.increment()
            return null
        } finally {
            admissionSample.stop(admissionWait)
        }
        return createdAt
    }

    fun retryAfterMillis(): Long = 100L

    @PreDestroy
    fun shutdown() = paymentExecutor.shutdown()

}