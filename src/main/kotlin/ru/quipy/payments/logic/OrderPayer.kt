package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.ResponseStatus
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.*


@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val meterRegistry: MeterRegistry,
    @param:Value("\${payment.admission.mode:bounded}") private val admissionMode: String,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private val paymentQueue = if (admissionMode == "blocking") {
        LinkedBlockingQueue<Runnable>(8_000)
    } else {
        LinkedBlockingQueue(paymentService.queueCapacity())
    }

    private val rejectionHandler: RejectedExecutionHandler = if (admissionMode == "blocking") {
        CallerBlockingRejectedExecutionHandler()
    } else {
        ThreadPoolExecutor.AbortPolicy()
    }

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        paymentQueue,
        NamedThreadFactory("payment-submission-executor"),
        rejectionHandler
    )

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
        Gauge.builder("payments.admission.queue.size", paymentExecutor.queue) { it.size.toDouble() }
            .register(meterRegistry)
        Gauge.builder("payments.admission.queue.capacity", paymentExecutor.queue) {
            (it.size + it.remainingCapacity()).toDouble()
        }.register(meterRegistry)
        Gauge.builder("payments.admission.executor.active", paymentExecutor) { it.activeCount.toDouble() }
            .register(meterRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val admissionSample = Timer.start(meterRegistry)
        try {
            paymentExecutor.execute {
                admissionStarted.increment()
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
            admissionAccepted.increment()
        } catch (e: RejectedExecutionException) {
            admissionRejected.increment()
            throw PaymentQueueFullException(e)
        } finally {
            admissionSample.stop(admissionWait)
        }
        return createdAt
    }
}

@ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
class PaymentQueueFullException(cause: Throwable) : RuntimeException("Payment queue is full", cause)