package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.AutoCloseableSemaphore
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.ceil


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metrics: PaymentExternalMetrics,
    private val client: OkHttpClient = OkHttpClient.Builder()
        .retryOnConnectionFailure(false)
        .followRedirects(false)
        .followSslRedirects(false)
        .readTimeout(Duration.ZERO)
        .build(),
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        window = Duration.ofMillis(1000),
        prioritizeDeadline = true,
    )

    private val concurrentRequests = AutoCloseableSemaphore(
        permits = properties.parallelRequests,
        fair = true,
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (attempt in 1..4) {
            if (attempt > 1 && (Thread.currentThread().isInterrupted || now() >= deadline)) return
            val startedAt = if (attempt == 1) paymentStartedAt else now()
            if (!performAttempt(paymentId, amount, startedAt, deadline)) return
        }
    }

    private fun performAttempt(paymentId: UUID, amount: Int, startedAt: Long, deadline: Long): Boolean {
        val transactionId = UUID.randomUUID()
        val submittedAt = now()
        paymentESService.update(paymentId) {
            it.logSubmission(submittedAt < deadline, transactionId, submittedAt,
                Duration.ofMillis((submittedAt - startedAt).coerceAtLeast(0)))
        }

        val body = try {
            callProvider(paymentId, transactionId, amount, deadline)
        } catch (e: Exception) {
            val reason = when (e) {
                is InterruptedException -> "Payment submission interrupted."
                is SocketTimeoutException -> e.message
                is InterruptedIOException -> "Request timeout."
                else -> e.message
            }
            metrics.responseReceived(false)
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            try {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
            } finally {
                if (e is InterruptedException) Thread.currentThread().interrupt()
            }
            return false
        }

        metrics.responseReceived(body.result)
        logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
        return !body.result && body.message == "Temporary error"
    }

    private fun callProvider(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long): ExternalSysResponse {
        val remainingMillis = deadline - now()
        val semaphoreWait = metrics.startSemaphoreWait()
        if (remainingMillis <= 0 || !concurrentRequests.semaphore.tryAcquire(remainingMillis, TimeUnit.MILLISECONDS)) {
            throw SocketTimeoutException("Payment deadline exceeded before acquiring account permit")
        }
        try {
            metrics.recordSemaphoreAcquired(semaphoreWait)
            if (!rateLimiter.tryTickBefore(deadline)) {
                throw SocketTimeoutException("Payment deadline exceeded while waiting for rate limit")
            }
            val request = Request.Builder()
                .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                .post(emptyBody)
                .build()
            val call = client.newCall(request)
            val remaining = deadline - now()
            if (remaining <= 0) throw SocketTimeoutException("Payment deadline exceeded before HTTP call")
            call.timeout().timeout(remaining, TimeUnit.MILLISECONDS)
            metrics.requestStarted()
            try {
                return call.execute().use { response ->
                    if (!response.isSuccessful) {
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "HTTP ${response.code}")
                    } else {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    }
                }
            } finally {
                metrics.requestFinished()
            }
        } finally {
            concurrentRequests.semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun throughputPerSec() = rateLimitPerSec.toLong()

    override fun parallelRequests() = parallelRequests

    override fun averageProcessingTime() = requestAverageProcessingTime

    override fun queueCapacity(): Int {
        val rateLimitedCapacity = ceil(
            rateLimitPerSec * requestAverageProcessingTime.toMillis() / 1_000.0
        ).toInt()
        return minOf(parallelRequests, rateLimitedCapacity).coerceAtLeast(1)
    }

    private fun now() = System.currentTimeMillis()
}