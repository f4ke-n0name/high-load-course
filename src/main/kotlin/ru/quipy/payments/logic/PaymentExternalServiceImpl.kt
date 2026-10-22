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
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.ceil


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metrics: PaymentExternalMetrics,
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
        window = Duration.ofMillis(1000)
    )

    private val concurrentRequests = AutoCloseableSemaphore(
        permits = properties.parallelRequests,
        fair = true,
    )

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        var submissionLogged = false
        var interrupted = false
        var slotAcquired = false

        try {
            val windowWaitMs = deadline - now() - requestAverageProcessingTime.toMillis()
            val semaphoreWait = metrics.startSemaphoreWait()
            if (windowWaitMs <= 0 || !concurrentRequests.semaphore.tryAcquire(windowWaitMs, TimeUnit.MILLISECONDS)) {
                throw SocketTimeoutException("Payment deadline exceeded while waiting for account window")
            }
            slotAcquired = true
            metrics.recordSemaphoreAcquired(semaphoreWait)

            val rateWaitMs = deadline - now() - requestAverageProcessingTime.toMillis()
            if (rateWaitMs <= 0 || !rateLimiter.tickBlocking(Duration.ofMillis(rateWaitMs))) {
                throw SocketTimeoutException("Payment deadline exceeded while waiting for rate limit")
            }

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            submissionLogged = true
            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()
            val remainingMs = deadline - now()
            if (remainingMs <= 0) {
                throw SocketTimeoutException("Payment deadline exceeded")
            }

            val call = client.newCall(request)
            call.timeout().timeout(remainingMs, TimeUnit.MILLISECONDS)
            metrics.requestStarted()
            val body = try {
                call.execute().use { response ->
                    val responseBody = response.body?.string()
                    try {
                        mapper.readValue(responseBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseBody")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
                }
            } finally {
                metrics.requestFinished()
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
            metrics.responseReceived(body.result)

            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }
        } catch (e: Exception) {
            interrupted = e is InterruptedException
            if (!submissionLogged) {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            }

            when (e) {
                is SocketTimeoutException -> {
                    metrics.responseReceived(false)
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                is InterruptedException -> {
                    metrics.responseReceived(false)
                    logger.error("[$accountName] Payment interrupted for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "submission interrupted")
                    }
                }

                else -> {
                    metrics.responseReceived(false)
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            if (slotAcquired) concurrentRequests.semaphore.release()
            if (interrupted) Thread.currentThread().interrupt()
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