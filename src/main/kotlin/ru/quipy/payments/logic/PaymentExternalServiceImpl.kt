package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Call
import okhttp3.EventListener
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.AutoCloseableSemaphore
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
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
    private val timeoutPolicy: PaymentTimeoutPolicy,
    private val client: OkHttpClient = defaultClient(),
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody()
        val mapper = ObjectMapper().registerKotlinModule()
        private class Transmission {
            @Volatile
            var headersStarted = false
        }

        private fun defaultClient() = OkHttpClient.Builder()
            .retryOnConnectionFailure(false)
            .followRedirects(false)
            .followSslRedirects(false)
            .readTimeout(Duration.ZERO)
            .eventListener(object : EventListener() {
                override fun requestHeadersStart(call: Call) {
                    call.request().tag(Transmission::class.java)?.headersStarted = true
                }
            })
            .build()
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
        val latestStart = timeoutPolicy.latestStart(deadline)
        for (attempt in 0 until timeoutPolicy.maxAttempts) {
            if (Thread.currentThread().isInterrupted || (attempt > 0 && now() >= latestStart)) return
            val startedAt = if (attempt == 0) paymentStartedAt else now()
            if (!performAttempt(paymentId, amount, startedAt, latestStart, attempt > 0)) return
        }
    }

    private fun performAttempt(
        paymentId: UUID,
        amount: Int,
        startedAt: Long,
        latestStart: Long,
        isRetry: Boolean,
    ): Boolean {
        val transactionId = UUID.randomUUID()
        val submittedAt = now()
        paymentESService.update(paymentId) {
            it.logSubmission(
                submittedAt < latestStart,
                transactionId,
                submittedAt,
                Duration.ofMillis((submittedAt - startedAt).coerceAtLeast(0)),
            )
        }

        val transmission = Transmission()
        val response = try {
            callProvider(paymentId, transactionId, amount, latestStart, isRetry, transmission)
        } catch (e: Exception) {
            val timedOut = isPaymentRequestTimeout(e)
            val reason = when (e) {
                is InterruptedException -> "Payment submission interrupted."
                else -> when {
                    timedOut && transmission.headersStarted -> "Request timeout; payment result unknown."
                    timedOut -> "Request timeout before sending headers."
                    else -> "Payment HTTP error: ${e.javaClass.simpleName}."
                }
            }
            logger.warn(
                "[{}] Payment call failed: payment={}, txId={}, reason={}",
                accountName, paymentId, transactionId, reason,
            )
            try {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
            } finally {
                if (e is InterruptedException) Thread.currentThread().interrupt()
            }
            return timedOut && !transmission.headersStarted && !Thread.currentThread().isInterrupted
        }

        if (response == null) {
            logger.warn(
                "[{}] Payment deadline exceeded before the provider call: payment={}, txId={}",
                accountName, paymentId, transactionId,
            )
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "deadline_exceeded")
            }
            return false
        }

        val body = response.body
        val success = body.result
        metrics.responseReceived(success)
        val reason = body.message
        if (success) {
            logger.info(
                "[{}] Payment succeeded: payment={}, txId={}",
                accountName, paymentId, transactionId,
            )
        } else {
            logger.warn(
                "[{}] Payment failed: payment={}, txId={}, reason={}",
                accountName, paymentId, transactionId, reason,
            )
        }
        paymentESService.update(paymentId) {
            it.logProcessing(success, now(), transactionId, reason = reason)
        }
        if (response.isTimeout) return true
        return !success && reason == "Temporary error"
    }

    private fun callProvider(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        latestStart: Long,
        isRetry: Boolean,
        transmission: Transmission,
    ): PaymentProviderResponse? {
        val semaphoreWait = metrics.startSemaphoreWait()
        val remainingMillis = latestStart - now()
        if (remainingMillis <= 0 || !concurrentRequests.semaphore.tryAcquire(remainingMillis, TimeUnit.MILLISECONDS)) {
            return null
        }

        try {
            metrics.recordSemaphoreAcquired(semaphoreWait)
            val url = "http://$paymentProviderHostPort/external/process".toHttpUrl().newBuilder()
                .addQueryParameter("serviceName", serviceName)
                .addQueryParameter("token", token)
                .addQueryParameter("accountName", accountName)
                .addQueryParameter("transactionId", transactionId.toString())
                .addQueryParameter("paymentId", paymentId.toString())
                .addQueryParameter("amount", amount.toString())
                .addQueryParameter("timeout", timeoutPolicy.providerTimeout)
            val request = Request.Builder()
                .url(url.build())
                .post(emptyBody)
                .tag(Transmission::class.java, transmission)
                .build()

            if (!rateLimiter.tryTickBefore(latestStart)) return null
            if (now() >= latestStart || Thread.currentThread().isInterrupted) return null

            val call = client.newCall(request)
            call.timeout().timeout(timeoutPolicy.callMillis, TimeUnit.MILLISECONDS)

            val attemptStartedAt = metrics.startHttpAttempt(isRetry)
            var outcome = "error"
            try {
                return call.execute().use { httpResponse ->
                    val body = mapper.readValue(httpResponse.body?.string(), ExternalSysResponse::class.java)
                    check(body.paymentId == paymentId.toString() && body.transactionId == transactionId.toString()) {
                        "Unmatched payment response"
                    }
                    check(httpResponse.isSuccessful || !body.result) { "Inconsistent payment response" }

                    val providerResponse = PaymentProviderResponse(body, httpResponse.code)
                    outcome = when {
                        providerResponse.isTimeout -> "provider_timeout"
                        body.result -> "success"
                        else -> "failure"
                    }
                    providerResponse
                }
            } catch (e: Exception) {
                outcome = if (isPaymentRequestTimeout(e)) "client_timeout" else "error"
                throw e
            } finally {
                metrics.finishHttpAttempt(attemptStartedAt, outcome)
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
