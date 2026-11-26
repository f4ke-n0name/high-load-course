package ru.quipy.payments.logic

import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration

data class PaymentTimeoutPolicy(
    val serverMillis: Long,
    val callMillis: Long,
    val maxAttempts: Int = 4,
    val resultWriteReserveMillis: Long = 200,
) {
    init {
        require(serverMillis > 0) { "Provider server timeout must be positive" }
        require(callMillis > serverMillis) { "Client call timeout must exceed provider server timeout" }
        require(maxAttempts > 0) { "Max attempts must be positive" }
        require(resultWriteReserveMillis >= 0) { "Result write reserve must be non-negative" }
    }

    val providerTimeout: String = Duration.ofMillis(serverMillis).toString()

    fun latestStart(deadline: Long): Long = deadline - callMillis - resultWriteReserveMillis
}

internal data class PaymentProviderResponse(val body: ExternalSysResponse, val status: Int) {
    val isTimeout: Boolean
        get() = status == 408 && !body.result && body.message == "Timeout"
}

internal fun isPaymentRequestTimeout(error: Exception): Boolean =
    error is SocketTimeoutException || (error is InterruptedIOException && error.message == "timeout")
