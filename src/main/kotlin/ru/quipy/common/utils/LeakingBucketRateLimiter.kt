package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    val windowMillis: Long
        get() = window.toMillis()

    init {
        require(rate > 0) { "Rate must be positive" }
        require(!window.isZero && !window.isNegative) { "Window must be positive" }
    }

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Int>()

    @Synchronized
    override fun tick(): Boolean {
        return tick(queue.remainingCapacity() + queue.size)
    }

    @Synchronized
    fun tick(maxSize: Int): Boolean {
        require(maxSize > 0) { "Bucket size must be positive" }
        return if (queue.size >= maxSize) false else queue.offer(1)
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay(window.toMillis())
            repeat(rate.toInt()) {
                queue.poll()
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}