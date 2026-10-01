package ru.quipy.common.utils

import java.util.concurrent.Semaphore

class AutoCloseableSemaphore(
    permits: Int,
    fair: Boolean = true,
) {
    val semaphore = Semaphore(permits, fair)

    inline fun <T> withPermit(block: () -> T): T {
        semaphore.acquire()

        return try {
            block()
        } finally {
            semaphore.release()
        }
    }
}