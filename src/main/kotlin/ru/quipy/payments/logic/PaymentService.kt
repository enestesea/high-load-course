package ru.quipy.payments.logic

import ru.quipy.common.utils.CoroutineRateLimiter
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import java.time.Duration
import java.util.*

interface PaymentService {
    val rateLimiter : CoroutineRateLimiter
    val window: NonBlockingOngoingWindow
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long)
}

interface PaymentExternalService : PaymentService

/**
 * Describes properties of payment-provider accounts.
 */
data class ExternalServiceProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val request95thPercentileProcessingTime: Duration = Duration.ofSeconds(11)
)

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val result: Boolean,
    val message: String? = null,
)