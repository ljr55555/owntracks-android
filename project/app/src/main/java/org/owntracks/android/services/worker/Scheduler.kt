package org.owntracks.android.services.worker

import android.content.Context
import android.os.SystemClock
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequest
import androidx.work.PeriodicWorkRequest
import androidx.work.WorkManager
import androidx.work.WorkRequest
import dagger.hilt.android.qualifiers.ApplicationContext
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import org.owntracks.android.preferences.Preferences
import timber.log.Timber

@Singleton
class Scheduler
@Inject
constructor(
    private val preferences: Preferences,
    @param:ApplicationContext private val context: Context
) : Preferences.OnPreferenceChangeListener {
  init {
    preferences.registerOnPreferenceChangedListener(this)
  }

  private val anyNetworkConstraint =
      Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build()
  private val workManager = WorkManager.getInstance(context)

  /** Used by the background service to periodically ping a location */
  fun scheduleLocationPing() {
    val pingWorkRequest: WorkRequest =
        PeriodicWorkRequest.Builder(
                SendLocationPingWorker::class.java, preferences.ping.toLong(), TimeUnit.MINUTES)
            .addTag(PERIODIC_TASK_SEND_LOCATION_PING)
            .setConstraints(anyNetworkConstraint)
            .build()
    Timber.d(
        "WorkManager queue task $PERIODIC_TASK_SEND_LOCATION_PING as ${pingWorkRequest.id} " +
            "with interval ${preferences.ping} minutes")
    workManager.cancelAllWorkByTag(PERIODIC_TASK_SEND_LOCATION_PING)
    workManager.enqueue(pingWorkRequest)
  }

  /** Cancels all WorkManager tasks. Called on app exit */
  fun cancelAllTasks() {
    Timber.d("Cancelling task tag (all mqtt tasks) $ONETIME_TASK_MQTT_RECONNECT")
    workManager.cancelUniqueWork(ONETIME_TASK_MQTT_RECONNECT)
    workManager.cancelUniqueWork(PERIODIC_TASK_MQTT_CONNECTION_WATCHDOG)
    workManager.cancelAllWorkByTag(PERIODIC_TASK_SEND_LOCATION_PING)
    resetMqttReconnectBackoff()
  }

  /**
   * Starts the periodic check that the MQTT connection is still alive, reconnecting it if not.
   *
   * Every other trigger for a reconnect is reactive — a connectivity callback, a dropped
   * connection, a failed publish — so anything they collectively fail to notice is never noticed at
   * all. The most important such case is a connection that is dead but still believed to be up,
   * which produces no event of any kind.
   *
   * Enqueued with [ExistingPeriodicWorkPolicy.KEEP] so that repeatedly re-activating the endpoint
   * cannot keep pushing the next run into the future and starve the check entirely.
   */
  fun scheduleMqttConnectionWatchdog() {
    PeriodicWorkRequest.Builder(
            MQTTConnectionWatchdogWorker::class.java,
            CONNECTION_WATCHDOG_INTERVAL.inWholeMinutes,
            TimeUnit.MINUTES)
        .addTag(PERIODIC_TASK_MQTT_CONNECTION_WATCHDOG)
        .setConstraints(anyNetworkConstraint)
        .build()
        .run {
          workManager.enqueueUniquePeriodicWork(
              PERIODIC_TASK_MQTT_CONNECTION_WATCHDOG, ExistingPeriodicWorkPolicy.KEEP, this)
        }
    Timber.i(
        "Scheduled $PERIODIC_TASK_MQTT_CONNECTION_WATCHDOG every $CONNECTION_WATCHDOG_INTERVAL")
  }

  /** How many consecutive reconnect attempts have been scheduled without an intervening success. */
  private val reconnectAttempt = AtomicInteger(0)

  /**
   * When the currently-pending reconnect attempt is due to run, on the elapsed-realtime clock, or
   * [NO_RECONNECT_PENDING] when nothing is pending.
   */
  private val reconnectDueAt = AtomicLong(NO_RECONNECT_PENDING)

  /**
   * Schedules an attempt to reconnect to the MQTT broker.
   *
   * Successive attempts back off exponentially, but never further apart than
   * [RECONNECT_MAX_DELAY], so a broker that has been unreachable for a long time is still retried
   * promptly once it comes back.
   *
   * The backoff is computed here rather than handed to WorkManager via [BackoffPolicy]: WorkManager
   * clamps its own backoff to [WorkRequest.MAX_BACKOFF_MILLIS], which is five hours, and a run of
   * failures reaches that in well under a day. Combined with Doze deferral on a device that isn't
   * exempt from battery optimisation, retries at that spacing are effectively unbounded and the app
   * never recovers on its own.
   *
   * Safe to call from anything that notices the connection is down, however often it fires: an
   * attempt that is already pending and due sooner is left alone. Without that, a flapping network
   * or a queue of failing publishes would keep REPLACEing the pending attempt with an ever-later
   * one and starve the reconnect completely.
   */
  fun scheduleMqttReconnect() {
    val now = SystemClock.elapsedRealtime()
    reconnectDueAt.get().let { dueAt ->
      if (dueAt != NO_RECONNECT_PENDING && now < dueAt) {
        Timber.v(
            "MQTT reconnect already pending in ${(dueAt - now).milliseconds}, not rescheduling")
        return
      }
    }
    val delay = reconnectDelayForAttempt(reconnectAttempt.getAndIncrement())
    reconnectDueAt.set(now + delay.inWholeMilliseconds)
    OneTimeWorkRequest.Builder(MQTTReconnectWorker::class.java)
        // Pause in case there's network turmoil
        .setInitialDelay(delay.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        .addTag(ONETIME_TASK_MQTT_RECONNECT)
        .setConstraints(anyNetworkConstraint)
        .build()
        .run {
          workManager.enqueueUniqueWork(
              ONETIME_TASK_MQTT_RECONNECT, ExistingWorkPolicy.REPLACE, this)
        }
    // Logged at INFO: when this goes wrong the connection is dead for hours, and at DEBUG the
    // evidence has long since rolled out of the in-memory log buffer by the time anyone looks.
    Timber.i("Scheduled ONETIME_TASK_MQTT_RECONNECT job in $delay")
  }

  /**
   * Puts the reconnect backoff back to its shortest delay. Called when a connection is established,
   * so that the next disconnection is retried promptly rather than at whatever spacing the previous
   * run of failures had reached.
   */
  fun resetMqttReconnectBackoff() {
    reconnectDueAt.set(NO_RECONNECT_PENDING)
    // A reconnect left pending from the run of failures would otherwise fire and tear down the
    // connection that has just been established.
    workManager.cancelUniqueWork(ONETIME_TASK_MQTT_RECONNECT)
    reconnectAttempt.getAndSet(0).run {
      if (this > 0) Timber.d("Reset MQTT reconnect backoff after $this attempts")
    }
  }

  companion object {
    private const val PERIODIC_TASK_SEND_LOCATION_PING = "PERIODIC_TASK_SEND_LOCATION_PING"
    private const val ONETIME_TASK_MQTT_RECONNECT = "ONETIME_TASK_MQTT_RECONNECT"
    private const val PERIODIC_TASK_MQTT_CONNECTION_WATCHDOG =
        "PERIODIC_TASK_MQTT_CONNECTION_WATCHDOG"

    /**
     * How often to verify the connection. WorkManager will not run periodic work more frequently
     * than [PeriodicWorkRequest.MIN_PERIODIC_INTERVAL_MILLIS], which is fifteen minutes, so asking
     * for less would achieve nothing.
     */
    internal val CONNECTION_WATCHDOG_INTERVAL = 15.minutes

    /** Delay before the first reconnect attempt of a run. */
    internal val RECONNECT_INITIAL_DELAY = 10.seconds

    /** Ceiling on the gap between reconnect attempts, however long the failure has persisted. */
    internal val RECONNECT_MAX_DELAY = 10.minutes

    /**
     * Exponential backoff from [RECONNECT_INITIAL_DELAY], capped at [RECONNECT_MAX_DELAY].
     *
     * @param attempt zero-based count of attempts already scheduled in this run of failures
     */
    internal fun reconnectDelayForAttempt(attempt: Int): Duration {
      // Clamp the shift as well as the result: Int.shl only uses the low five bits of its operand,
      // so a large attempt count would otherwise wrap around to a small — or negative — multiplier.
      val doublings = attempt.coerceIn(0, MAX_BACKOFF_DOUBLINGS)
      return (RECONNECT_INITIAL_DELAY * (1 shl doublings)).coerceAtMost(RECONNECT_MAX_DELAY)
    }

    /** Enough doublings to comfortably exceed [RECONNECT_MAX_DELAY] without overflowing the shift. */
    private const val MAX_BACKOFF_DOUBLINGS = 16

    private const val NO_RECONNECT_PENDING = Long.MIN_VALUE
  }

  override fun onPreferenceChanged(properties: Set<String>) {
    if (properties.contains(Preferences::ping.name)) {
      workManager.cancelAllWorkByTag(PERIODIC_TASK_SEND_LOCATION_PING)
      scheduleLocationPing()
    }
  }
}
