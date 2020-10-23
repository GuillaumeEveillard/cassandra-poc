import java.sql.Timestamp
import java.time.Instant
import kotlin.random.Random

interface Generator<T> {
    fun produce(partitionNumber: Long, partitionTotal: Long, elementNumber: Long, elementTotal: Long) : T
}

val randomLong = object : Generator<Long> {
    override fun produce(partitionNumber: Long, partitionTotal: Long, elementNumber: Long, elementTotal: Long): Long {
        return Random(0).nextLong()
    }
}

fun timestampsPerDay(differentDay: Long, starting: Timestamp) : Generator<Timestamp> {
    return object : Generator<Timestamp> {
        override fun produce(
            partitionNumber: Long,
            partitionTotal: Long,
            elementNumber: Long,
            elementTotal: Long
        ): Timestamp {
            val elementsPerDay = elementTotal / differentDay
            val day = elementNumber / elementsPerDay
            return Timestamp.from(Instant.now())
        }

    }
}