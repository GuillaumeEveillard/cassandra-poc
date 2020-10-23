import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.Cluster
import java.math.BigDecimal
import java.sql.Timestamp
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

/**
 * Sandbox. The idea is to replace this code by something generic which uses the schema builder and the data generators
 */

const val CREATE_SPACE = "create keyspace space1 with replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"
const val CREATE_TABLE = """create table space1.table1
(
    id bigint,
    chunk bigint,
    timestamp timestamp,
    int1 bigint,
    int2 bigint,
    int3 bigint,
    int4 bigint,
    int5 bigint,
    deci1 decimal, 
    deci2 decimal,
    deci3 decimal, 
    deci4 decimal, 
    deci5 decimal,
    PRIMARY KEY((chunk), id)  
)"""
const val INSERT_QUERY = "INSERT INTO space1.table1 (id, chunk, timestamp, int1, int2, int3, int4, int5, deci1, deci2, deci3, deci4, deci5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
const val CHUNK_SIZE = 10000L

fun main() {
    val cluster = Cluster.builder().addContactPoint("localhost").withPort(9042).build();

    deleteDatamodel(cluster)
    createDatamodel(cluster)

    val maxIds = maxId(cluster)
    insertParallel(cluster, maxIds.next(), chunkByThread=50,threadNumber=2)

    // read(cluster)
    cluster.close()
    println("cluster closed")

//    Thread.sleep(30000)

}

fun createDatamodel(cluster: Cluster) {
    val session = cluster.connect();
    session.execute(CREATE_SPACE)
    session.execute(CREATE_TABLE)
    session.close()
}

fun deleteDatamodel(cluster: Cluster) {
    val session = cluster.connect();
    session.execute("DROP KEYSPACE IF EXISTS space1")
    session.close()
}

private fun maxId(cluster: Cluster): CurrentIds {
    val session = cluster.connect();
    val rs = session.execute("select max(id), max(chunk) from space1.table1")
    val row = rs.one()
    val maxId = row.getLong(0)
    val maxChunk = row.getLong(1)
    println("Max id is $maxId, Max chunk is $maxChunk")
    session.close()
    return CurrentIds(maxId, maxChunk)
}

data class CurrentIds(val id: Long, val chunk: Long) {
    fun next() = CurrentIds(id+1, chunk+1)
    fun next(idOffset: Long, chunkOffset: Long) = CurrentIds(id+idOffset, chunk+chunkOffset)
    override fun toString() = "CurrentIds(id=$id, chunk=$chunk)"
}

private fun read(cluster: Cluster) {
    val session = cluster.connect();
    val rs = session.execute("select amount from space1.table1 where chunk=7000")
    var total = 0.0
    var n = 0
    val start = System.currentTimeMillis()
    for (rowN in rs) {
        total += rowN.getFloat("amount")
        n++
    }
    val end = System.currentTimeMillis()
    val ms = end - start
    println("$n - total: $total in $ms ms")

    session.close()
}

private fun insertParallel(cluster: Cluster, ids: CurrentIds, chunkByThread: Long = 25, threadNumber: Int = 4) {
    println("We are going to insert "+threadNumber*chunkByThread*CHUNK_SIZE+ " elements")
    val start = System.currentTimeMillis()
    val exec = Executors.newFixedThreadPool(threadNumber)
    for (t in 0..threadNumber) {
        exec.submit { insert(cluster, ids.next(t * chunkByThread * CHUNK_SIZE, t * chunkByThread), chunkByThread, CHUNK_SIZE) }
    }
    exec.shutdown()
    exec.awaitTermination(10, TimeUnit.MINUTES)
    val end = System.currentTimeMillis()
    val ms = end - start

    val inserted = chunkByThread * threadNumber * 10000
    val insertedBySeconds = inserted / (ms / 1000)

    println(" ==> $inserted elements inserted in $ms ms using $threadNumber threads ($insertedBySeconds by seconds)")
}

private fun insert(cluster: Cluster, ids: CurrentIds, chunkByThread: Long, chunkSize: Long) {
    println("Starting $ids")
    try {
        val session = cluster.connect();
        val prepared = session.prepare(INSERT_QUERY)
        val random = ThreadLocalRandom.current()

        for (c in 0 until chunkByThread) {
            val chunk = ids.chunk + c
            val batch = BatchStatement(BatchStatement.Type.UNLOGGED)
            for (i in 0 until chunkSize) {
                val id = ids.id + c*chunkSize + i
                val timestamp = Timestamp(random.nextLong())

               // println("chunk $chunk -- id $id")
                batch.add(prepared.bind(id, chunk, timestamp,
                    random.nextLong(10000), random.nextLong(10000), random.nextLong(10000), random.nextLong(10000), random.nextLong(10000),
                    random.nextBigDecimal(100000, 4), random.nextBigDecimal(100000, 4), random.nextBigDecimal(100000, 4), random.nextBigDecimal(100000, 4), random.nextBigDecimal(100000, 4))
                )
            }
            session.execute(batch)
        }

        session.close()
        println("Session closed $ids")

    } catch (e: Exception) {
        e.printStackTrace()
    }
}

private fun ThreadLocalRandom.nextBigDecimal(bound: Long, decimal: Int) : BigDecimal {
    return BigDecimal(this.nextLong(bound + decimal*10)).movePointLeft(decimal)
}