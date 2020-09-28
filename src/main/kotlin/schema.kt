import java.math.BigDecimal
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
            return Timestamp.from(Instant.now())
        }

    }
}


fun schema(lambda: SchemaBuilder.() -> Unit): Schema = SchemaBuilder().apply(lambda).build()

data class Schema(val keyspaceName: String, var tableName: String, val fields: Collection<Field<*>>) {
    override fun toString(): String {
        return "Schema(keyspaceName='$keyspaceName', tableName='$tableName', fields=$fields)"
    }
}

data class SchemaBuilder(var keyspaceName: String? = null, var tableName: String? = null) {
    private val fields = mutableListOf<Field<*>>()

    fun fields(puppiesList: FieldList.() -> Unit) {
        fields.addAll(FieldList().apply(puppiesList))
    }

    fun build(): Schema = Schema(keyspaceName!!, tableName!!, fields.toList())
}

class FieldList : ArrayList<Field<*>>() {
    fun <T> field(fieldLambda: Field<T>.() -> Unit) {
        add(Field<T>().apply(fieldLambda))
    }
    fun <T> multipleFields(size: Int, fieldLambda: Field<T>.() -> Unit) {
        for (i in 1..size) {
            add(Field<T>().apply(fieldLambda).apply { fieldName = "$fieldName-$i" })
        }
    }
    
}


data class Field<T>(var fieldName: String? = null, var type: Type<T>? = null, var valueGenerator: Generator<T>? = null) {
    override fun toString(): String {
        return "Field(fieldName=$fieldName, type=$type, valueGenerator=$valueGenerator)"
    }
}

sealed class Type<T>
class Bigint : Type<Long>()
class Decimal : Type<BigDecimal>()


val test = schema {
    keyspaceName = "keyspace 1"
    tableName = "table 1"
    fields {
        field<Long> {
            fieldName = "toto"
            type = Bigint()
            valueGenerator = randomLong
        }
        field<Long> {
            fieldName = "tata"
            type = Bigint()
            valueGenerator = randomLong
        }
        multipleFields<Long>(2) {
            fieldName = "auto"
            type = Bigint()
            valueGenerator = randomLong
        }
    }
}

fun main() {
    println(test)
}