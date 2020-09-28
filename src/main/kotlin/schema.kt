import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Instant
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract
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

fun schema(init: SchemaBuilder.() -> Schema): Schema = SchemaBuilder().init()

data class Schema(val keyspaceName: String, var tableName: String, val fields: Collection<Field<*>>) {
    override fun toString(): String {
        return "Schema(keyspaceName='$keyspaceName', tableName='$tableName', fields=$fields)"
    }
}

sealed class SchemaBuilder(var _keyspaceName: String? = null, var tableName: String? = null) {
    val fields = mutableListOf<Field<*>>()
    
    interface Named

    private class Impl : SchemaBuilder(), Named

    companion object {
        // This function invocation looks like constructor invocation
        operator fun invoke(): SchemaBuilder = Impl()
    }

    fun fields(puppiesList: FieldList.() -> Unit) {
        fields.addAll(FieldList().apply(puppiesList))
    }

    //fun build(): Schema = Schema(keyspaceName!!, tableName!!, fields.toList())
}

// This method can be called only if the builder has been named
fun <S> S.build(): Schema where S : SchemaBuilder,S : SchemaBuilder.Named {
  return Schema(keyspaceName, tableName!!, fields.toList())  
} 

// Extension property for <PersonBuilder & Named>
val <S> S.keyspaceName where
        S : SchemaBuilder,
        S : SchemaBuilder.Named
    get() = _keyspaceName!!

@ExperimentalContracts
fun SchemaBuilder.keyspaceName(keyspaceName: String) {
    contract {
        returns() implies (this@keyspaceName is SchemaBuilder.Named)
    }
    _keyspaceName = keyspaceName
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


@ExperimentalContracts
val test = schema {
    this.keyspaceName("keyspace 1")
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
    build()
}

@ExperimentalContracts
fun main() {
    println(test)
}