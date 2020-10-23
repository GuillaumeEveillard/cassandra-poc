import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Instant
import kotlin.random.Random


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
object Bigint : Type<Long>()
object Decimal : Type<BigDecimal>()
