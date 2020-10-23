import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.math.BigDecimal

internal class SchemaTest {
    @Test
    fun `build schema using dsl`() {
        val s = schema {
            keyspaceName = "keyspace 1"
            tableName = "table 1"
            fields {
                field<Long> {
                    fieldName = "toto"
                    type = Bigint
                }
                field<BigDecimal> {
                    fieldName = "tata"
                    type = Decimal
                }
            }
        }
        
        Assertions.assertEquals(
            Schema("keyspace 1", "table 1", listOf(
                Field("toto", Bigint),
                Field("tata", Decimal))),
            s)
    }

    @Test
    fun `multiple fields at once`() {
        val s = schema {
            keyspaceName = "keyspace 1"
            tableName = "table 1"
            fields {
                multipleFields<Long>(2) {
                    fieldName = "f"
                    type = Bigint
                }
            }
        }

        Assertions.assertEquals(
            Schema("keyspace 1", "table 1", listOf(
                Field("f-1", Bigint),
                Field("f-2", Bigint))),
            s)
    }
}