import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class Pivot(keycol : String, keyvals : Seq[String], cols : Seq[String]) extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      StructField(keycol, StringType) +: cols.map(x=>StructField(x, StringType)))

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    Seq(
      StructField("keyvals", dataType)
    )
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = ArrayType(
    StructType(
      Seq(
        StructField("index", IntegerType),
        StructField("element", ArrayType(StringType, true))
      )
    ), false
  )

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq[Row]()
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val keyval = input.getString(0)
    val keyindex = keyvals.indexOf(keyval)
    val cols = input.toSeq.drop(1).map(x=>x.asInstanceOf[String])
    buffer(0) = buffer.getAs[Seq[Row]](0) :+ Row(keyindex, cols)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Seq[Row]](0) ++ buffer1.getAs[Seq[Row]](0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Seq[Row]](0)
  }
}


