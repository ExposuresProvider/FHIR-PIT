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
  override def bufferSchema: StructType = StructType(Seq(
    StructField("indices", ArrayType(IntegerType, false)),
    StructField("elements", ArrayType(ArrayType(StringType, true), false))
  ))

  // This is the output type of your aggregatation function.
  override def dataType: DataType = bufferSchema

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new Array[Int](0)
    buffer(1) = new Array[Array[String]](0)
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val keyval = input.getString(0)
    val keyindex = keyvals.indexOf(keyval)
    val cols = input.toSeq.drop(1).map(x=>x.asInstanceOf[String])
    buffer(0) = buffer.getAs[Array[Int]](0) :+ keyindex
    buffer(1) = buffer.getAs[Array[Array[String]]](1) :+ cols
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Array[Int]](0) ++ buffer1.getAs[Array[Int]](0)
    buffer1(1) = buffer1.getAs[Array[Array[String]]](1) ++ buffer2.getAs[Array[Array[String]]](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    Row(
      buffer.getAs[Array[Int]](0),
      buffer.getAs[Array[Seq[String]]](1)
    )
  }
}


