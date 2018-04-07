package datatrans
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import datatrans.Utils._

class Pivot(keycols : Seq[String], cols : Seq[String]) extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function.
  override def inputSchema: StructType =
    StructType(
      keycols.map(x=>StructField(x, StringType)) ++ cols.map(x=>StructField(x, StringType)))

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    Seq(
      StructField("keyvals", dataType)
    )
  )

  private def buildDataType(keycols : Seq[String]) : DataType = keycols match {
    case Seq() => MapType(StringType, StringType, valueContainsNull = false)
    case Seq(_, kcs@_*) => MapType(StringType, buildDataType(kcs), valueContainsNull = false)
  }    

  // This is the output type of your aggregatation function.
  override def dataType: DataType = buildDataType(keycols)

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Any]()
  }



  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val inputSeq = input.toSeq
    val keyvals = inputSeq.take(keycols.length).map(x=>x.toString)
    val vals = inputSeq.drop(keycols.length)
    val curr = buffer.getAs[Map[String, Any]](0)
    val fs = vals.zip(cols).filter({ case (v, _) => v != null }).map({case (v, col) => col -> v})
    val newrow = Map(fs:_*)

    buffer(0) = updateMap(curr, keyvals, newrow)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = mergeMap(buffer1.getAs[Map[String,Row]](0), buffer2.getAs[Map[String,Row]](0))
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Seq[Row]](0)
  }
}


