package datatrans.step

import org.scalatest._
import Matchers._
import datatrans.step.PreprocFHIRResourceType._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.file.{Files, Paths, Path}
import java.nio.file.Files
import gnieh.diffson.circe._
import io.circe.parser._
import org.joda.time._
import org.joda.time.format._
import org.apache.commons.csv._

object TestUtils {
  
  def getFileTree(f: File): Stream[File] =
    if (f.isDirectory)
      f.listFiles().toStream.flatMap(getFileTree)
    else if (f.isFile)
      Seq(f).toStream
    else
      throw new RuntimeException(f"$f is neither a dir nor a file")

  def readFile(f: String): String = {
    val source = scala.io.Source.fromFile(f)
    try source.mkString finally source.close()
  }

  def deleteRecursively(file: Path): Unit = {
    if (Files.isDirectory(file)) {
      val stream = Files.newDirectoryStream(file)
      stream.iterator().asScala.foreach(deleteRecursively _)
      stream.close()
    } else {
      Files.delete(file)
    }
  }

  def readCSV(f : String, csv_schema : Map[String, String => Any]) : Seq[Map[String, Any]] = {
    val csvParser = new CSVParser(
      new InputStreamReader(new FileInputStream(f), "UTF-8"), CSVFormat.DEFAULT.withFirstRecordAsHeader())

    csvParser.getRecords().toSeq.map((x : CSVRecord) =>
      x.toMap().toMap.map{
        case (k, v) => csv_schema.get(k) match {
          case None => k -> v
          case Some(func) => k -> func(v)
        }
      }
    )
  }

  def showCSV(csv : Seq[Map[String, Any]]): String =
    csv.foldLeft("")((a, b) => f"$a\n$b")

  def compareCSV(output_path : String, expected_path : String, csv_schema : Map[String, String => Any]) : Unit = {
    val csv1 = readCSV(output_path, csv_schema)
    val csv2 = readCSV(expected_path, csv_schema)
    println("csv1 = " + showCSV(csv1))
    println("csv2 = " + showCSV(csv2))
    val n = Math.min(csv1.size, csv2.size)
    for(i <- 0 until n) {
      val strdiff = csv1(i).toSet diff csv2(i).toSet
      val strdiff2 = csv2(i).toSet diff csv1(i).toSet
      println("diff = " + strdiff)
      println("diff2 = " + strdiff2)
    }
    for(i <- n until csv1.size) {
      println("only = " + csv1(i))
    }
    for(i <- n until csv2.size) {
      println("only2 = " + csv2(i))
    }
    csv1 should equal (csv2)
  }

  def compareFileTree(src: String, tgt: String, csv: Boolean = false, csv_schema : Map[String, String => Any] = Map()) = {
    getFileTree(new File(src)).foreach(f => {
      println("comparing " + f.getPath())
      val expected_path = f.getPath()
      val output_path = expected_path.replace(src, tgt)
      println("output path " + output_path)
      val op = Paths.get(output_path)
      assert(Files.isRegularFile(op))
      val f1 = readFile(output_path)
      val f2 = readFile(expected_path)
      val json1 = parse(f1).right.getOrElse(null)
      val json2 = parse(f2).right.getOrElse(null)
      if (json1 != null && json2 != null) {
        println("json1 = " + json1)
        println("json2 = " + json2)
        val patch = JsonDiff.diff(json1, json2, true)
        println("diff = " + patch)
        assert(json1 == json2)
      } else if (csv || (output_path.endsWith(".csv") && expected_path.endsWith(".csv"))) {
        compareCSV(output_path, expected_path, csv_schema)
      } else {
        println("f1 = " + f1)
        println("f2 = " + f2)
        val strdiff = f1 diff f2
        val strdiff2 = f2 diff f1
        println("diff = " + strdiff)
        println("diff2 = " + strdiff2)
        assert(f1 == f2)
      }

    })

  }


}
