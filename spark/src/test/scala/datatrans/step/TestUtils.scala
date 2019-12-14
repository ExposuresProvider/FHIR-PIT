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
    else
      Seq(f).toStream

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

  def readCSV(f : String) : Seq[Map[String, String]] = {
    val csvParser = new CSVParser(
      new InputStreamReader(new FileInputStream(f), "UTF-8"), CSVFormat.DEFAULT.withFirstRecordAsHeader())

    Seq(csvParser.getRecords(): _*).map(_.toMap().toMap)
  }

  def compareFileTree(src: String, tgt: String) = {
    getFileTree(new File(src)).foreach(f => {
      println("comparing " + f.getPath())
      val expected_path = f.getPath()
      val output_path = expected_path.replace(src, tgt)
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
      } else if (output_path.endsWith(".csv") && expected_path.endsWith(".csv")) {
        val csv1 = readCSV(output_path)
        val csv2 = readCSV(expected_path)
        println("csv1 = " + csv1)
        println("csv2 = " + csv2)
        val strdiff = csv2 diff csv2
        println("diff = " + strdiff)
        csv1 should equal (csv2)
      } else {
        println("f1 = " + f1)
        println("f2 = " + f2)
        val strdiff = f1 diff f2
        println("diff = " + strdiff)
        assert(f1 == f2)
      }

    })

  }


}
