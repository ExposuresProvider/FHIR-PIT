package datatrans.step

import org.scalatest._
import Matchers._
import datatrans.step.PreprocFHIRResourceType._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
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

  def toMap[A](a: Seq[A]) : Map[A, Integer] =
    a.foldLeft(Map[A, Integer]())((b: Map[A, Integer], c: A) =>  b + (c -> ((b.getOrElse(c, 0) : Integer) + 1)))

  def compareCSV(output_path : String, expected_path : String, csv_schema : Map[String, String => Any]) : Unit = {
    val txt1 = readFile(output_path)
    val txt2 = readFile(expected_path)
    val csv1 = readCSV(output_path, csv_schema)
    val csv2 = readCSV(expected_path, csv_schema)
    println("csv1 = " + showCSV(csv1))
    println("csv2 = " + showCSV(csv2))
    println("txt1 = " + txt1)
    println("txt2 = " + txt2)
    def diff[A](a : Set[A], b : Set[A]) =
      (a diff b, b diff a)

    val n = Math.min(csv1.size, csv2.size)
    val csv2remaining = new ListBuffer[Map[String,Any]]()
    csv2remaining.addAll(csv2)
    for(i <- 0 until n) {
      val a = csv1(i).toSet
      val closest = csv2remaining.minBy(bm => {
        val b = bm.toSet
        val (ab, ba) = diff(a,b)
        ab.size + ba.size
      })
      val (ab, ba) = diff(a, closest.toSet)
      csv2remaining -= closest
      println("a = " + a)
      println("a2 = " + closest)
      println("diff = " + ab)
      println("diff2 = " + ba)
    }
    for(i <- n until csv1.size) {
      println("only = " + csv1(i))
    }
    for(b <- csv2remaining) {
      println("only2 = " + b)
    }
    toMap(csv1) should equal (toMap(csv2))
  }

  def printDirectoryTree( folder : File, indent : Integer, sb:StringBuilder) : Unit = {
    if (!folder.isDirectory()) {
      throw new IllegalArgumentException("folder is not a Directory")
    }
    sb.append(getIndentString(indent))
    sb.append("+--")
    sb.append(folder.getName())
    sb.append("/")
    sb.append("\n")
    for (file <- folder.listFiles()) {
      if (file.isDirectory()) {
        printDirectoryTree(file, indent + 1, sb)
      } else {
        printFile(file, indent + 1, sb)
      }
    }
  }

  def printFile(file : File, indent : Integer, sb : StringBuilder) : Unit = {
    sb.append(getIndentString(indent))
    sb.append("+--")
    sb.append(file.getName())
    sb.append("\n")
  }

  def getIndentString(indent: Integer) : String = {
    val sb = new StringBuilder()
    for (i <- 0 until indent) {
      sb.append("|  ")
    }
    sb.toString()
  }

  def compareFileTree(src: String, tgt: String, csv: Boolean = false, csv_schema : Map[String, String => Any] = Map()) = {
    val sb = new StringBuilder()
    printDirectoryTree(new File(tgt), 0, sb)
    println(s"output directory: ${sb.toString()}")
    val sb2 = new StringBuilder()
    printDirectoryTree(new File(src), 0, sb2)
    println(s"expected directory: ${sb2.toString()}")
    getFileTree(new File(src)).foreach(f => {
      val expected_path = f.getPath()
      val output_path = expected_path.replace(src, tgt)
      println("output path: " + output_path)
      println("expected path: " + f.getPath())
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
    getFileTree(new File(tgt)).foreach(f => {
      println(s"checking ${f.getPath}")
      val output_path = f.getPath()
      val expected_path = output_path.replace(tgt, src)
      println(s"expected path $expected_path")
      val ip = Paths.get(expected_path)
      assert(Files.isRegularFile(ip))
    })

  }


}
