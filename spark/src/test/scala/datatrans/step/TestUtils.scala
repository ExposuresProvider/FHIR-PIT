package datatrans.step

import org.scalatest._
import Matchers._
import datatrans.step.PreprocFHIRResourceType._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import java.io.{File, FileInputStream, InputStreamReader, OutputStreamWriter, FileOutputStream}
import java.nio.file.{Files, Paths, Path}
import java.nio.file.Files
import diffson._
import diffson.lcs._
import diffson.circe._
import diffson.jsonpatch._
import diffson.jsonpatch.lcsdiff._

import io.circe._
import io.circe.parser._
import org.joda.time._
import org.joda.time.format._
import org.apache.commons.csv._

import cats._
import cats.implicits._


object Tabulator {
  def format(table: Seq[Seq[Any]]) = table match {
    case Seq() => ""
    case _ => 
      val sizes = for (row <- table) yield (for (cell <- row) yield if (cell == null) 0 else cell.toString.length)
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator :: 
    rows.head :: 
    rowSeparator :: 
    rows.tail.toList ::: 
    rowSeparator :: 
    List()).mkString("\n")

  def formatRow(row: Seq[Any], colSizes: Seq[Int]) = {
    val cells = (for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%" + size + "s").format(item))
    cells.mkString("|", "|", "|")
  }

  def rowSeparator(colSizes: Seq[Int]) = colSizes map { "-" * _ } mkString("+", "+", "+")
}


object TestUtils {

  implicit val lcs = new Patience[Json]

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

  def readCSV(f : String, csv_schema : Map[String, String => Any]) : (Seq[String], Seq[Map[String, Any]]) = {
    val csvParser = new CSVParser(
      new InputStreamReader(new FileInputStream(f), "UTF-8"), CSVFormat.DEFAULT.withFirstRecordAsHeader())

    try {

      (csvParser.getHeaderNames().asScala, csvParser.getRecords().asScala.toSeq.map((x : CSVRecord) =>
        x.toMap().asScala.toMap.map{
          case (k, v) => csv_schema.get(k) match {
            case None => k -> v
            case Some(func) => k -> func(v)
          }
        }
      ))
    } finally {

      csvParser.close()
    }
  }

  def writeCSV(f : String, headers: Seq[String], table: Seq[Seq[Any]]) : Unit = {
    val csvPrinter = new CSVPrinter(
      new OutputStreamWriter(new FileOutputStream(f), "UTF-8"), CSVFormat.DEFAULT.withRecordSeparator("\n"))
    try {
      println(headers)
      csvPrinter.printRecord(headers.asJava)
      for(r <- table) {
        val rNoneToEmptyString = r.map(_ match {
          case None => ""
          case Some(s) => s
          case t => t
        })
        println(rNoneToEmptyString)
        csvPrinter.printRecord(rNoneToEmptyString.asJava)
      }
    } finally {
      csvPrinter.close()
    }
  }

  def showCSV(headers: Seq[String], csv : Seq[Map[String, Any]]): String = {
    val table = headers +: csv.map(row => headers.map(row))
    Tabulator.format(table)
  }

  def toMap[A](a: Seq[A]) : Map[A, Integer] =
    a.foldLeft(Map[A, Integer]())((b: Map[A, Integer], c: A) =>  b + (c -> ((b.getOrElse(c, 0) : Integer) + 1)))

  def compareCSV(output_path : String, expected_path : String, csv_schema : Map[String, String => Any]) : Unit = {
    val txt1 = readFile(output_path)
    val txt2 = readFile(expected_path)
    val (headers1raw, csv1) = readCSV(output_path, csv_schema)
    val (headers2raw, csv2) = readCSV(expected_path, csv_schema)
    val headersCommon = headers1raw.intersect(headers2raw)
    val headers1 = headersCommon ++ (headers1raw.diff(headersCommon))
    val headers2 = headersCommon ++ (headers2raw.diff(headersCommon))
    println("csv1 =\n" + showCSV(headers1, csv1))
    println("csv2 =\n" + showCSV(headers2, csv2))
    println("txt1 =\n" + txt1)
    println("txt2 =\n" + txt2)
    def diff[A](a : Set[A], b : Set[A]) =
      (a diff b, b diff a)

    val n = Math.min(csv1.size, csv2.size)
    val csv2remaining = new ListBuffer[Map[String,Any]]()
    csv2remaining ++= csv2
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

  def compareFileTree(tgt: String, src: String, csv: Boolean = false, csv_schema : Map[String, String => Any] = Map()) = {
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
        val patch = diff(json1, json2)
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
