package datatrans

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path, PathFilter }
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scala.collection.mutable.ListBuffer
import scopt._
import java.util.Base64
import java.nio.charset.StandardCharsets
import datatrans.Config._
import net.jcazevedo.moultingyaml._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import scala.collection.mutable.{Set, Queue}
import scala.util.control._
import Breaks._
import java.io.{StringWriter, PrintWriter}

import datatrans.Config._

import StepYamlProtocol._

object PreprocPipeline {


  def safely[T](handler: PartialFunction[Throwable, T]): PartialFunction[Throwable, T] = {
    case ex: ControlThrowable => throw ex
      // case ex: OutOfMemoryError (Assorted other nasty exceptions you don't want to catch)
	
    //If it's an exception they handle, pass it on
    case ex: Throwable if handler.isDefinedAt(ex) => handler(ex)
	
    // If they didn't handle it, rethrow. This line isn't necessary, just for clarity
    case ex: Throwable => throw ex
  }

  def main(args: Array[String]) {    

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    // import spark.implicits._

    parseInput[Seq[Step]](args) match {
      case Some(steps) =>
        val queued = Queue[Step]()
        val success = Set[String]()
        val failure = Set[(String, Throwable)]()
        val notRun = Set[String]()
        val skip = Set[String]()

        queued.enqueue(steps:_*)
        breakable {
          while(!queued.isEmpty) {
            breakable {
              while (true) {
                queued.dequeueFirst(step => !(step.dependsOn.toSet & (failure.map(_._1) | notRun)).isEmpty) match {
                  case None => break
                  case Some(step) =>
                    notRun.add(step.name)
                    println("not run: " + step.name)
                }
              }
            }

            queued.dequeueFirst(step => step.dependsOn.toSet.subsetOf(success | skip)) match {
              case None => break
              case Some(step) =>

                println(step)
                if(step.skip) {
                  println("skipped: " + step.name)
                  skip.add(step.name)
                } else {
                  try {
                    val stepConfigConfig = stepConfigConfigMap(step.name)
                    stepConfigConfig.step(spark, step.step.asInstanceOf[stepConfigConfig.ConfigType])
                    println("success: " + step.name)
                    success.add(step.name)
                  } catch safely {
                    case e: Throwable =>
                      failure.add((step.name, e))
                      val sw = new StringWriter
                      val pw = new PrintWriter(sw)
                      e.printStackTrace(pw)
                      pw.flush()
                      println("failure: " + step.name + " by " + e + " at " + sw.toString)
                  }
                }
            }
          }
        }
        queued.foreach(step => notRun.add(step.name))
        def printSeq[T](title: String, success: Iterable[T], indent: String = "  ") = {
          println(title)
          for (s <- success) {
            s match {
              case (a, b) =>
                println(indent + "(")
                println(indent + indent + a + ",")
                println(indent + indent + b)
                println(indent + ")")
              case _ => println(indent + s)
            }
          }
        }
        printSeq("===success===", success)
        printSeq("===skipped===", skip)
        printSeq("===failure===", failure)
        printSeq("===not run===", notRun)
      case None =>

    }


    spark.stop()


  }

}
