package datatrans.step

import sys.process.{Process, ProcessLogger}
import sys.process._
import java.io.File
import java.io.PrintWriter
import java.nio.file.Files
import scala.reflect.io.Directory
import org.apache.spark.sql.SparkSession
import scopt._
import io.circe.{Decoder}
import io.circe.generic.semiauto._
import datatrans.Utils._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class PreprocSystemConfig(
  pyexec: String,
  requirements: Seq[String],
  workdir : String,
  command : Seq[String]
)

object PreprocSystem extends StepImpl {

  type ConfigType = PreprocSystemConfig

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config: PreprocSystemConfig): Unit = {

    time {
      val tempDir = Files.createTempDirectory(null)
      try {
        Process(Seq("virtualenv", "-p", config.pyexec, f"${tempDir.toString}/venv")).!
        Process(Seq(f"${tempDir.toString}/venv/bin/pip", "install", "--no-cache-dir") ++ config.requirements).!
        val exitValue = Process(f"${tempDir.toString}/venv/bin/python" +: config.command, new File(config.workdir)).!(ProcessLogger(println, println))
        println(f"exitValue = ${exitValue}")
        if (exitValue != 0) {
          throw new RuntimeException(f"error: ${exitValue}")
        }
      } finally {
        new Directory(tempDir.toFile).deleteRecursively()
      }
    }
  }

}
