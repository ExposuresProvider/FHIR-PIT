package datatrans.step

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.io.File
import java.nio.file.{Files, Paths, Path}
import org.scalatest.Assertions._
import java.nio.file.Files
import diffson.circe._
import sys.process._
import TestUtils._
import datatrans.Utils._

class DhallSpec extends FlatSpec {
  
  "Spec" should "typecheck" in {
    val res = "dhall-to-yaml --file config/example2.dhall".!
    assert(res == 0)

  }

}
