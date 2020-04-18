package datatrans
import scala.sys.process._
import java.io.{ File, PrintWriter, ByteArrayOutputStream }
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import io.circe.yaml.printer._

object PythonUtils {
  def writeToTempFile(contents: String,
    prefix: Option[String] = None,
    suffix: Option[String] = None): File = {
    val tempFi = File.createTempFile(prefix.getOrElse("prefix-"),
      suffix.getOrElse("-suffix"))
    val pw = new PrintWriter(tempFi)
    
    try {
      pw.write(contents)
    } finally {
      pw.close()
    }
    tempFi
  }


  def runCommand(cmd: Seq[String]): (Int, String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdoutStream)
    val stderrWriter = new PrintWriter(stderrStream)
    val exitValue = cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdoutStream.toString, stderrStream.toString)
  }

  def runPython[T](script: String, a : T)(implicit f : Encoder[T]) : (Int, String, String) = {
    val inp = print(a.asJson)

    val tmp = writeToTempFile(inp)

    val (exitValue, out, err) = runCommand(Seq("python", script, "--config",  tmp.getPath()))

    tmp.delete()

    (exitValue, out, err)

  }
}
