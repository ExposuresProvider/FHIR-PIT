package datatrans

import org.apache.hadoop.fs.{FileUtil, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.SparkSession
import datatrans.Utils._

import scala.collection.mutable.ListBuffer


object PreprocCMAQ {

  def to_seq(header: Path, itr: RemoteIterator[LocatedFileStatus]) : Array[Path] = {
    val listBuf = new ListBuffer[Path]
    listBuf.append(header)
    while(itr.hasNext) {
      val fstatus = itr.next()
      listBuf.append(fstatus.getPath)
    }
    listBuf.toArray
  }

  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

      val input_file = args(0)
      val output_dir = args(1)

      val df = spark.read.format("csv").option("header", true).load(input_file)
      df.select("row","col","a","o3","pmij").write.partitionBy("row", "col").csv(output_dir)

      spark.sparkContext.setLogLevel("WARN")

      val hc = spark.sparkContext.hadoopConfiguration
      val output_dir_path = new Path(output_dir)
      val output_dir_fs = output_dir_path.getFileSystem(hc)

      def listDirs(path: Path) = {
        output_dir_fs.listStatus(path).filter(p => p.isDirectory).map(f => f.getPath)
      }

      val rowdirs = listDirs(output_dir_path).par

      val header = "start_date,o3,pm25\n"
      val header_filename = f"$output_dir/.header"
      val header_file_path = new Path(header_filename)
      writeToFile(hc, header, header_filename)

      for(rowdir <- rowdirs) {
        println(f"processing row $rowdir")
        val row = rowdir.getName.split("=")(1).toInt
        val coldirs = listDirs(rowdir).par
        for (coldir <- coldirs) {
          println(f"processing column $coldir")
          val col = coldir.getName.split("=")(1).toInt
          val output_filename = f"$output_dir/C$col%03dR$row%03d.csv"
          val output_file_path = new Path(output_filename)
          val srcs = to_seq(header_file_path, output_dir_fs.listFiles(coldir, false))
          FileUtil.copy(output_dir_fs, srcs, output_dir_fs, output_file_path, false, true, hc)
          output_dir_fs.delete(coldir, true)
        }
        output_dir_fs.delete(rowdir, true)
      }
      output_dir_fs.delete(header_file_path, false)

      spark.stop()

    }
  }
}
