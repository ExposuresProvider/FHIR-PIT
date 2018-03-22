package datatrans

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.SparkSession
import datatrans.Utils._


object PreprocCMAQ {
  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

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

      for(rowdir <- rowdirs) {
        println(f"processing row $rowdir")
        val row = rowdir.getName.split("=")(1).toInt
        val coldirs = listDirs(rowdir).par
        for (coldir <- coldirs) {
          println(f"processing column $coldir")
          val col = coldir.getName.split("=")(1).toInt
          val output_filename = output_dir + "/" + f"C$col%03dR$row%03d.csv"
          val output_file_path = new Path(output_filename)
          FileUtil.copyMerge(output_dir_fs, coldir, output_dir_fs, output_file_path, true, hc, null)
        }
      }

      spark.stop()

    }
  }
}
