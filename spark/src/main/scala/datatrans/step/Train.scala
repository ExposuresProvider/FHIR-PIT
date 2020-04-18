package datatrans.step

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java._
import scopt._
import io.circe._
import io.circe.generic.semiauto._
import org.datavec.spark.transform.misc._
import org.datavec.api.records.reader.impl.csv._
import org.nd4j.parameterserver.distributed.conf._
import org.deeplearning4j.util._
// import org.deeplearning4j.spark.parameterserver.training._
import org.deeplearning4j.spark.impl.paramavg._
import org.deeplearning4j.nn.conf._
import org.deeplearning4j.spark.impl.multilayer._
import org.deeplearning4j.spark.impl.graph._
import org.deeplearning4j.spark.datavec._
import datatrans.Utils._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class TrainConfig(
  model_input_file : String,
  model_output_file : String,
  input_file : String,
  num_lines_to_skip : Int,
  first_column_label : Int,
  last_column_label : Int,
  model_type : String,
  number_epochs : Int,
  batch_size_per_worker : Int,
  num_workers_per_node : Int,
  network_mask: String,
  controller_address: String
)

object Train extends StepImpl {

  type ConfigType = TrainConfig

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config: TrainConfig): Unit = {

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    time {

      val sparkContext = spark.sparkContext
      val hc = sparkContext.hadoopConfiguration

      val rddString0 = sparkContext.textFile(config.input_file);
      val rddString = rddString0.mapPartitionsWithIndex{ (id_x, iter) => if (id_x == 0) iter.drop(3) else iter }
      val recordReader = new CSVRecordReader(config.num_lines_to_skip, ",");
      val rddWritables = rddString.map(new StringToWritablesFunction(recordReader).call _);
      val trainingData = rddWritables.map(new DataVecDataSetFunction(config.first_column_label, config.last_column_label, 2, true, null, null).call _);

      // SharedTrainingMaster NPE when running on local
/*      // Configure distributed training required for gradient sharing implementation
      val conf = VoidConfiguration.builder()
	                        .unicastPort(40123)             //Port that workers will use to communicate. Use any free port
				.networkMask(config.network_mask)     //Network mask for communication. Examples 10.0.0.0/24, or 192.168.0.0/16 etc
				.controllerAddress(config.controller_address)  //IP of the master/driver
				.build()

      val batchSizePerWorker = config.batch_size_per_worker
      //Create the TrainingMaster instance
      val trainingMaster = new SharedTrainingMaster.Builder(conf, batchSizePerWorker)
				.batchSizePerWorker(batchSizePerWorker) //Batch size for training
				.updatesThreshold(1e-3)                 //Update threshold for quantization/compression. See technical explanation page
				.workersPerNode(config.num_workers_per_node)      // equal to number of GPUs. For CPUs: use 1; use > 1 for large core count CPUs
                                .build()*/

      val batchSizePerWorker = config.batch_size_per_worker
      //Create the TrainingMaster instance
      val trainingMaster = new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker)
        .build()

      //Model setup as on a single node. Either a MultiLayerConfiguration or a ComputationGraphConfiguration
      val sparkNet = config.model_type match {
        case "MultiLayer" =>
          //Create the SparkDl4jMultiLayer instance
          val model = MultiLayerConfiguration.fromJson(readFile(hc, config.model_input_file));
          new SparkDl4jMultiLayer(sparkContext, model, trainingMaster)
        case "ComputationGraph" =>
          val model = ComputationGraphConfiguration.fromJson(readFile(hc, config.model_input_file));
          new SparkComputationGraph(sparkContext, model, trainingMaster)
      }

      //Execute training:
      for (i <- 0 until config.number_epochs) {
        config.model_type match {
          case "MultiLayer" =>
            sparkNet.asInstanceOf[SparkDl4jMultiLayer].fit(trainingData)
          case "ComputationGraph" =>
            sparkNet.asInstanceOf[SparkComputationGraph].fit(trainingData)
        }
      }

      val model = config.model_type match {
        case "MultiLayer" =>
          sparkNet.asInstanceOf[SparkDl4jMultiLayer].getNetwork()
        case "ComputationGraph" =>
          sparkNet.asInstanceOf[SparkComputationGraph].getNetwork()
      }

      val path = new Path(config.model_output_file)
      val file_system = path.getFileSystem(hc)
      val out_stream = file_system.create(path)
      try {
        ModelSerializer.writeModel(model, out_stream, true)
      } finally {
        out_stream.close()
      }
    }
  }

}
