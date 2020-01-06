package datatrans.step

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import org.apache.hadoop.fs.Path
import datatrans.step.PreprocFHIRResourceType._
import datatrans.Utils._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.io.File
import java.nio.file.{Files}
import org.scalatest.Assertions._
import java.nio.file.Files
import gnieh.diffson.circe._
import io.circe.parser._
import org.joda.time._
import org.joda.time.format._
import org.deeplearning4j.util._
import org.deeplearning4j.nn.multilayer._
import org.deeplearning4j.nn.graph._
import org.deeplearning4j.nn.conf._
import org.deeplearning4j.nn.conf.inputs._
import org.deeplearning4j.nn.conf.layers._
import org.deeplearning4j.nn.api._
import org.deeplearning4j.nn.weights._
import org.nd4j.linalg.lossfunctions.LossFunctions._
import org.nd4j.linalg.learning.config._
import org.nd4j.linalg.activations._
import org.nd4j.linalg.activations.impl._
import TestUtils._

class TrainSpec extends FlatSpec {
  
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

  "Train" should "train a MultiLayer model" in {
    val tempFileModelInput = Files.createTempFile("model_input", ".json")
    val tempFileModelOutput = Files.createTempFile("model_output", ".json")

    val conf = new NeuralNetConfiguration.Builder()
      .seed(1)
      .l2(0.005)
      .activation(Activation.RELU)
      .weightInit(WeightInit.XAVIER)
      .updater(new Nesterovs(0.1, 0.9))
      .list()
      .layer(0, new DenseLayer.Builder().nOut(4).build())
      .layer(1, new OutputLayer.Builder(LossFunction.NEGATIVELOGLIKELIHOOD).activation(Activation.SOFTMAX).nOut(2).build())
      .setInputType(InputType.feedForward(3))
      .build()

    val hc = spark.sparkContext.hadoopConfiguration
    writeToFile(hc, tempFileModelInput.toString, conf.toJson())

    val config = TrainConfig(
      model_input_file = tempFileModelInput.toString,
      model_output_file = tempFileModelOutput.toString,
      num_lines_to_skip = 0, // currently only 0 works
      first_column_label = 3,
      last_column_label = 4,
      number_epochs = 10,
      batch_size_per_worker = 2, // currently only > 1 works
      num_workers_per_node = 1,
      network_mask = "127.0.0.0/8",
      controller_address = "127.0.0.1",
      input_file = "src/test/data/train/input_file.csv",
      model_type="MultiLayer"
    )

    Train.step(spark, config)

    Files.delete(tempFileModelInput)
    Files.delete(tempFileModelOutput)

  }

/*  "Train" should "train a ComputationGraph model" in {
    val tempFileModelInput = Files.createTempFile("model_input", ".json")
    val tempFileModelOutput = Files.createTempFile("model_output", ".json")

    val conf = new NeuralNetConfiguration.Builder()
      .seed(1)
      .l2(0.005)
      .activation(Activation.RELU)
      .weightInit(WeightInit.XAVIER)
      .updater(new Nesterovs(0.1, 0.9))
      .graphBuilder()
      .addInputs("inputs")
      .addLayer("L0", new DenseLayer.Builder().nIn(3).nOut(4).build(), "inputs")
      .addLayer("L1", new OutputLayer.Builder(LossFunction.NEGATIVELOGLIKELIHOOD).activation(Activation.SOFTMAX).nIn(4).nOut(2).build(), "L0")
      .setOutputs("L1")
      .build()

    val hc = spark.sparkContext.hadoopConfiguration
    val model_input_path = new Path(tempFileModelInput.toString)
    val model_input_file_system = model_input_path.getFileSystem(hc)
    val os = model_input_file_system.create(model_input_path, true)
    os.write(conf.toJson().getBytes())
    os.close()

    val config = TrainConfig(
      model_input_file = tempFileModelInput.toString,
      model_output_file = tempFileModelOutput.toString,
      num_lines_to_skip = 0, // currently only 0 works
      first_column_label = 3,
      last_column_label = 4,
      number_epochs = 10,
      batch_size_per_worker = 2, // currently only > 1 works
      num_workers_per_node = 1,
      network_mask = "127.0.0.0/8",
      controller_address = "127.0.0.1",
      input_file = "src/test/data/train/input_file.csv",
      model_type="ComputationGraph"
    )

    Train.step(spark, config)

    Files.delete(tempFileModelInput)
    Files.delete(tempFileModelOutput)

  } */

}
