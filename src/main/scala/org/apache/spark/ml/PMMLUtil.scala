package org.apache.spark.ml

import java.io.File

import org.apache.spark.sql.types.StructType
import org.jpmml.sparkml.PMMLBuilder

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/9/18 12:42 下午
 * description:
 */
object PMMLUtil {

//  def model2PMMLFile(schema: StructType, model: Model, fileName: String): Unit = {
//    val pipelineModel = new PipelineModel("", Array(model))
//    val pmmlBuilder = new PMMLBuilder(schema, pipelineModel)
//    pmmlBuilder.buildFile(new File(fileName))
//  }

  def pipeline2PMMLFile(schema: StructType, pipelineModel: PipelineModel, fileName: String): Unit = {
    val pmmlBuilder = new PMMLBuilder(schema, pipelineModel)
    pmmlBuilder.buildFile(new File(fileName))
  }

}
