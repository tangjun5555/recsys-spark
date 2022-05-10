package com.github.tangjun5555.recsys.spark.param;

import com.github.tangjun5555.recsys.spark.jutil.JavaTimeUtil;
import org.kohsuke.args4j.Option;
import java.io.Serializable;

/**
 * @author: tangj 1844250138@qq.com
 * time: 2022/5/10 4:19 PM
 * description:
 */
public class RankTaskParam implements Serializable {

    public final long currentTimestamp = System.currentTimeMillis();

    @Option(name = "--task_name", usage = "任务唯一标识", required = true)
    public String taskName = null;

    @Option(name = "--task_type", usage = "任务类型", required = true)
    public String taskType = null;

    @Option(name = "--model_save_path", usage = "模型保存地址", required = true)
    public String modelSavePath = null;

    @Option(name = "--model_save_version", usage = "模型保存版本", required = false)
    public String modelSaveVersion = "latest";

    @Option(name = "--train_df_table_name", usage = "训练集hive表名", required = false)
    public String trainDFTableName = null;

    @Option(name = "--train_df_start_dt", usage = "训练集hive表起始分区", required = false)
    public String trainDFStartDt = null;

    @Option(name = "--train_df_end_dt", usage = "训练集hive表结束分区", required = false)
    public String trainDFEndDt = null;

    @Option(name = "--train_test_ratio", usage = "训练集中作为测试集比例", required = false)
    public double trainTestRatio = 0.0;

    @Option(name = "--is_cycle_training", usage = "是否周期训练", required = false)
    public int isCycleTraining = 0;

    @Option(name = "--cycle_training_end_dt", usage = "周期训练结束分区", required = false)
    public String cycleTrainingEndDt = JavaTimeUtil.computeDiffDate(JavaTimeUtil.getCurrentDt(), -1);

    @Option(name = "--cycle_training_period", usage = "周期训练天数")
    public int cycleTrainingPeriod = 7;

    @Option(name = "--test_df_table_name", usage = "测试集hive表名")
    public String testDFTableName = "";

    @Option(name = "--test_df_start_dt", usage = "测试集hive表起始分区")
    public String testDFStartDt = "";

    @Option(name = "--test_df_end_dt", usage = "测试集hive表结束分区")
    public String testDFEndDt = "";

    @Option(name = "--predict_df_table_name", usage = "预测集hive表名")
    public String predictDFTableName = "";

    @Option(name = "--predict_df_start_dt", usage = "预测集hive表起始分区")
    public String predictDFStartDt = "";

    @Option(name = "--predict_df_end_dt", usage = "预测集hive表结束分区")
    public String predictDFEndDt = "";

    @Option(name = "--predict_result_table_name", usage = "预测结果hive表名")
    public String predictResultTableName = "";

    @Option(name = "--predict_result_table_partition_name", usage = "预测结果hive表名分区名")
    public String predictResultTablePartitionName = "dt";

    @Option(name = "--predict_result_table_partition_value", usage = "预测结果hive表名分区值")
    public String predictResultTablePartitionValue = "";

    @Option(name = "--predict_result_column_names", usage = "预测结果展示列名")
    public String predictResultTableColumnNames = "";

    @Option(name = "--features_col", usage = "特征列名")
    public String featuresCol = "features";

    @Option(name = "--label_col", usage = "标签列名")
    public String labelCol = "label";

    @Option(name = "--prediction_col", usage = "预测值列名")
    public String predictionCol = "prediction";

    @Option(name = "--weight_col", usage = "样本权重列名")
    public String weightCol = "";

    @Option(name = "--group_col", usage = "样本分组列名，用于计算GAUC指标")
    public String groupCol = "";

    @Option(name = "--features_format", usage = "特征格式,目前只支持libsvm、csv")
    public String featuresFormat = "libsvm";

    @Option(name = "--features_dim", usage = "特征维度，取值范围[1,10000]")
    public int featuresDim = 100;

}
