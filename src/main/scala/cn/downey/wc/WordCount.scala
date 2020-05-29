package cn.downey.wc

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {

    //创建批处理执行环境
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //从文件中
    val inputDataSet : DataSet[String] = env.readTextFile("D:\\Program\\FlinkTutorial\\src\\main\\resources\\word.txt")

    //基于DataSet做转换，首先按空格分词，按照word作为key做groupby
    val resultDataSet :DataSet[(String,Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)


    resultDataSet.print()


  }

}
