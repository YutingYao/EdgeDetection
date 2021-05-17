# EdgeDetection
当前有三个主函数

## EdgeDetection
对于jpg格式进行卷积
## Cacul
调用geotrellis的focal算子作为计算模型，使用自定义数据划分策略
## Origin
调用geotrellis的focal算子作为计算模型，使用spark默认分区策略

```shell script
实验一 大规模数据实验
docker exec -it spark-master spark/bin/spark-submit \
--class com.wzy.Experimental \
/root/jars/wzy/EdgeDetection-6.0-SNAPSHOT.jar \
origin \
wjf/Australia2.tif \
1
```

```shell script
实验二 大规模数据实验
docker exec -it spark-master spark/bin/spark-submit \
--conf spark.driver.memory=18g \
--conf spark.executor.memory=4g \
--class com.wzy.Experimental \
/root/jars/wzy/EdgeDetection-6.0-SNAPSHOT.jar \
origin \
wjf/Australia2.tif \
1
```

