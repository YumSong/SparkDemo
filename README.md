一、集群搭建和操作步骤

1、集群组件开启步奏，依次开启hadoop，spark，zookeeper和kafka

2、编写zookeeper和kafka的集群开启脚本的时候注意环境变量的位置，.bashrc指的是当前命令窗口使用的环境变量，   Ubuntu系统的环境变量主要是在/etc/profile 别改错了

3、远程调试spark项目前，首先要把整个项目make一遍，然后在setJar的时候别把其他的包放进去，远程调试spark的时候，spark里面   jar包库相当于你的mvn中央仓库了

4、版本千万主要对应好

5、等有钱加内存条把集群内存给扩大，虚拟机8g内存不能同时跑spark，hadoop，kafka，zookeeper和idea，后面要是加上redis和hbase等缓存和数据库就不够用了

二、spark

1、使用spark提取有效特征

2、使用spark MLlib训练推荐模型

3、使用模型推荐前10的电影与从数据集里面的排序选出的电影进行比较

4、完成均方差检验模型

5、完成MAPK检验模型（注意事项：当map里面有调用到外部的变量或者函数时，要将类和object进行序列化，但是sc和conf不能将其序列化

6、使用MLlib检验模型

三、spark分类特征

1、增加模型分类（采用MLlib的逻辑回归，线性向量机SVM，朴素贝叶斯模型，决策树等算法训练模型）（求正确率）

2、准确率和召回率的PR和ROC

3、性能改进之特征标准化

4、性能改进之增加类别特征
