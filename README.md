1、集群组件开启步奏，依次开启hadoop，spark，zookeeper和kafka<br>
2、编写zookeeper和kafka的集群开启脚本的时候注意环境变量的位置，.bashrc指的是当前命令窗口使用的环境变量，
   Ubuntu系统的环境变量主要是在/etc/profile 别改错了<br>
3、远程调试spark项目前，首先要把整个项目make一遍，然后在setJar的时候别把其他的包放进去，远程调试spark的时候，spark里面
   jar包库相当于你的mvn中央仓库了<br>
4、版本千万主要对应好<br>
5、等有钱加内存条把集群内存给扩大，虚拟机8g内存不能同时跑spark，hadoop，kafka，zookeeper和idea，后面要是加上redis和hbase等缓存和数据库就不够用了<br>
