1、集群组件开启步奏，依次开启hadoop，spark，zookeeper和kafka
2、编写zookeeper和kafka的集群开启脚本的时候注意环境变量的位置，.bashrc指的是当前命令窗口使用的环境变量，
   Ubuntu系统的环境变量主要是在/etc/profile 别改错了
3、远程调试spark项目前，首先要把整个项目make一遍，然后在setJar的时候别把其他的包放进去，远程调试spark的时候，spark里面
   jar包库相当于你的mvn中央仓库了
4、版本千万主要对应好
