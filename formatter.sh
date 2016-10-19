export hadoopClasspath=`hadoop classpath`
echo $hadoopClasspath
javac -cp $hadoopClasspath -d formatter_classes  Formatter.java
jar -cvf formatter.jar -C formatter_classes/ .
hadoop jar formatter.jar org.myorg.Formatter ../testInput1/BN
