export hadoopClassPath=`hadoop classpath`:/usr/lib/gson-2.2.2.jar
echo $hadoopClassPath
export HADOOP_CLASSPATH=/usr/lib/gson-2.2.2.jar
javac -cp $hadoopClassPath -d runner_classes  Node.java CliqueNode.java EvidenceCollection.java EvidenceDistribution.java OutputBuilder.java InferenceRunner.java
jar -cvf runner.jar -C runner_classes/ .
hadoop jar runner.jar org.myorg.InferenceRunner
