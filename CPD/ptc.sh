export hadoopClassPath=`hadoop classpath`

javac -cp $hadoopClassPath -d cpd_classes  Node.java CliqueNode.java PTC.java
jar -cvf cpd.jar -C cpd_classes/ .
hadoop jar cpd.jar org.myorg.PTC
