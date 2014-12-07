javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d ./IndexClasses Index.java
jar -cvf Index.jar -C IndexClasses/ .
sudo -u hdfs hadoop fs -rm -r /user/azeemshaikh38/output
sudo -u hdfs hadoop jar Index.jar org.myorg.Index /user/azeemshaikh38/input/roadnot.txt /user/azeemshaikh38/output/
sudo -u hdfs hadoop fs -get /user/azeemshaikh38/output/part-00000 ./index_roadnot.txt
sudo -u hdfs hadoop fs -rm -r /user/azeemshaikh38/output
sudo -u hdfs hadoop jar Index.jar org.myorg.Index /user/azeemshaikh38/input/ /user/azeemshaikh38/output/
sudo -u hdfs hadoop fs -get /user/azeemshaikh38/output/part-00000 ./index_all.txt
