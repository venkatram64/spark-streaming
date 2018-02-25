properties files

/etc/hadoop/conf/core-site.xml
/etc/hadoop/conf/hdfs-site.xml

name node ui :50070(port)

gateway node/client node

/etc/hadoop/cong

ls -ltr

view core-site.xml

fs.defaultFS => name node uri:

view hdfs-site.xml

dfs.blocksize =>
dfs.replication =>

hostname -f

hadoop fs -ls /user/venkatram  gives the hdfs user name

To create the user space on hartonworks sandbox

sudo -u hdfs hadoop fs -mkdir /user/root; sudo -u hdfs hadoop fs -chown -R root /user/root


ls -ltr /data/crime

to know the size of files

du -sh /data/crime

hadoop fs -ls


hadoop fs  (gives all the commands)

to copy from local to destinations

hadoop fs -help copyFromLocal

hadoop fs -copyFromLocal /data/crime /user/venkatram/.

hadoop fs -ls /user/venkatram

hadoop fs -ls -R /user/venkatram/crime

hadoop fs -du -s -h /user/venkatram/crime

hdfs fsck /user/venkatram/crime -files -blocks -locations

YARN (Yet another resource negotiation)

In certifications Spark typically runs in YARN mode

we should be able to check the memory configuration to
understand the cluser capacity

/etc/hadoop/conf/yarn-site.xml
/etc/spark/conf/spark-env.sh

Spark default settings
Number of executors -2
Memory 1GB

Quite often under utilize resources. Understanding memory
settings thoroughly and then mapping them with data size we are
trying to process we can accelerate the execution of our jobs


vi yarn-site.xml

resourcemanager => to get the something:8050

vi spark-env.sh


Go to https://github.com/dgadiraju/data

to get the datasets

clone or download on to virtual machine created using Cloudera
quickstart or Hortonworks Sandbox

you can set up locally for practicing for Spark, but it is highly
recommended to use HDFS which comes out of the box with Cloudera
Quickstart or Hortonworks or our labs

retail_db

wc -l /data/retail_db/*/*


