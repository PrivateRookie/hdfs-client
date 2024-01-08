rm -f /hdfs-fuse/a.txt
date > /tmp/a.txt
cp /tmp/a.txt /hdfs-fuse/a.txt
date >> /hdfs-fuse/a.txt
cat /hdfs-fuse/a.txt