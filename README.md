BigFS
====

bigFS is an experimenting work for hobby i was just trying to write an _Distributed File System_ like hdfs but instead of using namenode i try to use cassandra as metadata storage so there is no single point failure

bigFS was designed for web files like images and videos, files was writing to disk instead of cassandra but this project is born as dead and this codes just a reminder for my self 

Run
====
bin/cassandra  
python src/python/clientTest.py


