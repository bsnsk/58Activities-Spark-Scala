#!/usr/bin/env bash

if 
  sbt package 2>&1 | grep "[success]"
then
  echo -e "===== build success! =====\n\n"
  scp ./target/scala-2.10/scalaspark_2.10-1.0.jar changping12:~/bsnsk/
  echo -e "===== upload success! =====\n\n"
  num=4
  ssh changping12 "/mnt/disk1/daim/spark-1.6.1-bin-without-hadoop/bin/spark-submit --class \"$1\" --master yarn --num-executors 7 --executor-memory 2G ~/bsnsk/scalaspark_2.10-1.0.jar"
else
  echo "build failure"
fi
