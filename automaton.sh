#!/usr/bin/env bash

echo "Running automaton for class $1" > output.log

if 
  sbt package 2>&1 | grep "[success]"
then
  echo -e "===== build success! =====\n\n"
  echo -e "===== build success! =====\n\n" >> output.log
  scp ./target/scala-2.10/scalaspark_2.10-1.0.jar changping12:~/bsnsk/
  echo -e "===== upload success! =====\n\n"
  echo -e "===== upload success! =====\n\n" >> output.log
  result=$(ssh changping12 "/mnt/disk1/daim/spark-1.6.1-bin-without-hadoop/bin/spark-submit --class \"$1\" --master yarn --num-executors 7 --executor-memory 2G ~/bsnsk/scalaspark_2.10-1.0.jar 2>&1")
  echo "$result" >> output.log
  if 
    echo "$result" | grep "Exception in thread"
  then 
    subject='"Exception found :("'
  else 
    subject='"Probably a success :D"'
  fi
else
  echo "build failure"
  echo >> output.log
  subject='"Build failure :("'
fi

./notify.py $subject
