#!/bin/bash
# Runs $class on the cluster of $master with given command line arguments.
# Logs are saved to "$class".log and "$class".err in the current directory.

class="TwitterPopularTags"
master="spark://ukko160:7077"

jar=/cs/taatto/scratch/ayahmad/twitterPopTag/target/scala-2.10/TwitterPopularTags-assembly-0.1.0.jar
/cs/taatto/scratch/ayahmad/spark-1.0.2-bin-hadoop1/bin/spark-submit --class "$class" \
--master "$master" --jars /cs/taatto/scratch/msingh/twitterPopTag/spark-streaming-twitter_2.10-1.0.2.jar   "$jar" # \

# $@ 1>"$class".log 2>"$class".err
