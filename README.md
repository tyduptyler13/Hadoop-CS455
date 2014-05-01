Hadoop-CS455
============

My hadoop project

Building
--------

Currently I use maven to build my project instead of ant. This makes things easy for me
to run on my continuous integration server [here](http://ci.myuplay.com/job/Hadoop%20(CS455)).

All of my builds are available there.

If you need to build this project. The eclipse on my account has maven built in and
a package command setup. This compiles and places all components in a jar.

I no longer build with dependencies or classpath built into the jar as it discovered
it was easier to just leave them out for the sake of the project. If you need them then
lookup maven shade plugin. My main class is at `cs455.hadoop.Engine`.

Running
-------
My jar contains two jobs that spit out all of the needed data. To run it run a command like
the following:

`$HADOOP_HOME/bin/hadoop jar SomeJar.jar cs455.hadoop.Engine /books/ score/ ngram/`

There are two output directories (one for each job), score and ngram. These can
be found under /user/tydup13/.

Output
------

Currently the outputs of each are as follows: (As of 0.2.17)
score dumps two files: grade and ease.

These contain the grade and score for every book. (The scores do not seem correct yet)

`[Book] [Score]`

The ngram directory spits out a single file part... 

`[NGram] [year] [occurances]`

Statistics will be based on the corpus and the inverse scores will be calculated from that.

Need: Books per decade, words per decade, number books the word appears in.

