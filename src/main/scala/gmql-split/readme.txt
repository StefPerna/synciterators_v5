

Depending on which version of Scala you have, 
some parallel operations may hang Scala, esp. the REPL.
This is apparent due to Scala's default lazy initialization
of static functions and objects.

This can usually be solved using this Scala REPL option:

scala -Yrepl-class-based

https://github.com/scala/scala-parallel-collections/issues/34

