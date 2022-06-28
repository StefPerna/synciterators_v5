

/** This file shows some example queries **/


import scala.language.implicitConversions
import gmql.SAMPLEFileOps._
import gmql.SAMPLEFileOps.implicits._
import gmql.BEDFileOps._
import gmql.BEDFileOps.Genometric._
import gmql.BEDFileOps.BFAggr._
import gmql.BEDFileOps.BFCover._
import dbmodel.DBModel.{ Predicates, OpG }
import dbmodel.DBFile.OFile

//
// Import either SequentialGMQL or ParallelGMQL, but not both.
//
// Depending on which version of Scala you have, 
// some parallel operations may hang Scala, esp. the REPL.
// This is apparent due to Scala's default lazy initialization
// of static functions and objects.  This can usually be solved 
// using this Scala REPL option:
//
//       scala -Yrepl-class-based
//
// https://github.com/scala/scala-parallel-collections/issues/34


// import gmql.SAMPLEFileOps.SequentialGMQL._
import gmql.SAMPLEFileOps.ParallelGMQL._


dbmodel.DBFile.TMP       = "."    // Use current directory to store temp files
gmql.SAMPLEFileOps.ANS   = "."    // Use current directory to store results.
gmql.BEDFileOps.STORE    = true   // If true, always serialize BED files.
gmql.SAMPLEFileOps.STORE = true   // If true, always serialize Sample files.


val raw = {
  val path = "lib/MM/files"
  val filename = "lib/MM/list.txt"
  SampleFile.gmqlEncodeSampleFile(path, filename)
} // MM files. 


// Not sure if Stefano's MM files are sorted. So, sort them.

val samples = raw.tracksSorted("mm-sorted")  



// metadata of the 1st sample

samples(0).meta



// 1st BED entry of the 1st sample

samples(0).bedFile(0)



// Select: how many chr22 entries there are in each sample.

{ val qry1 = samples.onRegion { _.selectR { _.chrom == "chr22" } }

  qry1.foreach { s => val (sid, len) = (s.sid, s.bedFile.length);
                      println(s"** sid= ${sid}, chr22 entries= ${len} **") }
  qry1.close()
}



// Project: Show TF and filename

{ val qry2 = samples.projectS ("tf" -> "TF", "filename" -> "filename")

  qry2.foreach { s => println(s"** tf= ${s("tf")}, file= ${s("filename")} **") }
  qry2.close()
}



// Extend: Add # chr22 entries and #chr 20 entries as sample metadata

{ val qry3 = samples.extendS (
    "c20" -> { _.bedFile.selectR { _.chrom == "chr20" } .done {_.length } },
    "c22" -> { _.bedFile.selectR { _.chrom == "chr22" } .done {_.length } } )

  qry3.foreach { s => val (sid, tf) = (s.sid, s("TF"))
                      val (c20, c22) = (s("c20"), s("c22"))
                      println(s"** tf= ${tf}, sid= ${sid}, " +
                            s"chr20= ${c20}, chr22= ${c22} **") }
  qry3.close()
}



// Groupby: Count sanples for each cell type

{ val qry4 = samples.groupbyS("cell", "count" -> OpG.COUNT[SAMPLE])

  qry4.foreach { s => println(s.meta) }
  qry4.close()
}



// Difference: Remove chr1. Take diff in same cell.

{ val chr1 = samples.onRegion { _.selectR { _.chrom == "chr1" } }

  val qry6 = samples.differenceS(chr1, "cell")

  qry6(0).bedFile.done { _.take(3).foreach { println(_) } }

  qry6.close()
}



// Join: For samples in same cell, look for BED entries within 50 bases.

{ val qry7 = samples
             .joinS(samples, "cell")(
                 onRegion = BFOps.joinR(Genometric.NEAR(50))(BFOut.BOTH),
                 onSample = SFOut.OVERWRITE)
             .serialized

  // qry7 is serialized. so, can look at it multiple ways

  println(s"**** qry7 length = ${qry7.length}\n")

  qry7.foreach { s => println(s"** ${s.meta}, " +
                              s"length= ${s.bedFile.length} **\n") }

  qry7(0).bedFile.done  { _.take(3).foreach { println(_) } }

  qry7.close()
}  


// Map: map all samples to each other.

{ val qry8 = { samples mapS samples }.serialized

  println("\n*** qry8(0), counts should be mostly 1s ***\n")

  qry8(0)

  qry8(0).bedFile.done { _.take(3).foreach { println(_) } }

  println("\n*** qry8(2), counts should be mostly 0s ***\n")

  qry8(2).bedFile.done { _.take(3).foreach { println(_) } }

  qry8.close()
}



// Complement: What the gaps in the BED files.


{ val qry9 = samples.coverS(BFOps.complementR)

  qry9(0).bedFile.done { _.take(3).foreach { println(_) } }

  qry9.close()
}



//
// Timing #1
//

// [[bm]] runs [[f]] as many times as possible
// within [[duration]] number of milliseconds.


def qry1() = samples.onRegion { _.selectR { _.chrom == "chr22" } }

def qry2() = samples.projectS ("tf" -> "TF", "filename" -> "filename")

def qry3() = samples.extendS (
               "c20" -> { _.bedFile.selectR { _.chrom == "chr20" } 
                                   .done { _.length } },
               "c22" -> { _.bedFile.selectR { _.chrom == "chr22" }
                                   .done { _.length } } )

def qry4() = samples.groupbyS("cell", "count" -> OpG.COUNT[SAMPLE])

def qry6() = {
  val chr1 = samples.onRegion { _.selectR { _.chrom == "chr1" } }
  samples.differenceS(chr1, "cell")
}

def qry7() = samples.joinS(samples, "cell")(
                 onRegion = BFOps.joinR(Genometric.NEAR(50))(BFOut.BOTH),
                 onSample = SFOut.OVERWRITE)

def qry8() = samples mapS samples 

def qry9() = samples.coverS(BFOps.complementR)


def bm(duration: Long)(f: () => SAMPLEFILE): Int =
{
  val end = System.currentTimeMillis + duration
  var count = 0
  while(System.currentTimeMillis < end) { f().serialized.close(); count += 1 }
  count
}

def run(duration: Long)(qry: () => SAMPLEFILE) = 
  for (
    bstore <- List(false, true);
    sstore <- List(false, true);
    time   <- Some { gmql.BEDFileOps.STORE = bstore;
                     gmql.SAMPLEFileOps.STORE = sstore;
                     bm(duration) { qry } }
  ) yield println(s"** $bstore, $sstore, $time **")


def runAll(duration: Long) = {
  val queries = List("qry1" -> qry1 _, "qry2" -> qry2 _, "qry3" -> qry3 _,
                     "qry4" -> qry4 _, "qry6" -> qry6 _, "qry7" -> qry7 _,
                     "qry8" -> qry8 _, "qry9" -> qry9 _)
  for ((q, qry) <- queries) {
    println(s"**** $q ****");
    run(duration)(qry)
  }
}

runAll(20000)



//
// Timing #2
//



def showTiming[A](rounds: Int)(codes: () => SAMPLEFILE) = {
  var remains = rounds
  val t0 = System.nanoTime
  while (remains > 0) { remains = remains - 1; codes().serialized.close() }
  val dt = (System.nanoTime - t0) / 1e9d;
  println(s"Time taken per round: ${dt / rounds}")
}


def showAllTiming(rounds: Int) = {
  val queries = List("qry1" -> qry1 _, "qry2" -> qry2 _, "qry3" -> qry3 _,
                     "qry4" -> qry4 _, "qry6" -> qry6 _, "qry7" -> qry7 _,
                     "qry8" -> qry8 _, "qry9" -> qry9 _)
  for ((q, qry) <- queries) {
    println(s"**** $q ****");
    showTiming(rounds)(qry)
  }
}


showAllTiming(5)



  
