package demo

import synchrony.gmql.GMQL
import GMQL._
import Predicates._
import GMQL.Projections._
import GMQL.GenomeLocus._
import GMQL.DB._
import GMQL.implicits._
import synchrony.gmql.EncodeNP._

import scala.language.postfixOps
import scala.io.Source._
import scala.collection.mutable.ListBuffer
import scala.math._

import java.io.File
import java.io.PrintWriter

import org.biojava.nbio._
import org.biojava.nbio.genome.parsers.genename._

import scala.jdk.CollectionConverters._

// object Reporter {
// 	// Small object with auxiliary printing functions
// 	private def showSamples(db:DB) = db.samples.eiterator.foreach((s:Sample) => { println(s); println("**") } )

// 	private def showTrack(sample:Sample) = sample.track.foreach(println)

// 	private def showTrackHead(sample:Sample) = sample.track.slice(0,10).foreach(println)

// 	private def showSize(sample:Sample) = println(s"track size = ${sample.bedFile.filesize/1000.0} kb, track count = ${sample.track.length}")

// 	private def showTotalSize(db:DB) = println(s"total track size = ${db.samples.totalsizeOnDisk/(1000.0*1000.0)} mb")

// 	private def showTotalCount(db:DB) = println(s"total track count = ${countLines(db)} lines")

// 	private def mean(l:Seq[Double]): Double = l.reduceLeft(_ + _)/l.length
	
// 	private def std(l:Seq[Double]): Double = math.sqrt(mean(l.map((x:Double) => x * x)) - mean(l) * mean(l))
		

// 	def report(db:DB)
// 	{
// 		showSamples(db)
// 		showTotalSize(db)
// 		showTotalCount(db)
// 		db.samples.eiterator.zipWithIndex.foreach(
// 			{ case (s,count) => {println(s"Sample ${count}"); println(showSize(s)); showTrackHead(s)} }
// 		)
// 	}

// 	def sortProfiles(prf:Seq[(Double,Double)],name:String) {
// 		// Conv is used to convert memory to kb, mb, gb, etc.
// 		println(f"all ${name} profiles")
// 		println(prf)

// 		val times = prf map (_._1)
// 		val mems = prf map (_._2)

// 		println("average times")
// 		println(f"${mean(times)}%10.6f")
// 		println("time std")
// 		println(f"${std(times)}%10.6f")
// 		println("average mem")
// 		println(f"${mean(mems)}%10.6f")
// 		println("mem std")
// 		println(f"${std(mems)}%10.6f")
// 	}

// 	def countLines(db:DB) = db.samples.eiterator.foldLeft(0)((n,s) => n + s.track.length)
// 	// def countLines(db:DB) = for {s <- db.samples.eiterator} yield s.track.length
// }

// object SavUnwrapper {
// 	// reads and parses a sav file, then moves files from the temp variable folder


// 	def moveFiles(savpath:os.Path, tgtfldr:os.Path) {
// 		for (meta <- os.read.lines(savpath)
// 			if meta.split('\t')(0).startsWith("/var");
// 			p = os.Path(meta)) {
// 			os.move(p, tgtfldr / p.last)
// 		}
// 	}

// }

// object ErrorManagement {
// 	// reads and parses a sav file, then moves files from the temp variable folder

// 	def Try[A](a: => A): Either[Exception,A] = 
// 		try Right(a)
// 		catch { case e: Exception => Left(e)}

// }


object GMQLSynchro_Demo2 extends App {

	val genenames = GeneNamesParser.getGeneNames().asScala.toList

	synchrony.iterators.Serializers.DEBUG = false // remove printouts
	synchrony.iterators.FileCollections.DEBUG = false // remove printouts
	GMQL.Projections.DEBUG = false // remove printouts
	GMQL.DEBUG = false // remove printouts

	// Set current working directories
	val wd = os.pwd
	val tmp_folder = os.root / "var" / "folders" / "k8" / "sf3vvc_921d8r9n7qgy_7kh00000gn" / "T"
	val data_fldr = wd / "tests"

	val NEXEC = 1

	// clean up output folders

	os.list(wd / "temp").foreach(f => os.remove(f))


	os.remove.all(wd / "tests" / "output" / s"use_case_output")

	os.walk(tmp_folder,skip = (p: os.Path) => !(p.last.startsWith("synchrony-"))).foreach(os.remove(_))
	// os.walk(tmp_folder).foreach(os.remove(_))

	val byte2mb = 1024*1024

	val runtime = Runtime.getRuntime
	println(f"Total memory: ${runtime.totalMemory/byte2mb}")


	val gmqlResult = 1955658

	// Load dataset 1

	val dataset1_name = "K562"

	val samples1dir = data_fldr / "input" / s"${dataset1_name}" / "files"
	val samples1list = data_fldr / "input" / s"${dataset1_name}" / "list.txt"

	var tfbs = altOnDiskEncodeNPDB(samples1dir.toString)(samples1list.toString).select().tracksSorted.materialized

	println()
	println("tfbs db")

	Reporter.report(tfbs)

	// Load dataset 2
	val dataset2_name = "ncbiRefSeqCurated"

	val samplesdir2 = data_fldr / "input" / s"${dataset2_name}" / "files"
	val sampleslist2 = data_fldr / "input" / s"${dataset2_name}" / "list.txt"

	val genes = altOnDiskDB(samplesdir2.toString)(sampleslist2.toString).select().tracksSorted.materialized


	println()
	println("genes db")

	Reporter.report(genes)

	// BEGIN EXECUTION

	println("Starting execution")

	// def outdb = rdb
	def prom = genes.select(region=OnRegion(Strand === "+")).project(region=OnRegion("chromStart" as Start - 2000,"chromEnd" as Start + 1 + 1000)).materialized;
		
	// def getSymbol(n: String, gns: List[GeneName]): Double = {
	// 	val l = gns.filter(gn => gn.getRefseqIds == n)
	// 	l.length match {
	// 		case 0 => "NA"
	// 		case _ => l map ((gn:GeneName) => gn.getApprovedSymbol) mkString ("_")
	// 		}
	// 	}

	def mapped = DB.map(region=OnRegion("found" as Count))(tfbs,prom)

	def outdb = DB.select(region=OnRegion(MetaR[Double]("found")>0))(mapped)

	def tfbs_qval = {
		 val minusLog10 = FunObj[Double,Double](x => scala.math.pow(10,-1*x))
	     // val qval = GenBObj(r => pow(10, -1 * MetaR[Double]("qvalue")(r)))
	     tfbs.project(region=OnRegion("exp_qvalue" as minusLog10(MetaR[Double]("qval"))))
	}

	println()
	println("tfbs_qval")

	Reporter.report(tfbs_qval)


	val execProfile:IndexedSeq[(Double,Double)] = for (i <- 0 to (NEXEC-1)) yield { // System.gc();
		((t:Long,m:Long) => {println(Reporter.countLines(outdb)); ((System.nanoTime()-t)/1e9,(runtime.totalMemory - runtime.freeMemory - m)/byte2mb.toDouble)}) (System.nanoTime(),runtime.totalMemory - runtime.freeMemory)
		// ((t:Long) => {Reporter.countLines(outdb).foreach(println); (System.nanoTime()-t)/1e9}) (System.nanoTime())
	}

	println()

	val writeProfiles:IndexedSeq[(Double,Double)] = for (i <- 0 to (NEXEC-1)) yield { // System.gc();
		((t:Long,m:Long) => {println(Reporter.countLines(outdb.materializedOnDisk)); ((System.nanoTime()-t)/1e9,(runtime.totalMemory - runtime.freeMemory - m)/byte2mb.toDouble)}) (System.nanoTime(),runtime.totalMemory - runtime.freeMemory)
	}

	println()

	Reporter.sortProfiles(execProfile,"execution")
	println()
	Reporter.sortProfiles(writeProfiles,"writing")

	println()
	println("output")
	Reporter.report(outdb.materializedOnDisk)

	println()
	println("Matches GMQL?")
	println(Reporter.countLines(outdb) == gmqlResult)


	// Move output files to folder
	outdb.savedAs(s"use_case_output")
	os.move(wd / s"use_case_output.sfsav",
		wd / "tests" / "output" / s"use_case_output" / s"use_case_output.sfsav",
		createFolders=true)

	SavUnwrapper.moveFiles(wd / "tests" / "output" / s"use_case_output" / s"use_case_output.sfsav",
		wd / "tests" / "output" / s"use_case_output")

	// Cleanup tmp files

	os.walk(tmp_folder,skip = (p: os.Path) => !(p.last.startsWith("synchrony-"))).foreach(os.remove(_))

	println(s"Test use_case done.")

}