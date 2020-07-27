// package timings

// import synchrony.gmql.GMQL
// import GMQL._
// import Predicates._
// import GMQL.Projections._
// import GMQL.GenomeLocus._
// import GMQL.DB._
// import GMQL.implicits._
// import synchrony.gmql.EncodeNP._

// import scala.language.postfixOps
// import scala.io.Source._
// import scala.collection.mutable.ListBuffer
// import scala.math

// import java.io.File
// import java.io.PrintWriter

// object Reporter {
// 	// Small object with auxiliary printing functions
// 	private def showSamples(db:DB) =
// 	  for(s <- db.samples.eiterator) { 
// 	    println(s); println("**")
// 	  }


// 	private def showTrack(sample:Sample) = for(e <- sample.track) println(e)
// 	private def showTrackHead(sample:Sample) = for(e <- sample.track.slice(0,10)) println(e)


// 	private def showSize(sample:Sample) =
// 	{
// 	  val size = sample.bedFile.filesize
// 	  val count = sample.track.length
// 	  println(s"track size = ${size/1000.0} kb, track count = ${count}")
// 	}

// 	private def showTotalSize(db:DB) = 
// 	{
// 		println(s"total track size = ${db.samples.totalsizeOnDisk/(1000.0*1000.0)} mb")
// 	}

// 	private def showTotalCount(db:DB) = 
// 	{
// 		var total: Int = 0
// 		for (s <- db.samples.eiterator)
// 			total += s.track.length
// 		println(s"total track count = ${total} lines")
// 	}

// 	def report(db:DB) =
// 	{
// 		showSamples(db)
// 		showTotalSize(db)
// 		showTotalCount(db)
// 		for ((s,count) <- db.samples.eiterator.zipWithIndex)
// 		{	
// 			println(s"Sample ${count}")
// 			print(showSize(s))
// 			showTrackHead(s)
// 		}
// 	}
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


// object GMQLSynchro_Timings extends App {

// 	// Set current working directory

// 	// Set debugging to false to save output
// 	synchrony.iterators.Serializers.DEBUG = false // remove printouts
// 	GMQL.Projections.DEBUG = false // remove printouts
// 	GMQL.DEBUG = false // remove printouts

// 	val wd = os.pwd

// 	val NEXEC = 30

// 	// clean up output folders

// 	os.list(wd / "temp").foreach(f => os.remove(f))

// 	os.remove.all(wd / "tests" / "output" / s"${test_name}_output")

// 	val byte2mb = 1024*1024

// 	val dataset1_name = "HG19_BED_ANNOTATION"

// 	val data_fldr = wd / "tests"

// 	val samplesdir = data_fldr / "input" / s"${dataset1_name}" / "files"
// 	val sampleslist = data_fldr / "input" / s"${dataset1_name}" / "list.txt"

// 	val sorttimes: ListBuffer[Double] = ListBuffer[Double]()

// 	var rdbFiles = altOnDiskEncodeNPDB(samplesdir.toString)(sampleslist.toString)

// 	// var rdb = rdbFiles.select(sample=OnSample(MetaS("annotation_type") === "TSS")).tracksSorted.materializedOnDisk
// 	// var rdb = rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk
// 	var rdb = rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq"),region=OnRegion(Strand === "+")).tracksSorted.materializedOnDisk
// 	// var rdb = rdbFiles.select().tracksSorted.materializedOnDisk

// 	for (i <- 0 to (NEXEC-1)) {

// 		println(s"Sort ${i}")

// 		var rdbFiles = altOnDiskEncodeNPDB(samplesdir.toString)(sampleslist.toString)

// 		var sortstart = System.nanoTime()

// 		// var rdb = rdbFiles.select(sample=OnSample(MetaS("annotation_type") === "TSS")).tracksSorted.materializedOnDisk
// 		// var rdb = rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk
// 		var rdb = rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq"),region=OnRegion(Strand === "+")).tracksSorted.materializedOnDisk
// 		// var rdb = rdbFiles.select().tracksSorted.materializedOnDisk

// 	    sorttimes += (System.nanoTime() - sortstart)/1e9

// 	}

// 	println("all sort times")
// 	println(sorttimes)
// 	def mean(l:ListBuffer[Double]): Double = l.reduceLeft(_ + _)/l.length
// 	def std(l:ListBuffer[Double]): Double = math.sqrt(mean(l.map((x:Double) => x * x)) - mean(l) * mean(l))
// 	println("average")
// 	println(mean(sorttimes))
// 	println("std")
// 	println(std(sorttimes))

// 	rdbFiles = altOnDiskEncodeNPDB(samplesdir.toString)(sampleslist.toString)

// 	// rdb = rdbFiles.select(sample=OnSample(MetaS("annotation_type") === "TSS")).tracksSorted.materializedOnDisk
// 	// rdb = rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk
// 	rdb = rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq"),region=OnRegion(Strand === "+")).tracksSorted.materializedOnDisk
// 	// rdb = rdbFiles.select().tracksSorted.materializedOnDisk

// 	os.list(wd / "temp").foreach(f => os.remove(f))

// 	println()
// 	println("input1")

// 	Reporter.report(rdb)


// 	val dataset2_name = "H1"


// 	val samplesdir2 = data_fldr / "input" / s"${dataset2_name}" / "files"
// 	val sampleslist2 = data_fldr / "input" / s"${dataset2_name}" / "list.txt"

// 	val sorttimes2: ListBuffer[Double] = ListBuffer[Double]()

// 	var edbFiles = altOnDiskEncodeNPDB(samplesdir2.toString)(sampleslist2.toString)

// 	// var edb = edbFiles.select(sample=OnSample(MetaS("annotation_type") === "TSS"),region=OnRegion(Strand === "-")).tracksSorted.materializedOnDisk
// 	// var edb = edbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk
// 	var edb = edbFiles.tracksSorted.materializedOnDisk

// 	for (i <- 0 to (NEXEC-1)) {

// 		println(s"Sort ${i}")

// 		var edbFiles = altOnDiskEncodeNPDB(samplesdir2.toString)(sampleslist2.toString)

// 		var sortstart = System.nanoTime()

// 		// var edb = edbFiles.select(sample=OnSample(MetaS("annotation_type") === "TSS"),region=OnRegion(Strand === "-")).tracksSorted.materializedOnDisk
// 		// var edb = edbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk
// 		var edb = edbFiles.tracksSorted.materializedOnDisk

// 	    sorttimes2 += (System.nanoTime() - sortstart)/1e9

// 	}

// 	println("all sort times")
// 	println(sorttimes2)

// 	println("average")
// 	println(mean(sorttimes2))
// 	println("std")
// 	println(std(sorttimes2))

// 	edbFiles = altOnDiskEncodeNPDB(samplesdir2.toString)(sampleslist2.toString)

// 	// edb = edbFiles.select(sample=OnSample(MetaS("annotation_type") === "TSS"),
// 	// 					  region=OnRegion(Strand === "-")).tracksSorted.materializedOnDisk
// 	// edb = edbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk
// 	edb = edbFiles.tracksSorted.materializedOnDisk


// 	os.list(wd / "temp").foreach(f => os.remove(f))

// 	println()
// 	println("input2")

// 	Reporter.report(edb)

// 	println("Starting execution")
// 	val times: ListBuffer[Double] = ListBuffer[Double]()


// 	val formatName = FunObj[String,String](s => s.replace("-human",""))
// 	val minusLog10 = FunObj[Double,Double](x => scala.math.log10 (1/x))
// 	var outdb = DB.map(region=OnRegion())(rdb,edb).materializedOnDisk

// 	for (i <- 0 to (NEXEC-1)) {

// 		println(s"Exec ${i}")

// 		var querystart = System.nanoTime()

// 		// this is the actual query
// 		outdb = DB.map(region=OnRegion())(rdb,edb).materializedOnDisk

// 	    times += (System.nanoTime() - querystart)/1e9

// 	}

// 	println()
// 	println("output")
// 	Reporter.report(outdb)

// 	// Move output files to folder
// 	outdb.savedAs(s"${test_name}_output")
// 	os.move(wd / s"${test_name}_output.sfsav",
// 		wd / "tests" / "output" / s"${test_name}_output" / s"${test_name}_output.sfsav",
// 		createFolders=true)

// 	SavUnwrapper.moveFiles(wd / "tests" / "output" / s"${test_name}_output" / s"${test_name}_output.sfsav",
// 		wd / "tests" / "output" / s"${test_name}_output")

// 	println("all execution times")
// 	println(times)
// 	println("average")
// 	println(mean(times))
// 	println("std")
// 	println(std(times))

// 	println(s"Test ${test_name} done.")

// }