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
import scala.math

import java.io.File
import java.io.PrintWriter

object Reporter {
	// Small object with auxiliary printing functions
	private def showSamples(db:DB) = db.samples.eiterator.foreach((s:Sample) => { println(s); println("**") } )

	private def showTrack(sample:Sample) = sample.track.foreach(println)

	private def showTrackHead(sample:Sample) = sample.track.slice(0,10).foreach(println)

	private def showSize(sample:Sample) = println(s"track size = ${sample.bedFile.filesize/1000.0} kb, track count = ${sample.track.length}")

	private def showTotalSize(db:DB) = println(s"total track size = ${db.samples.totalsizeOnDisk/(1000.0*1000.0)} mb")

	private def showTotalCount(db:DB) = println(s"total track count = ${countLines(db)} lines")

	private def mean(l:Seq[Double]): Double = l.reduceLeft(_ + _)/l.length
	
	private def std(l:Seq[Double]): Double = math.sqrt(mean(l.map((x:Double) => x * x)) - mean(l) * mean(l))
		

	def report(db:DB)
	{
		showSamples(db)
		showTotalSize(db)
		showTotalCount(db)
		db.samples.eiterator.zipWithIndex.foreach(
			{ case (s,count) => {println(s"Sample ${count}"); println(showSize(s)); showTrackHead(s)} }
		)
	}

	def sortProfiles(prf:Seq[(Double,Double)],name:String) {
		// Conv is used to convert memory to kb, mb, gb, etc.
		println(f"all ${name} profiles")
		println(prf)

		val times = prf map (_._1)
		val mems = prf map (_._2)

		println("average times")
		println(f"${mean(times)}%10.6f")
		println("time std")
		println(f"${std(times)}%10.6f")
		println("average mem")
		println(f"${mean(mems)}%10.6f")
		println("mem std")
		println(f"${std(mems)}%10.6f")
	}

	def countLines(db:DB) = db.samples.eiterator.foldLeft(0)((n,s) => n + s.track.length)
	// def countLines(db:DB) = for {s <- db.samples.eiterator} yield s.track.length
}

object SavUnwrapper {
	// reads and parses a sav file, then moves files from the temp variable folder


	def moveFiles(savpath:os.Path, tgtfldr:os.Path) {
		for (meta <- os.read.lines(savpath)
			if meta.split('\t')(0).startsWith("/var");
			p = os.Path(meta)) {
			os.move(p, tgtfldr / p.last)
		}
	}

}

object ErrorManagement {
	// reads and parses a sav file, then moves files from the temp variable folder

	def Try[A](a: => A): Either[Exception,A] = 
		try Right(a)
		catch { case e: Exception => Left(e)}

}


object GMQLSynchro_Demo extends App {

	synchrony.iterators.Serializers.DEBUG = false // remove printouts
	synchrony.iterators.FileCollections.DEBUG = false // remove printouts
	GMQL.Projections.DEBUG = false // remove printouts
	GMQL.DEBUG = false // remove printouts

	// Set current working directory
	val wd = os.pwd
	val tmp_folder = os.root / "var" / "folders" / "k8" / "sf3vvc_921d8r9n7qgy_7kh00000gn" / "T"

	val NEXEC = 10
	val test_name = "014_8"

	// clean up output folders

	os.list(wd / "temp").foreach(f => os.remove(f))


	os.remove.all(wd / "tests" / "output" / s"${test_name}_output")

	os.walk(tmp_folder,skip = (p: os.Path) => !(p.last.startsWith("synchrony-"))).foreach(os.remove(_))

	val byte2mb = 1024*1024

	val runtime = Runtime.getRuntime
	println(f"Total memory: ${runtime.totalMemory/byte2mb}")

	val usesH1DiffDatabasesAsInput1 = Set("001_6","001_7","002_4","002_5","002_6","002_7",
		"003_3","004_1","004_2","004_4","004_5","006_3",
		"007_3","007_4","007_5","007_6","007_10",
		"007_11","007_12")
	val usesH1AsInput1 = Set("001_1","001_2","001_3","001_5","002_1","007_7","002_1","002_2",
		"002_3","002_8","003_1","003_2","004_3")
	val usesHG19AsInput1 = Set("001_4","005_1","005_2","005_4","005_6","006_1","006_2",
		"007_1","007_2","007_8","007_9",
		"007_13","007_14","007_16")
	val usesSevenCellsAsInput1 = Set("005_5","007_15")
	val usesJoinRefAsInput1 = Set("join")
	val usesSSAsInput1 = Set("010_1","010_10","011_1","011_10","014_1","014_10")
	val usesSMAsInput1 = Set("010_2","010_11","011_2","011_11","014_2","014_11")
	val usesSBAsInput1 = Set("010_3","010_12","011_3","011_12","014_3","014_12")
	val usesMSAsInput1 = Set("010_4","010_13","011_4","011_13","014_4","014_13")
	val usesMMAsInput1 = Set("010_5","010_14","011_5","011_14","014_5","014_14")
	val usesMBAsInput1 = Set("010_6","010_15","011_6","011_15","014_6","014_15")
	val usesBSAsInput1 = Set("010_7","010_16","011_7","011_16","014_7","014_16")
	val usesBMAsInput1 = Set("010_8","010_17","011_8","011_17","014_8","014_17")
	val usesBBAsInput1 = Set("010_9","010_18","011_9","011_18","014_9","014_18")
	val uses1SampleAsInput1 = Set("012_1","013_1","015_1")
	val uses5SampleAsInput1 = Set("012_2","013_2","015_2")
	val uses10SampleAsInput1 = Set("012_3","013_3","015_3")
	val uses20SampleAsInput1 = Set("012_4","013_4","015_4")
	val uses50SampleAsInput1 = Set("012_5","013_5","015_5")
	val uses75SampleAsInput1 = Set("012_6","013_6","015_6")
	val uses100SampleAsInput1 = Set("012_7","013_7","015_7")
	val uses1LineAsInput1 = Set("012_8","013_8","015_8")
	val uses10LineAsInput1 = Set("012_9","013_9","015_9")
	val uses100LineAsInput1 = Set("012_10","013_10","015_10")
	val uses1000LineAsInput1 = Set("012_11","013_11","015_11")
	val uses10000LineAsInput1 = Set("012_12","013_12","015_12")
	val uses100000LineAsInput1 = Set("012_13","013_13","015_13")
	val uses1000000LineAsInput1 = Set("012_14","013_14","015_14")


	val hasNoInput2 = Set("010_1","010_2","010_3","010_4","010_5","010_6","010_7","010_8",
			"010_9","010_10","010_11","010_12","010_13","010_14","010_15","010_16","010_17",
			"010_18","012_1","012_1","012_2","012_3","012_4","012_5",
			"012_6","012_7","012_8","012_9","012_10","012_11","012_12","012_13",
			"012_14")
	val usesHeLaS3AsInput2 = Set("007_3","007_4","007_5","007_6","007_10","007_11","007_12")
	val usesH1AsInput2 = Set("001_1","001_2","001_3","001_4","001_5","001_6","001_7",
		"001_8","002_1","002_4","002_5","002_6","002_7","002_8","003_1","003_3","004_1",
		"004_2","004_3","004_4","004_5","005_1","005_2","006_1","006_2",
		"007_1","005_5","007_2","007_8","002_2","002_3","003_2","007_9",
		"007_13","007_16")
	val usesHG19AsInput2 = Set("005_6","007_15")
	val usesH1DiffDatabasesAsInput2 = Set("005_3","007_7")
	val usesSevenCellsAsInput2 = Set("005_4","007_14")
	val usesJoinExpAsInput2 = Set("join")
	val usesSSAsInput2 = Set("011_1","011_10","014_1","014_10")
	val usesSMAsInput2 = Set("011_2","011_11","014_2","014_11")
	val usesSBAsInput2 = Set("011_3","011_12","014_3","014_12")
	val usesMSAsInput2 = Set("011_4","011_13","014_4","014_13")
	val usesMMAsInput2 = Set("011_5","011_14","014_5","014_14")
	val usesMBAsInput2 = Set("011_6","011_15","014_6","014_15")
	val usesBSAsInput2 = Set("011_7","011_16","014_7","014_16")
	val usesBMAsInput2 = Set("011_8","011_17","014_8","014_17")
	val usesBBAsInput2 = Set("011_9","011_18","014_9","014_18")
	val uses1SampleAsInput2 = Set("013_1","015_1")
	val uses5SampleAsInput2 = Set("013_2","015_2")
	val uses10SampleAsInput2 = Set("013_3","015_3")
	val uses20SampleAsInput2 = Set("013_4","015_4")
	val uses50SampleAsInput2 = Set("013_5","015_5")
	val uses75SampleAsInput2 = Set("013_6","015_6")
	val uses100SampleAsInput2 = Set("013_7","015_7")
	val uses1LineAsInput2 = Set("013_8","015_8")
	val uses10LineAsInput2 = Set("013_9","015_9")
	val uses100LineAsInput2 = Set("013_10","015_10")
	val uses1000LineAsInput2 = Set("013_11","015_11")
	val uses10000LineAsInput2 = Set("013_12","015_12")
	val uses100000LineAsInput2 = Set("013_13","015_13")
	val uses1000000LineAsInput2 = Set("013_14","015_14")

	val gmqlResult = Map(
		"001_1" -> 12969,
		"001_2" -> 2071,
		"001_3" -> 5995,
		"001_4" -> 12,
		"001_5" -> 11111,
		"001_6" -> 14038,
		"001_8" -> 0,
		"002_1" -> 12969,
		"002_2" -> 12969,
		"002_4" -> 30702,
		"002_5" -> 30702,
		"002_6" -> 30702,
		"002_7" -> 30702,
		"002_8" -> 12969,
		"003_1" -> 12969,
		"003_3" -> 30702,
		"004_1" -> 30702,
		"004_2" -> 30702,
		"004_3" -> 9926,
		"004_4" -> 27659,
		"004_5" -> 27659,
		"005_1" -> 101306,
		"005_2" -> 101306,
		"005_3" -> 1681640,
		"005_4" -> 30290494,
		"005_5" -> 20718230,
		"005_6" -> 26004,
		"006_1" -> 36913,
		"006_2" -> 50653,
		"006_3" -> 6599,
		"007_1" -> 20320,
		"007_2" -> 1320,
		"007_3" -> 2605,
		"007_4" -> 2605,
		"007_5" -> 2557,
		"007_6" -> 2,
		"007_7" -> 29489,
		"007_8" -> 33122,
		"007_9" -> 33879,
		"007_10" -> 30440,
		"007_11" -> 11486,
		"007_12" -> 30440,
		"007_13" -> 165393,
		"007_16" -> 68002,
		"010_1" -> 160,
		"010_2" -> 1794,
		"010_3" -> 17938,
		"010_4" -> 1811,
		"010_5" -> 18202,
		"010_6" -> 184208,
		"010_7" -> 26985,
		"010_8" -> 181941,
		"010_9" -> 1786890,
		"011_1" -> 976,
		"011_2" -> 10059,
		"011_3" -> 100204,
		"011_4" -> 111370,
		"011_5" -> 1023580,
		"011_6" -> 10070620,
		"011_7" -> 16872000,
		"011_8" -> 101231400,
		"011_9" -> -1,
		"014_1" -> 1022,
		"014_2" -> 10879,
		"014_3" -> 100204,
		"014_4" -> 16711,
		"014_5" -> 183722,
		"014_6" -> 2794032,
		"014_7" -> 1022384,
		"014_8" -> 13048884,
		"014_9" -> -1,
		"019_1" -> 50000,
		"019_2" -> 50000,
		"019_3" -> 50000,
		"019_4" -> 50000,
		"019_5"-> 50000,
		"019_6" -> 50000,
		"019_7" -> 50000,
		"019_8" -> 50000,
		"019_9" -> 50000,
		"020_1" -> 100000,
		"020_2" -> 100000,
		"020_3" -> 100000,
		"020_4" -> 100000,
		"020_5" -> 100000,
		"020_6" -> 100000,
		"020_7" -> 100000,
		"020_8" -> 100000,
		"020_9" -> 100000,
	)



	val dataset1_name = test_name match {
		case name if usesH1DiffDatabasesAsInput1 contains name => "H1_diff_databases"
		case name if usesSevenCellsAsInput1 contains name => "Seven_cells"
		case name if usesH1AsInput1 contains name => "H1"
		case name if usesHG19AsInput1 contains name => "HG19_BED_ANNOTATION"
		case name if usesJoinRefAsInput1 contains name => "join_test_ref"
		case name if usesSSAsInput1 contains name => "SS"
		case name if usesSMAsInput1 contains name => "SM"
		case name if usesSBAsInput1 contains name => "SB"
		case name if usesMSAsInput1 contains name => "MS"
		case name if usesMMAsInput1 contains name => "MM"
		case name if usesMBAsInput1 contains name => "MB"
		case name if usesBSAsInput1 contains name => "BS"
		case name if usesBMAsInput1 contains name => "BM"
		case name if usesBBAsInput1 contains name => "BB"
		case name if uses1SampleAsInput1 contains name => "timings_1_sample"
		case name if uses5SampleAsInput1 contains name => "timings_5_samples"
		case name if uses10SampleAsInput1 contains name => "timings_10_samples"
		case name if uses20SampleAsInput1 contains name => "timings_20_samples"
		case name if uses50SampleAsInput1 contains name => "timings_50_samples"
		case name if uses75SampleAsInput1 contains name => "timings_75_samples"
		case name if uses100SampleAsInput1 contains name => "timings_100_samples"
		case name if uses1LineAsInput1 contains name => "timings_1_line"
		case name if uses10LineAsInput1 contains name => "timings_10_lines"
		case name if uses100LineAsInput1 contains name => "timings_100_lines"
		case name if uses1000LineAsInput1 contains name => "timings_1000_lines"
		case name if uses10000LineAsInput1 contains name => "timings_10000_lines"
		case name if uses100000LineAsInput1 contains name => "timings_100000_lines"
		case name if uses1000000LineAsInput1 contains name => "timings_1000000_lines"
		case "005_3" => "GM12878"
		case "019_1" => "map_graph_1_ref"
		case "019_2" => "map_graph_2_ref"
		case "019_3" => "map_graph_3_ref"
		case "019_4" => "map_graph_4_ref"
		case "019_5" => "map_graph_5_ref"
		case "019_6" => "map_graph_6_ref"
		case "019_7" => "map_graph_7_ref"
		case "019_8" => "map_graph_8_ref"
		case "019_9" => "map_graph_9_ref"
		case "019_10" => "map_graph_10_ref"
		case "020_1" => "map_graph_1_on_ref_ref"
		case "020_2" => "map_graph_2_on_ref_ref"
		case "020_3" => "map_graph_3_on_ref_ref"
		case "020_4" => "map_graph_4_on_ref_ref"
		case "020_5" => "map_graph_5_on_ref_ref"
		case "020_6" => "map_graph_6_on_ref_ref"
		case "020_7" => "map_graph_7_on_ref_ref"
		case "020_8" => "map_graph_8_on_ref_ref"
		case "020_9" => "map_graph_9_on_ref_ref"
		case "020_10" => "map_graph_10_on_ref_ref"
		case _ => throw new Exception("Unknown test case for dataset name 1")
	}

	val data_fldr = wd / "tests"

	val samplesdir = data_fldr / "input" / s"${dataset1_name}" / "files"
	val sampleslist = data_fldr / "input" / s"${dataset1_name}" / "list.txt"

	val processedAsBedPeaks = Set("HG19_BED_ANNOTATION","map_graph_1_ref",
		"map_graph_2_ref","map_graph_3_ref","map_graph_4_ref","map_graph_5_ref",
		"map_graph_6_ref","map_graph_7_ref","map_graph_8_ref","map_graph_9_ref",
		"map_graph_10_ref","map_graph_1_exp",
		"map_graph_2_exp","map_graph_3_exp","map_graph_4_exp","map_graph_5_exp",
		"map_graph_6_exp","map_graph_7_exp","map_graph_8_exp","map_graph_9_exp",
		"map_graph_10_exp",
		"map_graph_1_on_ref_ref",
		"map_graph_2_on_ref_ref","map_graph_3_on_ref_ref","map_graph_4_on_ref_ref","map_graph_5_on_ref_ref",
		"map_graph_6_on_ref_ref","map_graph_7_on_ref_ref","map_graph_8_on_ref_ref","map_graph_9_on_ref_ref",
		"map_graph_10_on_ref_ref","map_graph_1_on_ref_exp",
		"map_graph_2_on_ref_exp","map_graph_3_on_ref_exp","map_graph_4_on_ref_exp","map_graph_5_on_ref_exp",
		"map_graph_6_on_ref_exp","map_graph_7_on_ref_exp","map_graph_8_on_ref_exp","map_graph_9_on_ref_exp",
		"map_graph_10_on_ref_exp")


	var rdbFiles = dataset1_name match {
		case  name if processedAsBedPeaks contains name => altOnDiskDB(samplesdir.toString)(sampleslist.toString)
		case _ => altOnDiskEncodeNPDB(samplesdir.toString)(sampleslist.toString)
	}

	var usesTSS = Set("007_1","007_2","007_8","007_9","007_13","007_16")
	var usesGenes = Set("005_1","005_2","005_4","006_1","006_2","007_14")
	var usesOnlyPlusStrand = Set("005_6")

	val dataset2_name: Option[String] = test_name match {
		case name if hasNoInput2 contains name => None
		case name if usesHeLaS3AsInput2 contains name => Some("HeLa-S3")
		case name if usesSevenCellsAsInput2 contains name => Some("Seven_cells")
		case name if usesH1AsInput2 contains name => Some("H1")
		case name if usesHG19AsInput2 contains name => Some("HG19_BED_ANNOTATION")
		case name if usesH1DiffDatabasesAsInput2 contains name => Some("H1_diff_databases")
		case name if usesJoinRefAsInput1 contains name => Some("join_test_exp")
		case name if usesSSAsInput2 contains name => Some("SS")
		case name if usesSMAsInput2 contains name => Some("SM")
		case name if usesSBAsInput2 contains name => Some("SB")
		case name if usesMSAsInput2 contains name => Some("MS")
		case name if usesMMAsInput2 contains name => Some("MM")
		case name if usesMBAsInput2 contains name => Some("MB")
		case name if usesBSAsInput2 contains name => Some("BS")
		case name if usesBMAsInput2 contains name => Some("BM")
		case name if usesBBAsInput2 contains name => Some("BB")
		case name if uses1SampleAsInput2 contains name => Some("timings_1_sample")
		case name if uses5SampleAsInput2 contains name => Some("timings_5_samples")
		case name if uses10SampleAsInput2 contains name => Some("timings_10_samples")
		case name if uses20SampleAsInput2 contains name => Some("timings_20_samples")
		case name if uses50SampleAsInput2 contains name => Some("timings_50_samples")
		case name if uses75SampleAsInput2 contains name => Some("timings_75_samples")
		case name if uses100SampleAsInput2 contains name => Some("timings_100_samples")
		case name if uses1LineAsInput2 contains name => Some("timings_1_line")
		case name if uses10LineAsInput2 contains name => Some("timings_10_lines")
		case name if uses100LineAsInput2 contains name => Some("timings_100_lines")
		case name if uses1000LineAsInput2 contains name => Some("timings_1000_lines")
		case name if uses10000LineAsInput2 contains name => Some("timings_10000_lines")
		case name if uses100000LineAsInput2 contains name => Some("timings_100000_lines")
		case name if uses1000000LineAsInput2 contains name => Some("timings_1000000_lines")
		case "006_3" => Some("A549")
		case "019_1" => Some("map_graph_1_exp")
		case "019_2" => Some("map_graph_2_exp")
		case "019_3" => Some("map_graph_3_exp")
		case "019_4" => Some("map_graph_4_exp")
		case "019_5" => Some("map_graph_5_exp")
		case "019_6" => Some("map_graph_6_exp")
		case "019_7" => Some("map_graph_7_exp")
		case "019_8" => Some("map_graph_8_exp")
		case "019_9" => Some("map_graph_9_exp")
		case "019_10" => Some("map_graph_10_exp")
		case "020_1" => Some("map_graph_1_on_ref_exp")
		case "020_2" => Some("map_graph_2_on_ref_exp")
		case "020_3" => Some("map_graph_3_on_ref_exp")
		case "020_4" => Some("map_graph_4_on_ref_exp")
		case "020_5" => Some("map_graph_5_on_ref_exp")
		case "020_6" => Some("map_graph_6_on_ref_exp")
		case "020_7" => Some("map_graph_7_on_ref_exp")
		case "020_8" => Some("map_graph_8_on_ref_exp")
		case "020_9" => Some("map_graph_9_on_ref_exp")
		case "020_10" => Some("map_graph_10_on_ref_exp")
		case _ =>  throw new Exception("Unknown test case for dataset name 2")
	}


	val samplesdir2 = data_fldr / "input" / s"${dataset2_name.getOrElse("INVALID")}" / "files"
	val sampleslist2 = data_fldr / "input" / s"${dataset2_name.getOrElse("INVALID")}" / "list.txt"

	val edbFiles: Option[SampleFile] = dataset2_name match {
		case None => None
		case Some(name) => name match {
			case value if processedAsBedPeaks contains value => Some(altOnDiskDB(samplesdir2.toString)(sampleslist2.toString))
			case _ => Some(altOnDiskEncodeNPDB(samplesdir2.toString)(sampleslist2.toString))
		}
	}

	usesTSS = Set()
	usesGenes = Set("007_15")
	usesOnlyPlusStrand = Set("005_6")

	// BEGIN EXECUTION
	System.gc()
	val initMem = (runtime.totalMemory - runtime.freeMemory)

	// val preloadRdb = test_name match {
	// 	case name if usesTSS contains name => rdbFiles.select(sample=OnSample(MetaS("annotation_type") === "TSS")).tracksSorted.materializedOnDisk;
	// 	case name if usesGenes contains name => rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk;
	// 	case name if usesOnlyPlusStrand contains name => rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq"),
	// 																	 region=OnRegion(Strand === "+")).tracksSorted.materializedOnDisk;
	// 	case _ => rdbFiles.select().tracksSorted.materializedOnDisk

	// }

	// val rdbSorttimes = for (i <- 0 to (NEXEC-1)) yield {
	// 	((t:Long) => {
	// 		test_name match {
	// 		case name if usesTSS contains name => rdbFiles.select(sample=OnSample(MetaS("annotation_type") === "TSS")).tracksSorted.materializedOnDisk;
	// 		case name if usesGenes contains name => rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk
	// 		case name if usesOnlyPlusStrand contains name => rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq"),
	// 																		 region=OnRegion(Strand === "+")).tracksSorted.materializedOnDisk;		
	// 		case _ => rdbFiles.select().tracksSorted.materializedOnDisk
	// 		}; 
	// 		(System.nanoTime()-t)/1e9
	// 	}) (System.nanoTime())
	// }

	System.gc()
	val rdb = test_name match {
		case name if usesTSS contains name => rdbFiles.select(sample=OnSample(MetaS("annotation_type") === "TSS")).tracksSorted.materializedOnDisk;
		case name if usesGenes contains name => rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk
		case name if usesOnlyPlusStrand contains name => rdbFiles.select(sample=OnSample(MetaS("provider") === "RefSeq"),
																		 region=OnRegion(Strand === "+")).tracksSorted.materializedOnDisk;		
		case _ => rdbFiles.select().tracksSorted.materializedOnDisk

	}

	val rdbMem = (runtime.totalMemory - runtime.freeMemory)

	os.list(wd / "temp").foreach(f => os.remove(f))

	// Reporter.sortTimes(rdbSorttimes,"input1 sort")

	println()
	println("input1")

	Reporter.report(rdb)


	// val preloadEdb:Option[DB] = edbFiles match {
	// 	case None => None
	// 	case Some(samplefile) => test_name match {
	// 		case name if usesTSS contains name => Some(samplefile.select(sample=OnSample(MetaS("annotation_type") === "TSS")).tracksSorted.materializedOnDisk)
	// 		case name if usesGenes contains name => Some(samplefile.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk)
	// 		case name if usesOnlyPlusStrand contains name => Some(samplefile.select(sample=OnSample(MetaS("annotation_type") === "TSS"),
	// 																		 region=OnRegion(Strand === "+")).tracksSorted.materializedOnDisk)
	// 		case _ => Some(samplefile.select().tracksSorted.materializedOnDisk)
	// 	}
	// }


	// val edbSorttimes = edbFiles match {
	// 	case None => Vector()
	// 	case Some(samplefile) => for (i <- 0 to (NEXEC-1)) yield {
	// 		((t:Long) => {
	// 			test_name match {
	// 			case name if usesTSS contains name => samplefile.select(sample=OnSample(MetaS("annotation_type") === "TSS")).tracksSorted.materializedOnDisk;
	// 			case name if usesGenes contains name => samplefile.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk
	// 			case name if usesOnlyPlusStrand contains name => samplefile.select(sample=OnSample(MetaS("provider") === "RefSeq"),
	// 																			 region=OnRegion(Strand === "+")).tracksSorted.materializedOnDisk;		
	// 			case _ => samplefile.select().tracksSorted.materializedOnDisk
	// 			}; 
	// 			(System.nanoTime()-t)/1e9
	// 		}) (System.nanoTime())
	// 	}
	// }

	System.gc()
	val edb:Option[DB] = edbFiles match {
		case None => None
		case Some(samplefile) => test_name match {
			case name if usesTSS contains name => Some(samplefile.select(sample=OnSample(MetaS("annotation_type") === "TSS")).tracksSorted.materializedOnDisk)
			case name if usesGenes contains name => Some(samplefile.select(sample=OnSample(MetaS("provider") === "RefSeq")).tracksSorted.materializedOnDisk)
			case name if usesOnlyPlusStrand contains name => Some(samplefile.select(sample=OnSample(MetaS("annotation_type") === "TSS"),
																			 region=OnRegion(Strand === "+")).tracksSorted.materializedOnDisk)
			case _ => Some(samplefile.select().tracksSorted.materializedOnDisk)
		}
	}


	val edbMem = (runtime.totalMemory - runtime.freeMemory)

	os.list(wd / "temp").foreach(f => os.remove(f))

	// ErrorManagement.Try(Reporter.sortTimes(edbSorttimes,"input2 sort")) match {
	// 	case Left(e) => println("No sort times for input 2: " +  e)
	// 	case Right(_) =>
	// }

	println()
	println("input2")

	ErrorManagement.Try(edb.map(x => Reporter.report(x))) match {
		case Left(e) => println("No input 2")
		case Right(v) => println(v) 
	}

	println("Starting execution")

	val formatName = FunObj[String,String](s => s.replace("-human",""))
	val minusLog10 = FunObj[Double,Double](x => scala.math.log10 (1/x))

	def queries(test_name:String,input: List[DB]) = input match {
		case db1 :: Nil => singleDBQueries(test_name)(db1)
		case db1 :: db2 :: Nil => pairDBQueries(test_name)(db1,db2)
		case _ => throw new Exception("Wrong amount of dbs")
	}

	val singleDBQueries = Map[String, DB => DB](
		"001_1" -> DB.select(),
		"001_2" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"001_3" -> DB.select(sample=OnSample(MetaS[String]("TF") === "REST-human")),
		"001_4" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2",Strand !== "+", Start >= 500000, End <= 600000)),
		"001_5" -> DB.select(region=OnRegion(MetaR[Double]("qval") >= 4.1)),
		"001_6" -> DB.select(sample=OnSample(MetaS("TF") !== "FOSL1",SemiJoin(excl=Excl("database"), exDB=edb.getOrElse(rdb).samples))),
		"001_7" -> DB.select(sample=OnSample((Average of MetaR[Double]("qval")) > (Biggest of MetaR[Double]("pval")))),
		"001_8" -> DB.select(region=OnRegion(Chr === "strange" or Chr === "funny")),
		"002_1" -> DB.project(region=OnRegion("length" as (End - Start))),
		"002_2" -> DB.project(sample=OnSample("TF","database")),
		"002_3" -> DB.project(sample=OnSample("target" as (formatName(MetaS[String]("TF"))))),
		"002_4" -> DB.project(region=OnRegion("chromStart" as Start - 2000,"chromEnd" as Start + 1000)),
		"002_5" -> DB.project(sample=OnSample("database","TF"),region=OnRegion("signalval","qval")),
		"002_6" -> {case db: DB => DB.extend(sample=OnSample("regcount" as Count))(db).project(sample=OnSample("regcount","scaled_regcount" as (MetaS("regcount") / 1000.0), "rescaled_regcount" as (MetaS("regcount") / 1000000.0)))},
		"002_7" -> DB.project(),
		"002_8" -> DB.project(region=OnRegion("chrom1" as Chr, "start1" as Start, "end1" as End, "strand1" as Strand)),
		"003_1" -> DB.extend(sample=OnSample("region_count" as Count)),
		"003_2" -> DB.extend(sample=OnSample("region_count" as Count, "min_qvalue" as (Biggest of minusLog10(MetaR[Double]("qval"))))),
		"003_3" -> DB.extend(sample=OnSample("region_count" as Count, "min_qvalue" as (Smallest of MetaR[Double]("qval")))),
		"004_1" -> {case db: DB => DB.extend(sample=OnSample("region_count" as Count))(db).flatGroupBy(sample=BySample(grp= MetaS[String]("database"), aggr= "MaxSize" as (Biggest of MetaS[Double]("region_count"))))},
		"004_2" -> DB.flatGroupBy(sample=BySample(grp = MetaS[String]("database"),aggr= "n_samp" as Count)),
		"004_3" -> DB.flatGroupBy(region=ByRegion(grp=locusOnly,aggr="regNum" as Count)),
		"004_4" -> DB.flatGroupBy(region=ByRegion(grp = Score,aggr="avg_pvalue" as (Average of MetaR[Double]("pval")),"max_qvalue" as (Biggest of MetaR[Double]("qval")))),
		"004_5" -> {case db: DB => DB.extend(sample=OnSample("regnum" as Count))(db).flatGroupBy(sample= BySample(grp = MetaS[String]("database"),aggr= "max_reg" as (Biggest of MetaS("regnum"))),region= ByRegion(grp = locusOnly,aggr= "min_signal" as (Smallest of MetaR("signalval"))))},
		"010_1" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_2" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_3" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_4" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_5" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_6" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_7" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_8" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_9" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_10" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_11" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_12" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_13" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_14" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_15" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_16" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_17" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"010_18" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_1" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_2" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_3" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_4" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_5" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_6" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_7" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_8" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_9" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_10" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_11" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_12" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_13" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		"012_14" -> DB.select(region=OnRegion(Chr === "chr1" or Chr === "chr2")),
		// "join" -> DB.join(pred = Genometric(DL(20)), outputR = IntR(),joinbyS = OnSample("case")) (rdb,edb)
	)

	val pairDBQueries = Map[String, (DB,DB) => DB](
		"005_1" -> DB.map(region=OnRegion()),
		"005_2" -> DB.map(region=OnRegion("avg_score" as (Average of Score))),
		"005_3" -> DB.map(region=OnRegion("minScore" as (Smallest of Score),"reg_num" as Count),joinby=OnSample("database")),
		"005_4" -> DB.map(region=OnRegion("avgSig" as (Average of MetaR[Double]("signalval")),"reg_num" as Count)),
		"005_5" -> DB.map(region=OnRegion("avgSig" as (Average of MetaR[Double]("signalval")),"reg_num" as Count)),
		"005_6" -> DB.map(region=OnRegion()),
		"006_1" -> DB.difference(),
		"006_2" -> DB.difference(exact=true),
		"006_3" -> DB.difference(joinby=OnSample("database")),
		"007_1" -> DB.join(limit = 30000, pred = Genometric(DL(30000)), outputR = LeftR, orderR  = DistinctR),
		"007_2" -> DB.join(limit   = 100, pred    = Genometric(DL(100)), outputR = BothR()),
		"007_3" -> DB.join(limit = 20, pred = Genometric(DL(20)), outputR = IntR(),joinbyS = OnSample("database")),
		"007_4" -> DB.join(pred = Genometric(DLE(0)), outputR = IntR(), joinbyS = OnSample("database")) ,
		"007_5" -> DB.join(pred = Genometric(Overlap(20)), outputR = IntR(), joinbyS = OnSample("database")) ,
		"007_6" -> DB.join(pred = Genometric(Touch), outputR = LeftR, joinbyS = OnSample("database")) ,
		"007_7" -> DB.join(pred = Genometric(Overlap(1)), outputR = LeftR, joinbyR = OnRegion("score")) ,
		"007_8" -> DB.joinNearest(md = 1, predA = Genometric(StartBefore), outputR = RightR),
		"007_9" -> DB.joinNearest(md = 1, predB = Genometric(StartAfter), outputR = RightR),
		"007_10" -> DB.joinNearest(limit=500000,predB = Genometric(DGE(120000)), md=1,outputR = RightR,joinbyS=OnSample("database")),
		"007_11" -> DB.joinNearest(limit=500000,predB = Genometric(DGE(120000)), md=1,outputR = RightR,orderR=DistinctR,joinbyS=OnSample("database")),
		"007_12" -> DB.joinNearest(limit=500000,predB = Genometric(DGE(120000)), md=1,outputR = CatR(), joinbyS = OnSample("database")),
		"007_13" -> DB.join(limit=100000, pred = Genometric(DLE(10000),DGE(5000)), outputR = LeftR) ,
		"007_14" -> DB.join(limit=100000, pred = Genometric(DLE(10000),DGE(5000)), outputR = LeftR) ,
		"007_15" -> DB.join(limit=100000, pred = Genometric(DLE(10000),DGE(5000)), outputR = LeftR) ,
		"007_16" -> DB.joinNearest(md=1,outputR = RightR),
		"011_1" -> DB.map(region=OnRegion()),
		"011_2" -> DB.map(region=OnRegion()),
		"011_3" -> DB.map(region=OnRegion()),
		"011_4" -> DB.map(region=OnRegion()),
		"011_5" -> DB.map(region=OnRegion()),
		"011_6" -> DB.map(region=OnRegion()),
		"011_7" -> DB.map(region=OnRegion()),
		"011_8" -> DB.map(region=OnRegion()),
		"011_9" -> DB.map(region=OnRegion()),
		"011_10" -> DB.map(region=OnRegion()),
		"011_11" -> DB.map(region=OnRegion()),
		"011_12" -> DB.map(region=OnRegion()),
		"011_13" -> DB.map(region=OnRegion()),
		"011_14" -> DB.map(region=OnRegion()),
		"011_15" -> DB.map(region=OnRegion()),
		"011_16" -> DB.map(region=OnRegion()),
		"011_17" -> DB.map(region=OnRegion()),
		"011_18" -> DB.map(region=OnRegion()),
		"013_1" -> DB.map(region=OnRegion()),
		"013_2" -> DB.map(region=OnRegion()),
		"013_3" -> DB.map(region=OnRegion()),
		"013_4" -> DB.map(region=OnRegion()),
		"013_5" -> DB.map(region=OnRegion()),
		"013_6" -> DB.map(region=OnRegion()),
		"013_7" -> DB.map(region=OnRegion()),
		"013_8" -> DB.map(region=OnRegion()),
		"013_9" -> DB.map(region=OnRegion()),
		"013_10" -> DB.map(region=OnRegion()),
		"013_11" -> DB.map(region=OnRegion()),
		"013_12" -> DB.map(region=OnRegion()),
		"013_13" -> DB.map(region=OnRegion()),
		"013_14" -> DB.map(region=OnRegion()),
		"014_1" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()) ,
		"014_2" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_3" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_4" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_5" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_6" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_7" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_8" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_9" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_10" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()) ,
		"014_11" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_12" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_13" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_14" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_15" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_16" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_17" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"014_18" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_1" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_2" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_3" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_4" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_5" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_6" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_7" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_8" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_9" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_10" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_11" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_12" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_13" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"015_14" -> DB.join(pred = Genometric(Overlap(1)), outputR = BothR()),
		"019_1" -> DB.map(region=OnRegion()),
		"019_2" -> DB.map(region=OnRegion()),
		"019_3" -> DB.map(region=OnRegion()),
		"019_4" -> DB.map(region=OnRegion()),
		"019_5" -> DB.map(region=OnRegion()),
		"019_6" -> DB.map(region=OnRegion()),
		"019_7" -> DB.map(region=OnRegion()),
		"019_8" -> DB.map(region=OnRegion()),
		"019_9" -> DB.map(region=OnRegion()),
		"019_10" -> DB.map(region=OnRegion()),
		"020_1" -> DB.map(region=OnRegion()),
		"020_2" -> DB.map(region=OnRegion()),
		"020_3" -> DB.map(region=OnRegion()),
		"020_4" -> DB.map(region=OnRegion()),
		"020_5" -> DB.map(region=OnRegion()),
		"020_6" -> DB.map(region=OnRegion()),
		"020_7" -> DB.map(region=OnRegion()),
		"020_8" -> DB.map(region=OnRegion()),
		"020_9" -> DB.map(region=OnRegion()),
		"020_10" -> DB.map(region=OnRegion()),
	// 	"join" -> DB.join(pred = Genometric(DL(20)), outputR = IntR(),joinbyS = OnSample("case")) (rdb,edb)
	)
	def outdb = test_name match {
		case s if singleDBQueries contains s => queries(test_name,List(rdb))
		case s if pairDBQueries contains s => queries(test_name,List(rdb,edb.getOrElse(throw new Exception("DB2 not found"))))
		case _ => throw new Exception("Unknown test name")
	}
	// Preload
	Reporter.countLines(outdb.materializedOnDisk)

	val execProfile:IndexedSeq[(Double,Double)] = for (i <- 0 to (NEXEC-1)) yield { System.gc();
		((t:Long,m:Long) => {println(Reporter.countLines(outdb)); ((System.nanoTime()-t)/1e9,(runtime.totalMemory - runtime.freeMemory - m)/byte2mb.toDouble)}) (System.nanoTime(),runtime.totalMemory - runtime.freeMemory)
		// ((t:Long) => {Reporter.countLines(outdb).foreach(println); (System.nanoTime()-t)/1e9}) (System.nanoTime())
	}

	println()

	val writeProfiles:IndexedSeq[(Double,Double)] = for (i <- 0 to (NEXEC-1)) yield {System.gc()
		((t:Long,m:Long) => {println(Reporter.countLines(outdb.materializedOnDisk)); ((System.nanoTime()-t)/1e9,(runtime.totalMemory - runtime.freeMemory - m)/byte2mb.toDouble)}) (System.nanoTime(),runtime.totalMemory - runtime.freeMemory)
	}

	println()

	println("rdb memory usage")
	println((rdbMem-initMem)/byte2mb)

	println("edb memory usage")
	println((edbMem-rdbMem-initMem)/byte2mb)

	Reporter.sortProfiles(execProfile,"execution")
	println()
	Reporter.sortProfiles(writeProfiles,"writing")

	println()
	println("output")
	Reporter.report(outdb.materializedOnDisk)

	println()
	println("Matches GMQL?")
	println(ErrorManagement.Try(gmqlResult(test_name)) match {
		case Left(e) => "No gmql data available"
		case Right(value) => Reporter.countLines(outdb) == value
	})


	// Move output files to folder
	outdb.savedAs(s"${test_name}_output")
	os.move(wd / s"${test_name}_output.sfsav",
		wd / "tests" / "output" / s"${test_name}_output" / s"${test_name}_output.sfsav",
		createFolders=true)

	SavUnwrapper.moveFiles(wd / "tests" / "output" / s"${test_name}_output" / s"${test_name}_output.sfsav",
		wd / "tests" / "output" / s"${test_name}_output")

	val res = os.proc("zip", "-rj", 
		wd / s"${test_name}_output.zip", 
		wd / "tests" / "output" / s"${test_name}_output").call()



	// // Move log and output to dropbox folder

	// val dropbox_fldr = os.home / "Dropbox" / "nus" / "synchrony_iterators" / "tests"

	// os.copy.over(wd / "log.txt",
	// 	dropbox_fldr / s"${test_name}" / "log.txt",
	// 	createFolders=false)

	// os.move.over(wd / s"${test_name}_output.zip",
	// 	dropbox_fldr / s"${test_name}" / "output"/ s"${test_name}_output.zip",
	// 	createFolders=false)

	// Cleanup tmp files

	os.walk(tmp_folder,skip = (p: os.Path) => !(p.last.startsWith("synchrony-"))).foreach(os.remove(_))

	println(s"Test ${test_name} done.")

}