import synchrony.iterators.MiscCollections._
import synchrony.iterators.AggrCollections._
import synchrony.iterators.AggrCollections.OpG._
import synchrony.iterators.AggrCollections.implicits._
import synchrony.iterators.SyncCollections._
import synchrony.iterators.SyncCollections.implicits._
import synchrony.programming.Sri._
import synchrony.genomeannot.GenomeAnnot._
import synchrony.genomeannot.GenomeAnnot.GenomeLocus._
import synchrony.genomeannot.BedWrapper._

import scala.language.postfixOps

object Main extends App {
	println("I'm alive!")

	val data_fldr: String = "tests/data"

	type Bed = SimpleBedEntry  // What is this?
	def genes = SimpleBedFile(s"$data_fldr/ncbiRefSeqCurated_sorted.txt").iterator
	def peaks = SimpleBedFile(s"$data_fldr/enc_gm12878_ep300_optimal_sorted.bed").iterator


	def connect(
  		lmtrack:Iterator[Bed],
  		extrack:Iterator[Bed],
  		isBefore:(Bed,Bed)=>Boolean = GenomeLocus.isBefore _,
  		canSee:(Bed,Bed)=>Boolean = GenomeLocus.canSee(1000) _,
  		exclusiveCanSee: Boolean = false) : (LmTrack[Bed],SyncedTrack[Bed,Bed]) =
	{
  		val itB = LmTrack(lmtrack)
  		val itA = itB.sync(
  			it = extrack,
  			isBefore = isBefore,
  			canSee = canSee,
  			exclusiveCanSee = exclusiveCanSee)
  		return (itB, itA)
  	}

	def join(
	  lmtrack:LmTrack[Bed],
	  extrack:Iterator[Bed],
	  isBefore:(Bed,Bed)=>Boolean = GenomeLocus.isBefore _,
	  canSee:(Bed,Bed)=>Boolean = GenomeLocus.canSee(1000) _,
	  exclusiveCanSee: Boolean = false) : SyncedTrack[Bed,Bed] =
	{
	  lmtrack.sync(
	     it = extrack,
	     isBefore = isBefore,
	     canSee = canSee,
	     exclusiveCanSee = exclusiveCanSee)
	}
	def joinN(n:Int)(
  		lmtrack:LmTrack[Bed],
  		extrack:Iterator[Bed]) : SyncedTrack[Vector[Bed],Bed] =
	{ 
		def isBefore(gp:Vector[Bed],y:Bed) = gp.last endBefore y
		def canSee(gp:Vector[Bed],y:Bed) = y.between(gp.head,gp.last)

		  lmtrack.sync(
		     it = SlidingIteratorN(n)(extrack),
		     isBefore = isBefore,
		     canSee = canSee,
		     exclusiveCanSee = false)
	}

	def printres[A](it:Iterator[A]) = it.foreach(x => println(x))
	def countres[A](it:Iterator[A]) = it.length

	//
	// Example 1----------
	//
	// Find for each chromosome, the number of genes in it.
	//
	//
	// SOLUTION: Use Synchrony's groupby with aggregate function count.


	printres(genes.groupby(_.chrom)(count))

	// WORKS. THIS CAN BE TURNED INTO A SCALATEST USING THE EXCEL RESULTS
}