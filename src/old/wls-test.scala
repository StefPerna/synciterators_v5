

import synchrony.gmql.GMQL._
import synchrony.gmql.GMQL.DB._
import synchrony.gmql.Samples._
import synchrony.gmql.Predicates._
import synchrony.gmql.Projections._
import Ordering.Double.TotalOrdering
import synchrony.genomeannot.GenomeAnnot.GenomeLocus._
import synchrony.genomeannot.GenomeAnnot._
import synchrony.genomeannot.BedWrapper._
import synchrony.iterators.SyncCollections._
import synchrony.iterators.SyncCollections.implicits._


object wlsMain extends App {
  val tfbsSample = 
    Sample(
      Map("provider" -> "UCSC", 
          "cell" -> "GM12878"),
      () => SimpleBedFile("./TFBS/TFBS-short/files/ENCFF188SZS.bed").iterator).
    sortT()

  val tssSample =
    Sample(
      Map("annotation_type" -> "TSS", "provider" -> "UCSC", "assembly" -> "hg19"),
      () => SimpleBedFile("./HG19_BED_ANNOTATION/files/TSS.bed").iterator).
    sortT

  val tfbsDB = DB(Vector(tfbsSample))
  val tssDB = DB(Vector(tssSample))

  def db = DB.join(
        limit = 100000,
        pred = Genometric(DGE(5000), DLE(100000)),
        outputR = LeftR)(tfbsDB, tssDB)

  def xx = db.samples.head.track()


  def zz = for(a <- tfbsSample.track(); b <- tssSample.track();
               if (Bed.cond(GenomeLocus.DLE(100000))(a, b));
               if (GenomeLocus.cond(DGE(5000), DLE(100000)))(a, b))
           yield (a, b, a.distFrom(b))

  // xx and zz should have the same length



  def db2 = DB.join(
        limit = 100000,
        outputR = LeftR)(tfbsDB, tssDB)

  def xx2 = db2.samples.head.track()

  def zz2 = for(a <- tfbsSample.track(); b <- tssSample.track();
               if (Bed.cond(GenomeLocus.DLE(100000))(a, b)))
           yield (a, b, a.distFrom(b))

// xx2 and zz2 should have the same length
}

