

package synchrony
package gmql


//
// Wong Limsoon
// 4/4/2020
//


object Samples {

  import synchrony.genomeannot.GenomeAnnot._
  import synchrony.genomeannot.GenomeAnnot.GenomeLocus._
  import synchrony.genomeannot.BedWrapper._
  import synchrony.iterators.SyncCollections.ExTrack
  import java.io._

  type Bed = SimpleBedEntry
  type Meta = Map[String,Any]
  

  case class Sample(meta:Meta, bedFile:BedFile)
  {
    def track = bedFile.iterator

    def trackSz = bedFile.filesize

    def cond(ps:Sample.SBPred*)(u:Bed) = Sample.cond(ps:_*)(this, u)
    def cond(ps:Sample.SPred*) = Sample.cond(ps:_*)(this)

    def hasM(f:String) = meta.contains(f)

    def getM[T](f:String) = meta(f).asInstanceOf[T]

    def checkM[T](f:String, chk:T=>Boolean) =
      meta.get(f) match { 
        case None => false
        case Some(a) => chk(a.asInstanceOf[T]) }

    def updateM(f:String, v:Any) = Sample(meta + (f -> v), bedFile) 

    def mergeM(m:Meta) = Sample(meta ++ m, bedFile)

    def overwriteM(m:Meta) = Sample(m ++ meta, bedFile)

    def delM(f:String) = Sample(meta - f, bedFile)

    def eraseM() = Sample(meta.empty, bedFile)
    
    def renameM(f:String=>String) = 
      Sample(for((k,v)<-meta) yield f(k)->v, bedFile)


    def sortT() = sortTBy(SimpleBedEntry.ordering)

    def sortTByChromEnd() = sortTBy(SimpleBedEntry.orderByChromEnd)

    def sortTByChromStart() = sortTBy(SimpleBedEntry.orderByChromStart)

    def sortTBy(ordering:Ordering[Bed]) =
      Sample(meta, InMemoryBedFile(track.toVector.sorted(ordering)))


    def sortTWith(cmp:(Bed,Bed)=>Boolean) =
      Sample(meta, InMemoryBedFile(track.toVector.sortWith(cmp)))
    


    def updateT(it:Iterator[Bed], estimatedSz:Long = 0L) =
      Sample(meta, BedFile(it, estimatedSz))



    def mergeT(it:Iterator[Bed], estimatedSz:Long = 0L) =
    {
      def merged = new Iterator[Bed]
      {
         val it1 = ExTrack(track)
         val it2 = ExTrack(it)

         override def hasNext = (it1.hasNext || it2.hasNext) 

         override def next() = 
           if (it1.hasNext && !it2.hasNext) it1.next() else
           if (!it1.hasNext && it2.hasNext) it2.next() else
           { if (it1.shadow().next() endBefore it2.shadow().next())
               it1.next()
             else it2.next() 
           }
      } 

      updateT(merged, estimatedSz + trackSz)
    }

    def eraseTM() = updateT(
      for(r <- track) yield r.eraseMisc(),
      trackSz)

    def eraseMandTM() = eraseM().eraseTM()
  }



  object Sample
  {
    //
    // Placeholders for defining predicates
    // on a sample. These predicates are
    // defined elsewhere, e.g. predicates.scale.

    import synchrony.gmql.Predicates

    type SBPred = Predicates.SBPred
    type SPred = Predicates.SPred
      
    def cond(ps:SBPred*)(s:Sample, u:Bed) = ps.forall(p => p(s,u))
    def cond(ps:SPred*)(s:Sample) = ps.forall(p => p(s))
  }

}




