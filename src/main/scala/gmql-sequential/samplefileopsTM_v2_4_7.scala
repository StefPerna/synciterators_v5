
package synchrony.gmql

/** Support for timing studies
 *
 *  def aQuery = ...
 *  nRuns(n)(aQuery) runs aQuery n times.
 *
 * Wong Limsoon
 * 15 Oct 2020
 */


object SampleFileOpsTimings {

  import synchrony.iterators.AggrCollections.{ AggrIterator, OpG }
  import synchrony.iterators.SyncCollections.EIterator
  type SampleFile      = synchrony.gmql.SampleFileOps.SampleFile 


  class Timings {
    var times = Seq[Double]()
    def add(t: Double) = { times = t +: times }
    def stats = AggrIterator(times.iterator).flatAggregateBy(OpG.stats[Double](x => x))
  }


  def iaTimeIt(t: Timings)(samples: => SampleFile) = {
    type Bed       = synchrony.genomeannot.BedFileOps.Bed 
    type Transient = synchrony.iterators.FileCollections.Transient[Bed]
    val t0 = System.nanoTime;
    val it = samples.eiterator
    val _  = while (it.hasNext) {
               val s = it.next()
               s.bedFile.efile match {
                 case _: Transient =>
                   val e = s.bedFile.eiterator
                   while (e.hasNext) e.next()
                 case _ =>
               }
             }
    val dt = (System.nanoTime - t0) / 1e9d;
    // println(s"Time take: ${dt}")
    t.add(dt)
    dt
  }

  def nRuns(n: Int, t: Timings = new Timings)(samples: => SampleFile) = {
    var c: Int = 0
    var res: SampleFile = null
    while (c < n) {
      c = c + 1
      iaTimeIt(t)(samples)
    }
    (t, samples)
  }

} // End object SampleFileOpsTimings 


