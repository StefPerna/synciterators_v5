
package synchrony
package iterators  

//
// Wong Limsoon
// 5/1/2020
//
// This module implements Synchrony's main iterator structure.
// The structure enables a "landmark track" to "synchronize"
// one or more "experiment track". Two predicates, isBefore 
// and canSee, are used to define the synchronization. 
// Intuitively, canSee(x,y) means the element x on an experiment
// track "can see" (i.e. is synchronized with the element y
// on the landmark track. And isBefore(x,y) means the position
// or locus of the element x on an experiment track corresponds
// to some position or locus in front of the position or locus
// of the element y on the landmark track.
//
// Synchrony makes some assumptions to efficiently synchronize
// iterators on the landmark and experiment tracks. These
// assumptions are the convexicity conditions (i) and (ii), and
// the monotonicity conditions (iii) and (iv) below. Let x and y
// be elements on a specific track (i.e. in the same file).
// Let x << y means the physical position of x is in front of 
// the physical position of y in the file (i.e. an iterator on
// the track or file is going to read/produce x before y. Then 
// the convexicity and monotonicity conditions sufficient to
// ensure correctness are:
//
// (i) For all x << y << z, and u: If (isBefore(x,u) or canSee(x,u))
//       and (isBefore(z,u) or canSee(z,u)), then (isBefore(y,u) or
//       canSee(y,u)).
//
// (ii) For all u << v << w, and y: If (isBefore(y,u) or canSee(y,u))
//      and (isBefore(y,w) or canSee(y,w)), then (isBefore(y,v) or
//      canSee(y,v)).
//
// (iii) For all u << v, and y: If isBefore(y,u), then isBefore(y,v).
//
// (iv) For all x << y, and v: If isBefore(y,v), then isBefore(x,v).
//


object SyncCollections {

  import scala.collection.mutable.ArrayBuffer
  import synchrony.iterators.MiscCollections._


  case class SyncError(msg: String) extends Throwable


  type ExTrack[A] = Iterator[A] with Shadowing[A] 

  object ExTrack
  { //
    // An "experiment track", which is a track to be synced to
    // a "landmark track", is defined simply as an iterator[A]
    // that has shadowing ability. The shadowing ability is
    // needed for implementing sync with non-exclusive canSee.

    def apply[A](it: Iterator[A]): ExTrack[A] = ShadowedIterator[A](it) 
  }


  type LmTrack[B] = Iterator[B] with Synchrony[B]

  object LmTrack
  { //
    // A "landmark track" is a track that "experiment tracks" sync 
    // to. It is defined as an iterator[B] that has snchrony ability.

    def apply[B](it: Iterator[B]): LmTrack[B] = SynchronyIterator[B](it)
  }
 

  case class SyncedTrack[A,B](
    syncWith: B => Vector[A])
  //
  // A synced ExTrack is represented by the "syncing" function
  // produced by Synchrony.sync.



  trait Synchrony[B]
  {
    protected val itB: Iterator[B]

    final protected var numSynced = 0
    final protected val syncedTracks = new ArrayBuffer[B=>Vector[Any]]()
    final protected val syncedResults = new ArrayBuffer[Vector[Any]]()


    final protected def updateSyncedResults(b:B) =
    { //
      // b is the current element on itB. To keep each synced
      // track in sync with b, its "syncing" function must be
      // called and the result must be cached. 
     
      for(id <- 0 to numSynced - 1) {
        syncedResults(id) = syncedTracks(id)(b)
      }
    }


    final def synchronyHasNext = itB.hasNext


    final def synchronyNext(): B = {
      val b = itB.next()
      updateSyncedResults(b) // Keep synced tracks in sync with b.
      return b
    }


    final def sync[A](
      it: Iterator[A],
      isBefore: (A,B)=>Boolean,
      canSee: (A,B)=>Boolean,
      exclusiveCanSee: Boolean = false): SyncedTrack[A,B] =
    { //
      // Function to add it as a synced track to this landmark track.

      val itA = ExTrack(it)

      def syncStep(b: B): Vector[A] = 
      { //
        // b is the current element on itB to sync itA with.

        itA.dropWhile (a => (isBefore(a, b) && (! canSee(a, b))))
        //
        // Discards all elements on itA that is before b
        // and cannot see b.

        val (shifted, rest) = itA.shadow
          .span (a =>(isBefore(a, b) || (canSee(a, b))))
        //
        // shifted contains all the elements on itA that
        // are between the first and the last element on
        // itA that can see b.
        //
        // This search is done using shadow. So all of
        // shifted elements are still retained in itA.
        // This is crucial to support non-exclusiveCanSee.

        val toSync = shifted.toVector
        val synced = toSync filter (a => canSee(a, b)) 
        //
        // Extract only those shifted elements that can see b. 
        // This is the list to be returned.

        if (exclusiveCanSee) { itA drop (toSync.length) } else { } 
        //
        // if exclusiveCanSee, discards from itA every element
        // that got shifted just now.

        return synced 
      } 

      val id = this.numSynced
      syncedTracks += (syncStep _)
      syncedResults += Vector()
      numSynced += 1
      //
      // Insert the synced track into the syncedTracks list.

      return SyncedTrack((b:B) => syncedResults(id).asInstanceOf[Vector[A]])
      //
      // Return a function for looking up current synced 
      // of this newly added synced track.
    }
  }


  object SynchronyIterator
  {
    def apply[B](it:Iterator[B]): Iterator[B] with Synchrony[B] =
      new Iterator[B] with Synchrony[B] {
        override val itB = it
        override def hasNext = synchronyHasNext
        override def next() = synchronyNext()
      }
  }


  object implicits
  { //
    // Implicit conversions to endow ExTrack and LmTrack
    // with groupBy and aggregateBy functions.

    import synchrony.iterators.AggrCollections.AggrIterator
    import scala.language.implicitConversions

    implicit def ExTrack2Aggr[A](it:ExTrack[A]):AggrIterator[A] =
      AggrIterator(it)

    implicit def LmTrack2Aggr[A](it:LmTrack[A]):AggrIterator[A] =
      AggrIterator(it)
  }

}



/*
 * Example 
 *

import synchrony.iterators.SyncCollections._

def bf(x:Int,y:Int) = x < y
def cs(x:Int,y:Int) = (y>= x-1) && (x+1 >= y)
def a = LmTrack(Iterator(1,2,3,4,5,6,7,8,9,10))
def b = Iterator(1,2,3,4,5,6,7,8,9,10)
def connect(ex:Boolean) = {val as = a; val bs = as.sync(b,bf,cs,ex); (as,bs)}

def eg1(ex:Boolean) = {
  val (as, bs) = connect(ex)
  for(x <- as; y = bs.syncWith(x)) yield (x,y) }

def eg2(ex: Boolean) = {
  val (as, bs) = connect(ex)
  for (x <- as;
    u <- bs.syncWith(x);
    v <- bs.syncWith(x) if (u != v))
  yield (x, u) }

def result[A](eg:Iterator[A]) = {
  var count = 0
  eg foreach (x => { println(x); count += 1 })
  count }

result(eg1(true))
result(eg1(false))
result(eg2(true))
result(eg2(false))

 *
 */


/*
 * Expected output
 *

scala> result(eg1(true))
(1,List(1, 2))
(2,List(3))
(3,List(4))
(4,List(5))
(5,List(6))
(6,List(7))
(7,List(8))
(8,List(9))
(9,List(10))
(10,List())
res0: Int = 10

scala> result(eg1(false))
(1,List(1, 2))
(2,List(1, 2, 3))
(3,List(2, 3, 4))
(4,List(3, 4, 5))
(5,List(4, 5, 6))
(6,List(5, 6, 7))
(7,List(6, 7, 8))
(8,List(7, 8, 9))
(9,List(8, 9, 10))
(10,List(9, 10))
res1: Int = 10

scala> result(eg2(true))
(1,1)
(1,2)
res2: Int = 2

scala> result(eg2(false))
(1,1)
(1,2)
(2,1)
(2,1)
(2,2)
(2,2)
(2,3)
(2,3)
(3,2)
(3,2)
(3,3)
(3,3)
(3,4)
(3,4)
(4,3)
(4,3)
(4,4)
(4,4)
(4,5)
(4,5)
(5,4)
(5,4)
(5,5)
(5,5)
(5,6)
(5,6)
(6,5)
(6,5)
(6,6)
(6,6)
(6,7)
(6,7)
(7,6)
(7,6)
(7,7)
(7,7)
(7,8)
(7,8)
(8,7)
(8,7)
(8,8)
(8,8)
(8,9)
(8,9)
(9,8)
(9,8)
(9,9)
(9,9)
(9,10)
(9,10)
(10,9)
(10,10)
res3: Int = 52

 *
 */



