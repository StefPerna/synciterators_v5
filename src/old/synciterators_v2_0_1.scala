
package synchrony.iterators  

/** Synchrony iterators provide generalized synchronized iterations,
 *  e.g. performining a merge join, on multiple iterators. 
 *
 *  This module implements Synchrony's two main iterator structures,
 *  the "landmark track" (LmTrack) and the "experiment track" (ExTrack),
 *  where a landmark track "synchronizes" one or more "experiment track".
 *  Two predicates, isBefore and canSee, are used to define the
 *  synchronization. 
 *
 *  Intuitively, canSee(x,y) means the element x on an experiment
 *  track "can see" (i.e. is synchronized with the element y on the
 *  landmark track. And isBefore(x,y) means the position or locus of
 *  the element x on an experiment track corresponds to some position 
 *  or locus in front of the position or locus of the element y on 
 *  the landmark track.
 * 
 *  Synchrony makes some assumptions to efficiently synchronize
 *  iterators on the landmark and experiment tracks. These assumptions
 *  are the monotonocity conditions (i) and (ii), and the antimonotonicity
 *  conditions (iii) - (iv) below. Let x and y be elements on a 
 *  specific track (i.e. in the same file/iterator). Let x << y means
 *  the physical position of x is in front of the physical position of 
 *  y in the file/iterator (i.e. an iteration on the track is going to
 *  encounter x before y. Then the monotonicity and antimonotonicty
 *  conditions sufficient to ensure correctness are:
 * 
 *    (i) x << x' iff for all y in Y: y isBefore x implies y isBefore x'.
 * 
 *   (ii) y' << y iff for all x in X: y isBefore x implies y' isBefore x.
 * 
 *  (iii) If x << x', then for all y in Y: y isBefore x, and not y canSee x,
 *                                         implies not y canSee x'.
 * 
 *   (iv) If y << y', then for all x in X: not y isBefore x, and not y canSee x,
 *                                         implies not y' canSee x.
 * 
 * Wong Limsoon
 * 26 April 2020
 *
 */


object SyncCollections {

  import scala.collection.mutable.ArrayBuffer
  import synchrony.iterators.ShadowCollections._
  import java.io.EOFException 

  var DEBUG = true



  /** Constructors for an ExTrack that is synced to a LmTrack.
   *
   *  Provide a method [[syncWith]] for the LmTrack to synchronize
   *  this SyncedExTrack to its current element b. The result is 
   *  memoised for [[syncedWith]] to return. This method should
   *  be called only by the LmTrack for each element b.
   *
   *  Provide also a method [[syncedWith]] for accessing the
   *  vector of elements that are synced to the LmTrack's current
   *  element b. This method can be called by anyone as often as 
   *  they like. It always returns the latest memoised result of
   *  the most recent [[syncWith]].
   *
   *  PvtSyncedExTrack is strictly for internal use by the LmTrack.
   *  SyncedExTrack is for everyone to use; it has only [[syncedWith]].
   *
   */


  case class SyncedExTrack[A, B](syncedWith: B => Vector[A])


  private class PvtSyncedExTrack[A, B](
    ex: ExTrack[A],
    isBefore: (A, B) => Boolean,
    canSee: (A, B) => Boolean,
    isExclusive: Boolean) {

  /** Cache for memoising the most recent result of [[syncWith]].
   */

    protected var cache: Vector[A] = Vector[A]()


  /** Close the ExTrack. Call this when the LmTrack it is synced with ends.
   */

    val close: () => Unit = ex.close _


  /** Called by the LmTrack to synchronize this PvtSyncedExTrack to its
   *  current element b. 
   *
   *  @param b is the current element on the LmTrack.
   *
   *  This method should be called only by the LmTrack once for each 
   *  element b. The result is memoised in cache for [[syncedWith]] 
   *  to return.
   */

    val syncWith: B => Unit = {
      val nxMatches = ex.nextMatches(isBefore, canSee, isExclusive)
      val empty = Vector[A]()
      (b: B) => { cache = if (ex.hasNext) nxMatches(b) else { close(); empty } }
    }


  /** Return the memoised results of the most recent [[syncWith]].
   *  These are elements in the ExTrack that are synced to the
   *  current element b in the LmTrack. 
   *
   *  @param b is the current element on the LmTrack.
   */

    val syncedWith: B => Vector[A] = (b: B) => cache


  /** Export a restricted version of this PvtSyncedExTrack which
   *  only provides [[syncedWith]]. 
   *
   *  In normal use, this PvtSyncedExTrack is created by the LmTrack.
   *  The LmTrack provides this exported version for external use,
   *  so that only the LmTrack itself can access [[syncWith]].
   */

    val exported = SyncedExTrack[A, B](syncedWith)

  } // End class SyncedExTrack




  /** Constructor for ExTracks.
   *
   *  @param itA is the EIterator to iterate on.
   *  @return an ExTrack iterating on itA.
   *
   *  An ExTrack is a like an iterator that iterates on the elements
   *  of input file/iterator itA a "chunk" at a time.  It does not
   *  have the next() method of normal iterators. Instead, it 
   *  provides a nextMatches(isBefore, canSee, isExclusive)(b) method
   *  which returns a vector of elements on itA that "matches" a given
   *  value b; "matches" is defined by the isBefore, canSee, and
   *  isExclusive predicates as per Synchrony iterators.
   */ 

  case class ExTrack[A](itA: EIterator[A]) {


  /** Close this ExTrack; i.e. close itA.
   */

    def close(): Unit = ExTrack.close(this)


  /** Any more elements on this ExTrack? i.e. any more elements on itA?
   */

    def hasNext: Boolean = ExTrack.hasNext(this)


  /** Get the next chunk of elements that match b.
   *  The b's are assumed to come from a LmTrack.
   *
   *  @param isBefore satisfies Monotonicity conditions (i) and (ii). 
   *  @param canSee satisfies Antimonotonicty conditions (iii) and (iv).
   *  @param isExclusive specifies whether the same elements in this
   *     ExTrack are allowed to match multiple b's.
   *  @return a function which accepts b and produces the next chunk
   *     of elements in this ExTrack that match b. Here b is assumed
   *     to be the current element of a LmTrack that this EXTrack is
   *     synced to.
   */

    def nextMatches[B](
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      isExclusive: Boolean = false)
    : B => Vector[A] = {

      ExTrack.nextMatches[A, B](isBefore, canSee, isExclusive)(this) 

    }


  /** Register this ExTrack to an LmTrack for synchronization.
   *
   *  @param lm is the LmTrack.
   *  @param isBefore
   *  @param canSee
   *  @param isExclusive are predicates defining how elements in
   *     this ExTrack are matched to elements in the LmTrack.
   *  @return the SyncedExTrack.
   */

    def sync[B](
      lm:          LmTrack[B],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      isExclusive: Boolean = false
    ): SyncedExTrack[A, B] = {

      lm.sync(this, isBefore, canSee, isExclusive)

    }



  /** Connect an LmTrack and this ExTrack to build Synchrony iterators.
   *
   *  @param lmtrack is an LmTrack
   *  @param isBefore
   *  @param canSee are SynChrony iterator predicates defining "match"
   *  @param isExclusive indicates whether elements in ExTrack
   *     are allowed to match multiple elements in LmTrack.
   *  @return the pair of Synchrony iterators constructed.
   */

    def connect[B](
      lm:          LmTrack[B],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      isExclusive: Boolean = false
    ): (LmTrack[B], SyncedExTrack[A, B]) = {

     lm.connect[A](this, isBefore, canSee, isExclusive)

    }

  } // End class ExTrack



  /* Constructors for building ExTrack from iterators, vectors, and files.
   */

  object ExTrack {

  /** Construct ExTrack from an ordinary iterator.
   *  
   *  @param it is the ordinary iterator.
   *  @return n ExTrack wrapped around it.
   */

    def apply[A](it: Iterator[A]): ExTrack[A] = new ExTrack[A](EIterator[A](it))


  /** Construct ExTrack from a vector.
   *
   *  @param it is the vector.
   *  @return n ExTrack wrapped around it.
   */

    def apply[A](it: Vector[A]): ExTrack[A] = new ExTrack[A](EIterator[A](it))


  /** Construct ExTrack from a file.
   *
   *  @param filename is name of the file.
   *  @param deserializer is a deserializer for reading the file.
   *  @return an ExTrack on this file.
   */

    def apply[A](
      filename:     String, 
      deserializer: String => EIterator[A])
    : ExTrack[A] = {

      new ExTrack[A](EIterator[A](filename, deserializer))

    }



  /** Close an ExTrack.
   *
   *  @param ex is the ExTrack to be closed.
   */

    def close[A](ex: ExTrack[A]): Unit = ex.itA.close()

  
  /** Any more elements on an ExTrack? If none, also close the ExTrack.
   *
   *  @param ex is the ExTrack to check.
   */

    def hasNext[A](ex: ExTrack[A]): Boolean = {

      ex.itA.hasNext || { ex.itA.close(); false }

    }



  /** Return the next chunk of elements on an ExTrack that "matches"
   *  an item b, which is assumed to be the current element on the
   *  LmTrack that this ExTrack is synced with.
   *
   *  @param isBefore
   *  @param canSee
   *  @param isExclusive are predicates that define "matches".
   *     These predicates are assumed to satisfy the conditions 
   *     of Synchrony iterators.
   *  @param ex is the ExTrack
   *  @param b is assumed to be the current element on the LmTrack
   *     that the ExTrack is synced to.
   *  @return the nect chunk of elements on ex that matches b.
   */

    def nextMatches[A, B]
      (isBefore:    (A, B) => Boolean,
       canSee:      (A, B) => Boolean,
       isExclusive: Boolean = false)
      (ex: ExTrack[A])
    : B => Vector[A] = { (b: B) => 

      ex.itA dropWhile (a => (isBefore(a, b) && (! canSee(a, b))))

      //
      // Discarded all elements in ex that are before b and cannot see b,
      // as per Antimonotonicity condition (iii).

      val (shifted, rest) =
        ex.itA.shadow() span (a => (isBefore(a, b) || (canSee(a, b))))
      //
      // shifted contains all the elements in ex which are between
      // the first and the last element in ex that can see b, as per
      // Antimonotonicty condition (iv). This search was done using
      // shadowing. So all of shifted elements are still retained
      // in ex; this is crucial to support non-exclusiveness.

      val toSync = shifted.toVector
      val synced = toSync filter (a => canSee(a, b)) 
      //
      // Extracted those shifted elements that can see b. 
      // This is the list to be cached in ex and returned.

      if (isExclusive) { 
        ex.itA drop (toSync.length)
      //
      // if exclusiveness is desired, discards from ex every 
      // element that got shifted just now.
      } 

      else {
        ex.itA filterBuffer (a => !isBefore(a,b) || canSee(a,b))
      //
      // Removed elems in ex's buffer that cannot see the items
      // that can be expected to come after this b, as per
      // Antimonotonicity condition (iv).
      } 

      synced 
    } 

  } // End object ExTrack



  /** Constructor for LmTrack.
   *
   *  @param itB is an EIterator to iterate on.
   *  @return an LmTrack iterating on itB.
   *
   *  An LmTrack can synchronize multiple ExTracks. When the next
   *  element on the LmTrack is demanded, it immediately [[syncWith]]
   *  this element to all the ExTracks synced to the LmTrack, and
   *  the matching elements on these ExTracks are memoised. 
   */

  case class LmTrack[B](itB: EIterator[B]) extends Iterator[B] {


  /** Array keep track of the [[syncWith]] methods of ExTracks
   *  synced to this LmTrack.
   */

    protected val exTrackUpdaters = new ArrayBuffer[B => Unit]


  /** Array to keep track of the [[close]] methods of ExTracks
   *  synced to this LmTrack.
   */

    protected val exTrackClosers = new ArrayBuffer[() => Unit]


  /** Call all the [[syncWith]] methods of ExTracks synced to this LmTrack.
   *  Thus, synchronize all these ExTracks to the current element on 
   *  this LmTrack.
   *
   *  @param b is the current element on this LmTrack.
   */

    protected def updateExTracks(b:B) = exTrackUpdaters.foreach(_(b))


  /** Close all the ExTracks synced to this LmTrack. 
   *  Do this when this LmTrack reaches its end.
   */ 

    protected def closeExTracks() = exTrackClosers.foreach(_())

    def close(): Unit = { itB.close(); closeExTracks() }


  /** Any more elements on this LmTrack? If none, close this LmTrack.
   */

    def hasNext = itB.hasNext || { close(); false }


  /** Shift to and return the next element on this LmTrack,  
   *  synchronize all the ExTracks.
   *
   *  @return the next element on this LmTrack.
   */

    def next(): B = {
      val b = itB.next()
      updateExTracks(b) // Keep synced tracks in sync with b.
      return b
    }


  /** Register an ExTrack for synchronization.
   *
   *  @param ex is the ExTrack to be synced.
   *  @param isBefore
   *  @param canSee
   *  @param isExclusive are predicates defining how to match/sync
   *     elements on ex to elements on this LmTrack. These predicates
   *     are assumed to satisfy the conditions on Synchrony iterators.
   *  @return the synced ExTrack.
   */

    def sync[A](
      ex:          ExTrack[A],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      isExclusive: Boolean = false)
    : SyncedExTrack[A, B] = {

      val track = new PvtSyncedExTrack[A, B](ex, isBefore, canSee, isExclusive)
      //
      // Create the SyncedExTrack.

      exTrackUpdaters += track.syncWith
      exTrackClosers += track.close
      //
      // Register its [[syncWith]] and [[close]] methods

      track.exported
      //
      // Return the export version that effectively exposes only
      // the [[syncedWith]] method of this synced ExTrack.
    }


  /** Connect this LmTrack and an ExTrack.
   *
   *  @param extrack is an ExTrack
   *  @param isBefore
   *  @param canSee are SynChrony iterator predicates defining "match"
   *  @param isExclusive indicates whether elements in ExTrack
   *     are allowed to match multiple elements in LmTrack.
   *  @return the pair of Synchrony iterators constructed.
   */

    def connect[A](
      ex:          ExTrack[A],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      isExclusive: Boolean = false)
    : (LmTrack[B], SyncedExTrack[A, B]) = {

      (this, sync[A](ex, isBefore, canSee, isExclusive))

    }


  } // End class LmTrack




  /** Constructors for building LmTracks from ordinary iterators,
   *  vectors, and files.
   */

  object LmTrack {


  /** Construct LmTrack from an ordinary iterator.
   *  
   *  @param it is the iterator.
   *  @return an LmTrack wrapped around it.
   */

    def apply[B](it:Iterator[B]): LmTrack[B] = new LmTrack[B](EIterator[B](it))


  /** Construct LmTrack from a vector.
   *  
   *  @param it is the vector.
   *  @return an LmTrack wrapped around it.
   */

    def apply[B](it:Vector[B]): LmTrack[B] = new LmTrack[B](EIterator[B](it))


  /** Construct LmTrack from a file.
   *
   *  @param filename is name of the file.
   *  @param deserializer is a deserializer for reading the file.
   *  @return an LmTrack on  the file.
   */

    def apply[B](
      filename:     String, 
      deserializer: String => EIterator[B])
    : LmTrack[B] = {

      new LmTrack[B](EIterator[B](filename, deserializer))

    }

  } // End object LmTrack



  /** EIterators are intended as iterators on files.
   *
   * EIterators need to allow users to close the files midway during
   * an iteration. EIterators only need to allow for arbitrarily long
   * look-ahead.
   */

  trait EIterator[A] extends Iterator[A] with Shadowing[A] {

  //
  // Functions to be overridden by inheriting classes.
  // These define the underlying file/iterator.
  //

  /** Is there more elements in the underlying file/iterator?
   */

    protected def myHasNext:Boolean


  /** Get the next element in the underlying file/iterator.
   */

    protected def myNext():A

  /** Close the underlying file/iterator.
   */

    protected def myClose():Unit



  //
  // The main EIterator functions 
  //


  /** Is the underlying file closed?
   */

    protected var closed:Boolean = false


  /** Close the underlying file.
   */

    def close():Unit = { myClose(); closed = true }


  /** Are there more elements on this EIterator?
   */

    override def hasNext = primary.hasNext


  /** Return the next element on this EIterator.
   */

    override def next() = primary.next()


  /** Endow this EIterator with shadowing functions.
   *
   *  This is done by overriding the [[itS]] field inherited from Shadowing.
   */
    
    override val itS = new Iterator[A] 
    {
      override def hasNext:Boolean = {

        if (closed) false else if (myHasNext) true else { close(); false }

      }

      override def next():A = {

        if (!closed) myNext() else { throw new EOFException() }

      }
    }


  /** Endow this EIterator with comprehension syntax.
   */

    override def foreach[B](f: A => B): Unit = EIterator.foreach[A, B](this)(f)

    override def map[B](f: A => B): EIterator[B] = EIterator.map[A, B](this)(f)

    override def withFilter(f: A => Boolean): EIterator[A] = {

      EIterator.withFilter[A](this)(f)

    }

    def flatMap[B](f: A => EIterator[B]): EIterator[B] = {

      EIterator.flatMap[A, B](this)(f)
 
    }


  } // End trait EIterator



  /** Constructor of EIterators.
   */
  
  object EIterator {

  /** Construct an EIterator from an ordinary iterator.
   *
   *  @param it is an ordinary iterator.
   *  @return an EIterator wrapped around it.
   */

    def apply[A](it:Iterator[A]) = fromIterator(it)


    def fromIterator[A](it:Iterator[A]) = new EIterator[A] {

      override protected def myHasNext = it.hasNext
      override protected def myNext() = it.next()
      override protected def myClose() = { }

    }



  /** Construct an EIterator from a vector.
   *
   *  @param it is a vector.
   *  @return an EIterator wrapped around it.
   */

    def apply[A](it:Vector[A]) = fromVector(it)


    def fromVector[A](entries:Vector[A]) = fromIterator(entries.iterator)



  /** Construct an EIterator from a file.
   *
   *  @param filename is a name of a file.
   *  @param deserializer is a function for deserializing the file.
   *  @return an EIterator on the file.
   */

    def apply[A](filename:String, deserializer:String => EIterator[A]) = {

      fromFile(filename, deserializer)

    }


    def fromFile[A](filename:String, deserializer:String => EIterator[A]) = {

      deserializer(filename)

    }



    /** @param itX
     *  @param itY are two EIterator.
     *  @return concatenation of these two EIterator.
     */

    def concat[A](itX: EIterator[A], itY:EIterator[A])
    : EIterator[A] = new EIterator[A] {

      override protected def myHasNext = itX.hasNext || itY.hasNext


      override protected def myNext() = {

        if (itX.hasNext) itX.next() else itY.next()

      }


      override protected def myClose() = { itX.close(); itY.close() }

    }



  /** Foreach, map, flatMap, and withFilter provide comprehension syntax.
   */

    def foreach[A, B](it: EIterator[A])(f: A => B): Unit = {

      while (it.hasNext) f(it.next())
      it.close()

    }


    def map[A, B](it: EIterator[A])(f: A => B)
    : EIterator[B] = new EIterator[B] {

      override def myHasNext = it.hasNext

      override def myNext() = f(it.next())

      override def myClose() = it.close()

    }


    def withFilter[A](it: EIterator[A])(f: A => Boolean)
    : EIterator[A] = new EIterator[A] {

      var mybuf = None: Option[A]

      def seekNext() = {
        while (it.hasNext && mybuf == None) {
          val nx = it.next()
          if (f(nx)) mybuf = Some(nx)
        }
        if (! it.hasNext) it.close()
      }

      override def myHasNext = { seekNext(); mybuf != None }

      override def myNext() =
         try { seekNext(); mybuf.get }
         finally { mybuf = None }
        
      override def myClose() = it.close()

    }


    def flatMap[A, B](it: EIterator[A])(f: A => EIterator[B])
    : EIterator[B] = new EIterator[B] {

      var mybuf = fromVector(Vector[B]())

      def seekNext() = {
        while (it.hasNext && !mybuf.hasNext) {
          mybuf.close()
          mybuf = f(it.next())
        } 
        if (!it.hasNext) it.close()
      }

      override def myHasNext = { seekNext(); mybuf.hasNext }

      override def myNext() = { seekNext(); mybuf.next() }

      override def myClose() = { mybuf.close(); it.close() }

    }



  /** Produce deserializers for user's own formats.
   *
   *  @param openFile is function to open a file.
   *  @param parseItem is parser to read an item in the file.
   *  @param closeFile is function to close the file.
   *  @return a deserializer, which given a filename, opens it,
   *          constructs an EIterator on items in the file,
   *          and closes the file when all items are produced
   *          by the EIterator.
   */

    def makeParser[B, A](
      openFile:  String => B,
      parseItem: B => A,
      closeFile: B => Unit)
    : String => EIterator[A] = (filename:String) => new EIterator[A] {

      private val ois = openFile(filename)

      private def getNext() =
        try Some(parseItem(ois))
        catch {
          case e:EOFException => { close(); None }
          case e:NoSuchElementException => { close(); None }
          case e:Throwable => { 
            if (DEBUG) println(s"Caught unexpected exception: ${e}")
            close(); None
          }
        }
  
      private var nextEntry = getNext() 

      override protected def myHasNext = nextEntry != None 

      override protected def myNext() = 
        try nextEntry match {
          case Some(e) => e
          case None => throw new EOFException()
        }
        finally { nextEntry = getNext() } 

      override protected def myClose() = closeFile(ois)
    }

  } // End object EIterator


} // End SyncCollections



/*
 * Example 
 *
   {{{

import synchrony.iterators.SyncCollections._

def bf(x:Int,y:Int) = x < y
def cs(x:Int,y:Int) = (y>= x-1) && (x+1 >= y)
def a = LmTrack(Iterator(1,2,3,4,5,6,7,8,9,10))
def b = ExTrack(Iterator(1,2,3,4,5,6,7,8,9,10))

def connect(ex:Boolean) = {val as = a; val bs = as.sync(b,bf,cs,ex); (as,bs)}

def eg1(ex:Boolean) = {
  val (as, bs) = connect(ex)
  for(x <- as; y = bs.syncedWith(x)) yield (x,y) }

def eg2(ex: Boolean) = {
  val (as, bs) = connect(ex)
  for (x <- as;
    u <- bs.syncedWith(x);
    v <- bs.syncedWith(x) if (u != v))
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
 * Expected output
 *

scala> result(eg1(true))
(1,Vector(1, 2))
(2,Vector(3))
(3,Vector(4))
(4,Vector(5))
(5,Vector(6))
(6,Vector(7))
(7,Vector(8))
(8,Vector(9))
(9,Vector(10))
(10,Vector())
res0: Int = 10

scala> result(eg1(false))
(1,Vector(1, 2))
(2,Vector(1, 2, 3))
(3,Vector(2, 3, 4))
(4,Vector(3, 4, 5))
(5,Vector(4, 5, 6))
(6,Vector(5, 6, 7))
(7,Vector(6, 7, 8))
(8,Vector(7, 8, 9))
(9,Vector(8, 9, 10))
(10,Vector(9, 10))
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

      }}}
 *
 */



