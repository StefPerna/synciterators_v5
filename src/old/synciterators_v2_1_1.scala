
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
 * 10 May 2020
 *
 */


object SyncCollections {

  import scala.collection.mutable.ArrayBuffer
  import synchrony.iterators.ShadowCollections._

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



  private trait SyncedState[B] {
    def close(): Unit
    def syncWith(b: B): Unit
  }


  private class PvtSyncedExTrack[A, B](
    ex: ExTrack[A],
    isBefore:    (A, B) => Boolean,
    canSee:      (A, B) => Boolean,
    filter:      (A, B) => Boolean,
    n:           Int,
    isExclusive: Boolean)
  extends SyncedState[B] {

  /** Cache for memoising the most recent result of [[syncWith]].
   */

    protected var cache: Vector[A] = Vector[A]()


  /** Close the ExTrack. Call this when the LmTrack it is synced with ends.
   */

    override def close(): Unit = ex.close()


  /** Called by the LmTrack to synchronize this PvtSyncedExTrack to its
   *  current element b. 
   *
   *  @param b is the current element on the LmTrack.
   *
   *  This method should be called only by the LmTrack once for each 
   *  element b. The result is memoised in cache for [[syncedWith]] 
   *  to return.
   */

    override def syncWith(b: B): Unit = (ex.hasNext, n < 0) match {

      case (true, true) => 
        cache = ex.nextMatches(isBefore, canSee, filter, isExclusive)(b)

      case (true, false) =>
        cache = ex.nextNMatches(isBefore, canSee, filter, n, isExclusive)(b)

      case (false, _) => close(); cache = Vector[A]()
    }


  /** Return the memoised results of the most recent [[syncWith]].
   *  These are elements in the ExTrack that are synced to the
   *  current element b in the LmTrack. 
   *
   *  @param b is the current element on the LmTrack.
   */

    def syncedWith(b: B): Vector[A] = cache


  /** Export a restricted version of this PvtSyncedExTrack which
   *  only provides [[syncedWith]]. 
   *
   *  In normal use, this PvtSyncedExTrack is created by the LmTrack.
   *  The LmTrack provides this exported version for external use,
   *  so that only the LmTrack itself can access [[syncWith]].
   */

    val exported = SyncedExTrack[A, B](syncedWith _)

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
   *  @param isBefore    satisfies Monotonicity conditions (i) and (ii). 
   *  @param canSee      satisfies Antimonotonicty conditions (iii) and (iv).
   *  @param filter      what matches to return.
   *  @param n           max # of items to return.
   *  @param isExclusive specifies whether the same elements in this
   *                     ExTrack are allowed to match multiple b's.
   *  @return a function which accepts b and produces the next chunk
   *         of elements in this ExTrack that match b. Here b is assumed
   *         to be the current element of a LmTrack that this EXTrack is
   *         synced to.
   */

    def nextMatches[B]
      (isBefore:    (A, B) => Boolean,
       canSee:      (A, B) => Boolean,
       filter:      (A, B) => Boolean = (a: A, b: B) => true,
       isExclusive: Boolean           = false)
      (b: B):
    Vector[A] = { 

      ExTrack.nextMatches[A, B](isBefore, canSee, filter, isExclusive)(this)(b) 

    }


    def nextNMatches[B]
      (isBefore:    (A, B) => Boolean,
       canSee:      (A, B) => Boolean,
       filter:      (A, B) => Boolean = (a: A, b: B) => true,
       n:           Int               = 1,
       isExclusive: Boolean           = false)
      (b: B):
    Vector[A] = { 

      ExTrack.nextNMatches[A, B](isBefore, canSee, filter, n, isExclusive)(this)(b) 

    }


  /** Register this ExTrack to an LmTrack for synchronization.
   *
   *  @param lm is the LmTrack.
   *  @param isBefore
   *  @param canSee
   *  @param filter
   *  @param isExclusive are predicates defining how elements in
   *     this ExTrack are matched to elements in the LmTrack.
   *  @return the SyncedExTrack.
   */

    def sync[B](
      lm:          LmTrack[B],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      filter:      (A, B) => Boolean = (a: A, b: B) => true,
      isExclusive: Boolean           = false): 
    SyncedExTrack[A, B] = {

      lm.sync(this, isBefore, canSee, filter, isExclusive)

    }


    def syncN[B](
      lm:          LmTrack[B],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      filter:      (A, B) => Boolean = (a: A, b: B) => true,
      n:           Int               = 1,
      isExclusive: Boolean           = false): 
    SyncedExTrack[A, B] = {

      lm.syncN(this, isBefore, canSee, filter, n, isExclusive)

    }




  /** Connect an LmTrack and this ExTrack to build Synchrony iterators.
   *
   *  @param lmtrack  is an LmTrack
   *  @param isBefore
   *  @param canSee   are SynChrony iterator predicates defining "match"
   *  @param filter   select the matches to return.
   *  @param n        max # of matches to return,
   *  @param isExclusive indicates whether elements in ExTrack
   *                  are allowed to match multiple elements in LmTrack.
   *  @return the pair of Synchrony iterators constructed.
   */

    def connect[B](
      lm:          LmTrack[B],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      filter:      (A, B) => Boolean  = (a: A, b: B) => true,
      isExclusive: Boolean            = false): 
    (LmTrack[B], SyncedExTrack[A, B]) = {

     lm.connect[A](this, isBefore, canSee, filter, isExclusive)

    }


    def connectN[B](
      lm:          LmTrack[B],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      filter:      (A, B) => Boolean  = (a: A, b: B) => true,
      n:           Int                = 1,
      isExclusive: Boolean            = false): 
    (LmTrack[B], SyncedExTrack[A, B]) = {

     lm.connectN[A](this, isBefore, canSee, filter, n, isExclusive)

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

    def apply[A](it: Iterable[A]): ExTrack[A] = new ExTrack[A](EIterator[A](it))


  /** Construct ExTrack from a file.
   *
   *  @param filename is name of the file.
   *  @param deserializer is a deserializer for reading the file.
   *  @return an ExTrack on this file.
   */

    def apply[A](
      filename:     String, 
      deserializer: String => EIterator[A]): 
    ExTrack[A] = {

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
   *  @param canSee      are predicates that define "matches".
   *                     These predicates are assumed to satisfy the
   *                     conditions of Synchrony iterators.
   *  @param filter      filter the matches.
   *  @param n           max # of items to return.
   *  @param isExclusive whether to allow items on this EIterator to match
   *                     multiple b.
   *  @param ex          is the ExTrack
   *  @param b           is assumed to be the current element on the LmTrack
   *                     that the ExTrack is synced to.
   *  @return the next chunk of elements on ex that matches b.
   */

    def nextMatches[A, B]
      (isBefore:    (A, B) => Boolean,
       canSee:      (A, B) => Boolean,
       filter:      (A, B) => Boolean = (a: A, b: B) => true,
       isExclusive: Boolean = false)
      (ex: ExTrack[A])
      (b: B):
    Vector[A] = { 
 
      ex.itA.discardedWhile(a => isBefore(a, b), a => canSee(a, b))
      //
      // Discarded all elements in ex that are before b and cannot see b,
      // as per Antimonotonicity condition (iii). 

      val synced = ex.itA.collectedWhile(
        a => isBefore(a, b), 
        a => canSee(a, b),
        a => filter(a, b))
      //
      // Extracted all the elements in ex that can see b, as per
      // Antimonotonicty condition (iv).  All of extracted elements 
      // are still retained in ex; this is crucial to support 
      // non-exclusiveness. Return (in synced) only those extracted
      // elements satisfying filter.

      if (isExclusive) { ex.itA.discarded(synced.size) }
      //
      // if exclusiveness is desired, discards from ex every 
      // element that got extracted just now.
      
      synced 
    } 


    def nextNMatches[A, B]
      (isBefore:    (A, B) => Boolean,
       canSee:      (A, B) => Boolean,
       filter:      (A, B) => Boolean = (a: A, b: B) => true,
       n:           Int = 1,
       isExclusive: Boolean = false)
      (ex: ExTrack[A])
      (b: B):
    Vector[A] = { 
 
      ex.itA.discardedWhile(a => isBefore(a, b), a => canSee(a, b))
      //
      // Discarded all elements in ex that are before b and cannot see b,
      // as per Antimonotonicity condition (iii). 

      val synced = ex.itA.collectedNWhile(
        a => isBefore(a, b), 
        a => canSee(a, b),
        a => filter(a, b),
        n)
      //
      // Extracted all the elements in ex that can see b, as per
      // Antimonotonicty condition (iv).  All of extracted elements 
      // are still retained in ex; this is crucial to support 
      // non-exclusiveness. Return (in synced) only those extracted
      // elements satisfying filter.

      if (isExclusive) { ex.itA.discarded(synced.size) }
      //
      // if exclusiveness is desired, discards from ex every 
      // element that got extracted just now.
      
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


  /** Array keep track of the ExTracks synced to this LmTrack.
   */

    private val exTracks = new ArrayBuffer[SyncedState[B]]


  /** Call all the [[syncWith]] methods of ExTracks synced to this LmTrack.
   *  Thus, synchronize all these ExTracks to the current element on 
   *  this LmTrack.
   *
   *  @param b is the current element on this LmTrack.
   */

    protected def updateExTracks(b:B) = exTracks.foreach(_.syncWith(b))


  /** Close all the ExTracks synced to this LmTrack. 
   *  Do this when this LmTrack reaches its end.
   */ 

    protected def closeExTracks() = exTracks.foreach(_.close())

    def close(): Unit = { itB.close(); closeExTracks() }


  /** Any more elements on this LmTrack? If none, close this LmTrack.
   */

    override def hasNext: Boolean = itB.hasNext || { close(); false }


  /** Shift to and return the next element on this LmTrack,  
   *  synchronize all the ExTracks.
   *
   *  @return the next element on this LmTrack.
   */

    override def next(): B = {
      val b = itB.next()
      updateExTracks(b) // Keep synced tracks in sync with b.
      return b
    }


  /** Register an ExTrack for synchronization.
   *
   *  @param ex is the ExTrack to be synced.
   *  @param isBefore
   *  @param canSee
   *  @param filter
   *  @param n
   *  @param isExclusive are predicates defining how to match/sync
   *     elements on ex to elements on this LmTrack. These predicates
   *     are assumed to satisfy the conditions on Synchrony iterators.
   *  @return the synced ExTrack.
   */

    def sync[A](
      ex:          ExTrack[A],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      filter:      (A, B) => Boolean = (a: A, b: B) => true,
      isExclusive: Boolean           = false):
    SyncedExTrack[A, B] = {

      val track = 
        new PvtSyncedExTrack[A, B](ex, isBefore, canSee, filter, -1, isExclusive)
      //
      // Create the SyncedExTrack.

      exTracks += track
      //
      // Register the SyncedExTrack

      track.exported
      //
      // Return the export version that effectively exposes only
      // the [[syncedWith]] method of this synced ExTrack.
    }


   def syncN[A](
      ex:          ExTrack[A],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      filter:      (A, B) => Boolean = (a: A, b: B) => true,
      n:           Int               = 1,
      isExclusive: Boolean           = false):
    SyncedExTrack[A, B] = {

      val track = 
        new PvtSyncedExTrack[A, B](ex, isBefore, canSee, filter, n, isExclusive)
      //
      // Create the SyncedExTrack.

      exTracks += track
      //
      // Register the SyncedExTrack

      track.exported
      //
      // Return the export version that effectively exposes only
      // the [[syncedWith]] method of this synced ExTrack.
    }



  /** Connect this LmTrack and an ExTrack.
   *
   *  @param extrack is an ExTrack
   *  @param isBefore
   *  @param canSee  are Synchrony iterator predicates defining "match"
   *  @param filter  select what matches to return
   *  @param n       max # of matches to return.
   *  @param isExclusive indicates whether elements in ExTrack
   *                 are allowed to match multiple elements in LmTrack.
   *  @return the pair of Synchrony iterators constructed.
   */

    def connect[A](
      ex:          ExTrack[A],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      filter:      (A, B) => Boolean = (a: A, b: B) => true,
      isExclusive: Boolean           = false):
    (LmTrack[B], SyncedExTrack[A, B]) = {

      (this, sync[A](ex, isBefore, canSee, filter, isExclusive))

    }


    def connectN[A](
      ex:          ExTrack[A],
      isBefore:    (A, B) => Boolean,
      canSee:      (A, B) => Boolean,
      filter:      (A, B) => Boolean = (a: A, b: B) => true,
      n:           Int               = 1,
      isExclusive: Boolean           = false):
    (LmTrack[B], SyncedExTrack[A, B]) = {

      (this, syncN[A](ex, isBefore, canSee, filter, n, isExclusive))

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

    def apply[B](it:Iterable[B]): LmTrack[B] = new LmTrack[B](EIterator[B](it))


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
   *  EIterators provides: (i) closing the files midway during an
   *  iteration, (ii) arbitrarily long look-ahead, (iii) editable
   *  look-ahead buffer.
   *  
   */

  trait EIterator[A] extends Iterator[A] {

    import scala.collection.mutable.Queue


    //
    // The editable look-ahead buffer
    //

    protected val buffer = new Queue[A]()


    //
    // Functions to be overridden by inheriting classes.
    // These define the underlying file/iterator.
    //


  /** Is there more elements in the underlying file/iterator?
   */

    protected def myHasNext: Boolean


  /** Get the next element in the underlying file/iterator.
   */

    protected def myNext(): A


  /** Close the underlying file/iterator.
   */

    protected def myClose(): Unit



    //
    // The main EIterator functions 
    //


  /** Is the underlying file closed?
   */

    protected var closed: Boolean = false


  /** Close the underlying file.
   */

    def close():Unit = { myClose(); closed = true }


  /** Are there more elements on this EIterator?
   */

    override def hasNext = (buffer.isEmpty && (closed || !myHasNext)) match {
      case true  => close(); false
      case false => true
    }
      


  /** Return the next element on this EIterator.
   */

    override def next(): A = try {
      buffer.isEmpty match {
        case true  => myNext() 
        case false => buffer.dequeue()
      }
    }
    catch { case e: Throwable => close(); throw e }



    //
    // Endow this EIterator with shadowing functionalities.
    //


  /** @return head of this EIterator w/o shifting it.` 
   */

    def head: A = EIterator.headOf[A](this)


  /** @param n is # of items to look ahead.
   *  @return the next n items on this EIterator.
   */

    def lookahead(n: Int): Vector[A] =  EIterator.lookaheadOf[A](this, n)



  /** Discard items on this EIterator.
   *
   *  @param n is # of items to discard.
   */

    def discarded(n: Int): Unit = EIterator.discard[A](this, n)

    

  /** Discard items on this EIterator.
   *
   *  @param keep
   *  @param skip are two conditions.
   *  @effect is items on this EIterator are discarded until keep becomes
   *          true or skip becomes false.
   */

    def discardedWhile(
      skip: A => Boolean, 
      keep: A => Boolean = (a: A) => false): 
    Unit = {

      EIterator.discardWhile(this, skip, keep)

    }


  /** Selectively collect items on this EIterator.
   *
   *  @param skip 
   *  @param keep
   *  @param filter are three conditions.
   *  @param n      is max # of items to return.
   *  @effects items are deleted from this EIterator when skip is true
   *           except items satisfying keep. items satisfying both
   *           keep and filter are returned until both skip and keep
   *           become false.
   *  @return  the kept and filtered items.  
   */

    def collectedWhile(
      skip:   A => Boolean,
      keep:   A => Boolean = (a: A) => true,
      filter: A => Boolean = (a: A) => true): 
    Vector[A] = {

      EIterator.collectWhile[A](this, skip, keep, filter)

    }


    def collectedNWhile(
      skip:   A => Boolean,
      keep:   A => Boolean = (a: A) => true,
      filter: A => Boolean = (a: A) => true,
      n:      Int          = 1): 
    Vector[A] = {

      EIterator.collectNWhile[A](this, skip, keep, filter, n)

    }



/*
 * Omit for now
 *

    //
    // Endow this EIterator with comprehension syntax.
    //


    override def foreach[B](f: A => B): Unit = EIterator.foreach[A, B](this)(f)

    override def map[B](f: A => B): EIterator[B] = EIterator.map[A, B](this)(f)

    override def withFilter(f: A => Boolean): EIterator[A] = {

      EIterator.withFilter[A](this)(f)

    }

    def flatMap[B](f: A => EIterator[B]): EIterator[B] = {

      EIterator.flatMap[A, B](this)(f)
 
    }

  *
  * End omission.
  */


  } // End trait EIterator





  object EIterator {

    import scala.collection.mutable.Queue


    //
    // Constructors for EIterator
    //


  /** Construct an EIterator from an ordinary iterator.
   *
   *  @param it is an ordinary iterator.
   *  @return an EIterator wrapped around it.
   */

    def apply[A](it:Iterator[A]): EIterator[A] = fromIterator(it)


    def fromIterator[A](it:Iterator[A]): EIterator[A] = new EIterator[A] {
      override def myHasNext = it.hasNext
      override def myNext() = it.next()
      override def myClose() = { }
    }



  /** Construct an EIterator from a vector.
   *
   *  @param it is a vector.
   *  @return an EIterator wrapped around it.
   */

    def apply[A](it: Iterable[A]): EIterator[A] = fromVector(it)


    def fromVector[A](entries: Iterable[A]): EIterator[A] = new EIterator[A] {
      import java.util.NoSuchElementException
      buffer.enqueueAll(entries)
      override def myHasNext = false
      override def myNext() = throw new NoSuchElementException("")
      override def myClose() = { }
    }



  /** Construct an EIterator from a file.
   *
   *  @param filename is a name of a file.
   *  @param deserializer is a function for deserializing the file.
   *  @return an EIterator on the file.
   */

    def apply[A](
      filename:     String, 
      deserializer: String => EIterator[A]): 
    EIterator[A] = {

      fromFile(filename, deserializer)

    }


    def fromFile[A](
      filename:     String, 
      deserializer: String => EIterator[A]):
    EIterator[A] = {

      deserializer(filename)

    }



    /** @param itX
     *  @param itY are two EIterator.
     *  @return concatenation of these two EIterator.
     */

    def concat[A](itX: EIterator[A], itY:EIterator[A]): EIterator[A] =  {
      new EIterator[A] {
        override def myHasNext = itX.hasNext || itY.hasNext
        override def myNext() = if (itX.hasNext) itX.next() else itY.next()
        override def myClose() = { itX.close(); itY.close() }
      }
    }



    //
    // Endow this EIterator with shadowing functionalities.
    //


  /** @param it is an EIterator
   *  @return head of it w/o shifting it.` 
   */

    def headOf[A](it: EIterator[A]): A = try {
      it.buffer.isEmpty match {
        case false => it.buffer.head
        case true  => it.buffer.enqueue(it.myNext()); it.buffer.head
      }
    }
    catch { case e: Throwable => it.close(); throw e }


  /** @param it is an EIterator
   *  @param n is # of items to look ahead.
   *  @return the next n items on this EIterator.
   */

    def lookaheadOf[A](it: EIterator[A], n: Int): Vector[A] = {
      val acc = it.buffer.clone()
      var m = n - acc.size
      while (it.myHasNext && (m > 0)) {
        val c = it.myNext()
        it.buffer.enqueue(c)
        acc.enqueue(c)
        m = m - 1
      }
      acc.take(n).toVector
    }


  /** Discard items on an EIterator.
   *
   *  @param n is # of items to discard
   *  @effect n items discarded from EIterator.
   */

    def discard[A](it: EIterator[A], n: Int): Unit = {
      var m = n
      while (it.hasNext && m > 0) {
        it.next()
        m = m - 1
      }
    }

    
  /** Discard items on an EIterator.
   *
   *  @param  it is an EIterator
   *  @param  skip
   *  @param  keep are two conditions.
   *  @effect is items on this EIterator are discarded until keep becomes
   *          true or skip becomes false.
   */

    def discardWhile[A](
      it:   EIterator[A],
      skip: A => Boolean, 
      keep: A => Boolean = (a: A) => false): 
    Unit = {

      while (it.hasNext && skip(it.head) && !keep(it.head)) { 
        it.buffer.dequeue()
      }
    }


  /** Selectively collect items on an EIterator.
   *
   *  @param   it     is an EIterator
   *  @param   skip
   *  @param   keep 
   *  @param   filter are three conditions.
   *  @param   n      is max # of items to return.
   *  @effects items are deleted from EIterator it when skip is true
   *           except items satisfying keep. items satisfying both
   *           keep and filter are returned until both skip and keep
   *           become false.
   *  @return  the kept and filtered items.  
   */

    def collectWhile[A](
      it:     EIterator[A],
      skip:   A => Boolean, 
      keep:   A => Boolean = (a: A) => true,
      filter: A => Boolean = (a: A) => true):
    Vector[A] = {

      val acc = Queue[A]()
      def processBuffer(a: A) =  (keep(a), filter(a), skip(a)) match {
        case (true, true, _)  => acc.enqueue(a); true
        case (true, _, _)     => true
        case (_, _, true)     => false
        case (_, _, false)    => true
      } 
      def processEIter(a: A) = (keep(a), filter(a), skip(a)) match {
        case (true, true, _) => acc.enqueue(a); it.buffer.enqueue(a); true
        case (true, _, _)    => it.buffer.enqueue(a); true
        case (_, _, true)    => true
        case (_, _, false)   => it.buffer.enqueue(a); false
      }
      it.buffer.filterInPlace(processBuffer(_))
      while (it.myHasNext && processEIter(it.myNext())) {
      }
      acc.toVector 
    }


    def collectNWhile[A](
      it:     EIterator[A],
      skip:   A => Boolean, 
      keep:   A => Boolean = (a: A) => true,
      filter: A => Boolean = (a: A) => true,
      n:      Int          = 1):
    Vector[A] = {
      
      var m = n
      val acc = Queue[A]()
      def processBuffer(a: A) =  (keep(a), filter(a), skip(a), m > 0) match {
        case (true, true, _, true)  => acc.enqueue(a); m = m - 1; true
        case (true, _, _, _)        => true
        case (_, _, true, _)        => false
        case (_, _, false, _)       => true
      } 
      def processEIter(a: A) = (keep(a), filter(a), skip(a)) match {
        case (true, true, _) =>
          acc.enqueue(a) 
          m = m - 1
          it.buffer.enqueue(a)
          true
        case (true, _, _)   => it.buffer.enqueue(a); true
        case (_, _, true)   => true
        case (_, _, false)  => it.buffer.enqueue(a); false
      }
      it.buffer.filterInPlace(processBuffer(_))
      while (it.myHasNext && m > 0 && processEIter(it.myNext())) {
      }
      acc.toVector 
    }



 /*
  * Omit for now
  *

    //
    // Endow EIterator with comprehension syntax
    //


    def foreach[A, B](it: EIterator[A])(f: A => B): Unit = {
      while (it.hasNext) f(it.next()) 
      it.close()
    }


    def map[A, B](it: EIterator[A])(f: A => B): EIterator[B] = {
      new EIterator[B] {
        override def myHasNext = it.hasNext
        override def myNext() = f(it.next())
        override def myClose() = it.close()
      }
    }


    def withFilter[A](it: EIterator[A])(f: A => Boolean): EIterator[A] = {
      new EIterator[A] {
        var mybuf = None: Option[A]
        def seekNext() = {
          while (it.hasNext && mybuf == None) {
            val nx = it.next()
            if (f(nx)) mybuf = Some(nx)
          }
          if (!it.hasNext) it.close()
        }

        override def myHasNext = { seekNext(); mybuf != None }

        override def myNext() = try { 
          seekNext()
          mybuf.get
        } 
        finally { mybuf = None }
        
        override def myClose() = it.close()
      }
    }


    def flatMap[A, B](it: EIterator[A])(f: A => EIterator[B]) : EIterator[B] = {
      new EIterator[B] {
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
    }

  *
  * End omission
  */


  } // End object EIterator


} // End SyncCollections



/*
 * Example 
 *
   {{{

import synchrony.iterators.SyncCollections._

def bf(x:Int,y:Int) = x < y
def cs(x:Int,y:Int) = (y>= x-1) && (x+1 >= y)
def fi(x:Int, y:Int) = true
def a = LmTrack(Iterator(1,2,3,4,5,6,7,8,9,10))
def b = ExTrack(Iterator(1,2,3,4,5,6,7,8,9,10))
def c = ExTrack(Iterator(4,5,8,9,11,12))

def connect(ex:Boolean) = {
  val as = a; 
  val bs = as.sync(b,bf,cs,fi,ex);
  (as,bs)
  }

def connect2(ex:Boolean) = {
  val as = a; 
  val bs = as.sync(c,bf,cs,fi, ex);
  (as,bs)
  }

def connect3(ex:Boolean) = {
  val as = a; 
  val bs = as.syncN(c,bf,cs,fi, 1, ex);
  (as,bs)
  }

def eg1(ex:Boolean) = {
  val (as, bs) = connect(ex)
  for(x <- as; y = bs.syncedWith(x)) yield (x,y) }

def eg2(ex: Boolean) = {
  val (as, bs) = connect(ex)
  for (x <- as;
    u <- bs.syncedWith(x);
    v <- bs.syncedWith(x) if (u != v))
  yield (x, u) }

def eg3(ex:Boolean) = {
  val (as, bs) = connect2(ex)
  for(x <- as; y = bs.syncedWith(x)) yield (x,y) }

def eg4(ex:Boolean) = {
  val (as, bs) = connect3(ex)
  for(x <- as; y = bs.syncedWith(x)) yield (x,y) }


def result[A](eg:Iterator[A]) = {
  var count = 0
  eg foreach (x => { println(x); count += 1 })
  count }

result(eg1(true))
result(eg1(false))
result(eg2(true))
result(eg2(false))
result(eg3(true))
result(eg3(false))
result(eg4(true))
result(eg4(false))

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



scala> result(eg3(true))
(1,Vector())
(2,Vector())
(3,Vector(4))
(4,Vector(5))
(5,Vector())
(6,Vector())
(7,Vector(8))
(8,Vector(9))
(9,Vector())
(10,Vector(11))
res4: Int = 10



scala> result(eg3(false))
(1,Vector())
(2,Vector())
(3,Vector(4))
(4,Vector(4, 5))
(5,Vector(4, 5))
(6,Vector(5))
(7,Vector(8))
(8,Vector(8, 9))
(9,Vector(8, 9))
(10,Vector(9, 11))
res5: Int = 10


      }}}
 *
 */



