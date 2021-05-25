
package synchrony.iterators  

/** Synchrony iterators provide generalized synchronized iterations,
 *  e.g. performining a merge join, on multiple iterators.  This module
 *  implements Synchrony's main iterator structure. 
 *
 *  Conceptually, a Synchrony iterator is an iterator over a source 
 *  called the "landmark track" (LmTrack) and simultaneously over one
 *  or more other sources called the "experiment track" (ExTrack). 
 *  Usually, LmTrack and ExTrack are files, collections, or normal
 *  iterators.  In a Synchrony iterator, the LmTrack "synchronizes
 *  the ExTrack. Two predicates, isBefore (bf) and canSee (cs), are
 *  used to define the synchronization.
 *
 *  Intuitively, cs(a,b) means the element a on an ExTrack "can see"
 *  (i.e. is synchronized with the element b on the LmTrack.  And 
 *  bf(a,b) means the position or locus of the element a on an ExTrack
 *  corresponds to some position or locus strickly "before" or 
 *  "in front of" the position or locus of the element b on the LmTrack.
 * 
 *  Synchrony makes some assumptions to efficiently synchronize iterators
 *  on the LmTrack and ExTrack. These assumptions are the monotonocity
 *  conditions (i) & (ii), and antimonotonicity conditions (iii) & (iv).
 *  Let x and y be elements on a specific track (e.g. in the same file,
 *  collection, or iterator). Let x << y means the physical position of 
 *  x is in front of the physical position of y in the file, collection,
 *  or iterator; i.e. an iteration on the track is going to encounter x 
 *  before y. Then the monotonicity and antimonotonicty conditions 
 *  sufficient to ensure correctness of a Synchrony iterator are:
 * 
 *    (i) b << b' iff for all a in ExTrack: bf(a, b) implies bf(a, b').
 * 
 *   (ii) a' << a iff for all b in LmTrack: bf(a, b) implies bf(a', b).
 * 
 *  (iii) If b << b', then 
 *             for all a in ExTrack: bf(a, b) and not cs(a, b),
 *                                   implies not cs(a, b').
 * 
 *   (iv) If a << a', then 
 *             for all b in LmTrack: not bf(a, b) and not cs(a, b),
 *                                   implies not cs(a', b).
 *
 * isBefore/canSee are predicates on a pair of files which are not
 * necessarily of the same type. Often, these files do have the same
 * type. 
 *
 * Also, canSee is not required to be reflexive and convex.
 * But it often is:
 *
 *   (v) reflexive:  for all x in X: x canSee x.
 *
 *  (iv) convex:     for all (y << y' << y'' | Y): 
 *                     y canSee x and y'' canSee x implies y' canSee x; and 
 *                   for all (x << x' << x'' | X): 
 *                     y canSee x and y canSee x'' implies y canSee x'.
 * 
 * Wong Limsoon
 * 6 April 2021
 */



object SyncCollections {

  import scala.collection.mutable.{ Queue, ArrayBuffer }
  import scala.reflect.ClassTag

  var DEBUG     = true
  var PEEKAHEAD = 10
  var PATIENCE  = 4




  /** EIterators are intended as iterators on files. EIterators 
   *  provides: (i) closing the files midway during an iteration,
   *  (ii) arbitrarily long look-ahead, (iii) editable look-ahead
   *  buffer, and most importantly, (iv) Synchrony iteration.
   *
   *  An EIterator is wrapped on top of an existing ordinary
   *  Iterator, which we call here the "underlying" iterator.
   */


  trait EIterator[A] extends Iterator[A] { self =>

    //
    // The editable look-ahead buffer
    //

    val buffer = new Queue[A]()



    //
    // Functions to be overridden by inheriting classes.
    // These define the underlying iterator.
    //


  /** @return whether there are more elements in this underlying
   *          iterator.  This corresponds to the hasNext 
   *          predicate in an ordinary Iterator.
   */

    protected def myHasNext: Boolean




  /** @return the next element in this underlying iterator. This
   *          corresponds to the next() method in an ordinary 
   *          Iterator.
   */

    protected def myNext(): A




  /** @effect close this underlying iterator. In case this
   *          underlying iterator is opened on a file or a resource,
   *          this is a method for closing the file or releasing
   *          the resource.
   */

    protected def myClose(): Unit




    //
    // The main EIterator functions 
    //


  /** Is the underlying iterator closed?
   */

    protected var closed: Boolean = false



  /** @effect close this EIterator.
   */

    def close(): Unit = { myClose(); closed = true }



  /** @return whether there are more elements on this EIterator.
   */

    override def hasNext = (buffer.isEmpty && (closed || !myHasNext)) match {
      case true  => close(); false
      case false => true
    }
      


  /** @return the next element on this EIterator.
   */

    override def next(): A = try {
      buffer.isEmpty match {
        case true  => myNext() 
        case false => buffer.dequeue()
      }
    }
    catch { case e: Throwable => close(); throw e }


  /** @return the next n elements (as a vector).
   */

    def nextN(n: Int): Vector[A] = {
      val nItems: Vector[A] = lookahead(n)
      buffer.dropInPlace(n)
      nItems
    }


  /** @return an EIterator that presents elements n at a time.
   */

    def grouped(n: Int): EIterator[Vector[A]] =  new EIterator[Vector[A]] {
      override def myHasNext: Boolean = self.myHasNext
      override def myNext(): Vector[A] = self.nextN(n)
      override def myClose(): Unit = self.close()
    }



    //
    // Endow this EIterator with lookahead.
    //


  /** @return head of this EIterator w/o shifting it.` 
   */

    def head: A = try { 
      if (buffer.isEmpty) buffer.enqueue(myNext())
      buffer.head
    }
    catch { case e: Throwable => close(); throw e }




  /** @return this EIterator after dropping its head.
   */

    def tail(): EIterator[A] = {
      next()
      this
    }



  /** @param n is # of items to look ahead.
   *  @return the next n items on this EIterator.
   */

    def lookahead(n: Int): Vector[A] = {
      var m = n - buffer.length
      while (myHasNext && (m > 0)) {
        val c = myNext()
        buffer.enqueue(c)
        m = m - 1
      }
      buffer.take(n).toVector
    }



  /** @param n is the n-th item to peek at.
   *  @return the n-th item
   */

    def peekahead(n: Int): Option[A] = {
      var item: Option[A] = None
      var m = n - buffer.length
      (m > 0) match {
        case true  => 
          while (myHasNext && (m > 0)) {
            val c = myNext()
            buffer.enqueue(c)
            m = m - 1
            if (m == 0) { item = Some(c) }
          }
          item
        case false =>
          Some(buffer(n - 1))
      }
    }


 

  /**  @param  stuff is a vector of items to be inserted.
   *   @return this EIterator with stuff inserted in front.
   */

    def ++:(stuff: Iterable[A]): EIterator[A] = {
      buffer.prependAll(stuff)
      this
    }




  //
  // Endow this EIterator with Synchrony iterator functionality.
  //

  /** Use this EIterator as an ExTrack.
   *
   *  Seek/SyncedWith defines the synchronizationb between this
   *  ExTrack and an LmTrack. The input b to which seek/syncedWith
   *  is applied is assumed to be an element on the LmTrack, and
   *  is being supplied as input to seek/syncedWith in the order
   *  in LmTrack. The isBefore/canSee predicates are assumed to
   *  satisfy the monotonicity/antimonotonicity conditions wrt
   *  this ExTrack/LmTrack.  
   *
   *  @param isBefore
   *  @param canSee   are predicates defining the Synchrony iterator, 
   *                  satisfying monotonocity and antimonotonicity.
   *
   *  @param screen   is a filter for filtering results.
   *
   *  @param n        is # of elements to return, from this ExTrack
   *                  which are synchronized to the current element b 
   *                  on this LmTrack to return. If n < 0 is given,
   *                  return all elements synchronized to b. 
   *
   *  @param convex   whether canSee is convex.
   *
   *  @param b        is the current element on this LmTrack.
   *
   *  @return         the # of elements on this Extrack that can
   *                  see b currently.
   */

    private var cachedR: Vector[A] = Vector[A]()
    private var cachedB: Any       = _

    def seek[B]
      (isBefore: (A, B) => Boolean,
       canSee:   (A, B) => Boolean,
       screen:   (A, B) => Boolean = (x: A, y: B) => true,
       n:        Int               = -1,
       convex:   Boolean           = false)
      (b: B):
    Vector[A] = {

      if (b != cachedB) {
        cachedR = EIterator
                  .seek[A, B](this)(isBefore, canSee, screen, n, convex)(b)
        cachedB = b
      }
      cachedR
    }


    def syncedWith[B]
      (b: B)
      (isBefore: (A, B) => Boolean,
       canSee:   (A, B) => Boolean,
       screen:   (A, B) => Boolean = { (x: A, y: B) => true },
       n:        Int               = -1,
       convex:   Boolean           = false):
    Vector[A] = {

      if (b != cachedB) {
        cachedR = EIterator
                  .seek[A, B](this)(isBefore, canSee, screen, n, convex)(b)
        cachedB = b
      }
      cachedR
    }


  /** @require canSee is convex.
   *  @effect  quickly drop, as many as possible, items in this EIterator 
   *           that isBefore b and cannot see b.
   */ 

    def position[B]
      (isBefore: (A, B) => Boolean,
       canSee:   (A, B) => Boolean)
      (b: B):
    Unit = {

      EIterator.position[A, B](this)(isBefore, canSee)(b)

    }



    //
    // Endow this EIterator with comprehension syntax.
    //


    override def foreach[B](f: A => B): Unit = EIterator.foreach[A, B](this)(f)

    override def map[B](f: A => B): EIterator[B] = EIterator.map[A, B](this)(f)

    override def withFilter(f: A => Boolean): EIterator[A] = {
      EIterator.withFilter[A](this)(f)
    }

    override def filter(f: A => Boolean): EIterator[A] = {
      EIterator.withFilter[A](this)(f)
    }

    def flatMap[B](f: A => EIterator[B]): EIterator[B] = {
      EIterator.flatMap[A, B](this)(f)
    }


  } // End trait EIterator



  object EIterator {


    //
    // Constructors for EIterator
    //


  /** Construct an EIterator from an ordinary iterator.
   *
   *  @param it is an ordinary iterator.
   *  @return   an EIterator wrapped around it.
   */

    def apply[A](it:Iterator[A]): EIterator[A] = fromIterator(it)


    def fromIterator[A](it:Iterator[A]): EIterator[A] = it match {
      case eit: EIterator[A] => eit
      case _                 => new EIterator[A] {
        override def myHasNext = it.hasNext
        override def myNext() = it.next()
        override def myClose() = { }
      }
    }


  /** Construct an EIterator from an array.
   *
   *  @param it is an array.
   *  @return   an EIterator wrapped around it.
   */

  def apply[A](it: Array[A]): EIterator[A] =  fromArray(it)


  def fromArray[A](it: Array[A]): EIterator[A] =  new EIterator[A] {
    var myCurr = 0
    override def myHasNext = myCurr < it.length
    override def myNext() = try it(myCurr) finally { myCurr = myCurr + 1 }
    override def myClose() = { }
  }


  /** Construct an EIterator from a vector.
   *
   *  @param it is a vector.
   *  @return   an EIterator wrapped around it.
   */

    def apply[A: ClassTag](it: Iterable[A]): EIterator[A] = fromVector(it)


    def fromVector[A: ClassTag](it: Iterable[A]): EIterator[A]
    = fromArray[A](it.toArray[A])



  /** Construct an empty EIterator
   */

    def empty[A] = new EIterator[A] {
      override def myClose()   = {  }
      override def myHasNext   = false
      override def myNext(): A = throw new java.util.NoSuchElementException()
    }




  /** Construct an EIterator from a file.
   *
   *  @param filename     is a name of a file.
   *  @param deserializer is a function for deserializing the file.
   *  @return             an EIterator on the file.
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



    def StringfromFile(
      filename: String,
      guard:    String => Boolean = (_ : String) => true): 
    EIterator[String] = {

      new EIterator[String] {
        val file = scala.io.Source.fromFile(filename)
        val it   = file.getLines().map(_.trim).filter(guard)

        override def myHasNext = closed match {
          case true  => false
          case false => it.hasNext match {
            case true  => true
            case false => myClose(); false 
          }
        }

        override def myNext() = it.next()

        override def myClose() = { closed = true; file.close() }
      }
    }



    //
    // Endow EIterator with Synchrony iterator functionalities.
    //


    def seek[A, B]
      (it:       EIterator[A])
      (isBefore: (A, B) => Boolean,
       canSee:   (A, B) => Boolean,
       screen:   (A, B) => Boolean  = (x: A, y: B) => true,
       n:        Int                = -1,
       convex:   Boolean            = false )
      (b: B):
    Vector[A] = {

      // isBefore and canSee satisfy the monotonicity and
      // antimonotonicity defining a Synchrony iterator.

      var m     = n
      var zs    = Vector[A]()
      var mybuf = Vector[A]()
      var p     = 0
      
      def aux(): Unit = (it.isEmpty, zs.isEmpty, m > 0 || m < 0) match {
        case (true, true, _)  => { it.close() }
        case (true, false, _) => { zs ++: it  }
        case (_, _, false)    => { zs ++: it  }
        case _ =>
          val a = it.head
          p = p + 1
          if (isBefore(a, b) && !canSee(a, b)) {
            // isBefore(a,b) and !canSee(a,b) implies 
            // forall b' after b: !canSee(a,b').
            // So a can be discarded safely, no need to save it in zs.
            it.tail()
            if (convex && p == PATIENCE) { 
              it.position(isBefore, canSee)(b); p = 0
            }
            aux()
          }
          else if (!isBefore(a, b) && !canSee(a, b)) {
            // !isBefore(a,b) and !canSee(a,b) implies 
            // forall a' after a: !canSee(a',b).
            // So b can be discarded safely. But the next b may still be
            // able to see some a saved earlier in zs.
            zs ++: it
            zs = Vector[A]()
          }
          else it.peekahead(2) match {
            case None =>
              // At this point, canSee(a,b). If there is no more a, 
              // then we are finished with this b. Note that, the next
              // b may still be able to see this a, as well as zs.
              // so stuff zs back into it.  
              zs ++: it
              if (screen(a, b)) { mybuf = mybuf :+ a; m = m - 1 }
              zs = Vector[A]()

            case Some(_) =>
              // At this point, canSee(a, b). But there maybe more a
              // in it that can see this b. So keep accumulating in zs.
              it.tail()
              zs = zs :+ a
              if (screen(a, b)) { mybuf = mybuf :+ a; m = m - 1 }
              aux()
          }
      }
      aux()
      mybuf
    }



    def position[A,B]
      (it:       EIterator[A])
      (isBefore: (A, B) => Boolean, 
       canSee:   (A, B) => Boolean)
      (b: B):
    Unit = {
 
      // Require canSee is convex.

      do {  }
      while (it.peekahead(PEEKAHEAD) match {
        case None    => false
        case Some(a) => 
          (isBefore(a, b), canSee(a, b)) match {
             case (true, false) => it.buffer.dropInPlace(PEEKAHEAD); true
             case _ => false
          }
        }
      )
    }





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
        var mybuf = empty[B]
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

  } // End object EIterator



/** Endow eiterators with positional index.
 *
 *  This makes it more convenient to use Synchrony eiterators
 *  to look at consecutive positions.
 *
 */

  object IndexedEIterator {

    import scala.reflect.ClassTag


    def apply[A](it: Iterator[A]): EIterator[(A, Int)] = fromIterator(it)

    def fromIterator[A](it: Iterator[A]): EIterator[(A, Int)] = 
      new EIterator[(A, Int)] {
        var myCurr = 0
        override def myHasNext = it.hasNext
        override def myClose() = it match {
          case eit: EIterator[A] => eit.close()
          case _ => { }
        }
        override def myNext() = try (it.next(), myCurr) finally { myCurr += 1 }
    }


    def apply[A](it: Array[A]): EIterator[(A, Int)] =  fromArray(it)

    def fromArray[A](it: Array[A]): EIterator[(A, Int)] =
      new EIterator[(A, Int)] {
        var myCurr = 0
        override def myHasNext = myCurr < it.length
        override def myClose() = { }
        override def myNext() = try (it(myCurr), myCurr) finally { myCurr += 1 }
      }


    def apply[A: ClassTag](it: Iterable[A]): EIterator[(A, Int)] =
      fromVector(it)

    def fromVector[A: ClassTag](it: Iterable[A]): EIterator[(A, Int)] = 
      fromArray[A](it.toArray[A])



    def apply[A](filename: String, deserializer: String => EIterator[A])
    : EIterator[(A, Int)] = fromFile(filename, deserializer)

    def fromFile[A](filename: String, deserializer: String => EIterator[A])
    : EIterator[(A, Int)] = fromIterator(deserializer(filename))

  } // End object IndexedEIterator




  /** An EIterator is an iterator on one file/resource,
   *  and a Synchrony iterator is an iterator on a pair of
   *  files/resources, and so an EIterator only defines a
   *  Synchrony iterator "implicitly": viz. the EIterator
   *  itself is explicitly an ExTrack (this is what the 
   *  seek/syncedWith method is for), while the LmTrack is
   *  implicit (as it manifests only in the order in which
   *  the input b is provided to seek/syncedWith.
   *
   *  Some people probably prefer a more explicit structure
   *  for Synchrony iterators. This structure is defined below:
   *  "SynchroEIterator".
   */


  class SynchroEIterator[A, B, C](
    isBefore:   (A, B) => Boolean,
    canSee:     (A, B) => Boolean,
    screen:     (A, B) => Boolean,
    grpscreen:  (B, Vector[A]) => Boolean,
    iter:       (B, Vector[A]) => C,
    n:          Int,
    convex:     Boolean,
    early:      Boolean,
    extrack: => EIterator[A],
    lmtrack: => EIterator[B])
  extends EIterator[C] {

    // Requires: isBefore is monotonic wrt (itB, itA) 
    //      and  canSee is antimonotonic wrt isBefore.
    // The monotonicity of isBefore can be guaranteed if 
    // itB and itA are sorted in a way consistent with isBefore.


    var b: B  = _
    val bs    = lmtrack
    val as    = extrack
    var mybuf = None: Option[Vector[A]]


    def seekNext(): Unit = {
      while (mybuf == None && !bs.isEmpty) {
        b      = bs.next()
        val aa = as.seek(isBefore, canSee, screen, n, convex)(b)
        if (grpscreen(b, aa)) { mybuf = Some(aa) }
      }
      if (bs.isEmpty) { myClose() }
      if (early && as.isEmpty) { myClose() }  
    }


    override def myClose() = { bs.close(); as.close() }


    override def myHasNext = { seekNext(); mybuf != None }


    override def myNext() = try { 
      seekNext()
      iter(b, mybuf.get)
    } 
    finally { mybuf = None }


  }  // End class SynchroEIterator



  object SynchroEIterator {


  /** Construct a "default" SynchroEIterator. 
   *
   *  After LmTrack and ExTrack are synchronized", this SynchroEIterator
   *  produces a stream of pairs (b1, aa1), (b2, aa2), ...,
   *  where b1, b2, ... are elements on the LmTrack, 
   *  and aa1, aa2, ... are respectively vectors of elements on the 
   *  ExTrack that can see b1, b2, ... respectively.
   */
  
    def apply[A, B]
      (isBefore:  (A, B) => Boolean,
       canSee:    (A, B) => Boolean,
       extrack:   EIterator[A],
       lmtrack:   EIterator[B],
       screen:    (A, B) => Boolean         = (x: A, y: B) => true,
       grpscreen: (B, Vector[A]) => Boolean = (y: B, x: Vector[A]) => true,
       n:          Int                      = -1,
       convex:    Boolean                   = false,
       early:     Boolean                   = false ):
    SynchroEIterator[A, B, (B, Vector[A])]  = {

      def iter(b: B, as: Vector[A]) = (b, as)

      new SynchroEIterator[A, B, (B, Vector[A])](
            isBefore, canSee, 
            screen, grpscreen, iter _, n, convex, early,
            extrack, lmtrack)
    }



  /** Construct a SynchroEIterator and apply a map function, iter,
   *  to its elements (b1, aa1), (b2, aa2), ... to produces a stream
   *  of results c1, c2, ...
   */

    def map[A, B, C]
      (isBefore:  (A, B) => Boolean,
       canSee:    (A, B) => Boolean,
       extrack:   EIterator[A],
       lmtrack:   EIterator[B],
       screen:    (A, B) => Boolean         = (x: A, y: B) => true,
       grpscreen: (B, Vector[A]) => Boolean = (y: B, x: Vector[A]) => true,
       n:          Int                      = -1,
       convex:    Boolean                   = false,
       early:    Boolean                    = false )
      (iter:      (B, Vector[A]) => C):
    SynchroEIterator[A, B, C] = {

      new SynchroEIterator[A, B, C](
            isBefore, canSee, 
            screen, grpscreen, iter, n, convex, early,
            extrack, lmtrack)
    }



    def ext[A, B, C]
      (isBefore:  (A, B) => Boolean,
       canSee:    (A, B) => Boolean,
       extrack:   EIterator[A],
       lmtrack:   EIterator[B],
       screen:    (A, B) => Boolean         = (x: A, y: B) => true,
       grpscreen: (B, Vector[A]) => Boolean = (y: B, x: Vector[A]) => true,
       n:          Int                      = -1,
       convex:    Boolean                   = false,
       early:    Boolean                    = false )
      (iter:      (B, Vector[A]) => Vector[C]):
    EIterator[C] = new EIterator[C] {

      val synchro = new SynchroEIterator[A, B, Vector[C]](
            isBefore, canSee, 
            screen, grpscreen, iter, n, convex, early,
            extrack, lmtrack)

      var pos = 0
      var res = Vector[C]()

      def seekNx(): Unit =
       (res.isEmpty, pos >= res.length, synchro.hasNext) match {
          case (false, false, _) => { }
          case (_, true, false)  => { myClose() }
          case (_, true, true)   => { res = synchro.next(); pos = 0; seekNx() }
          case (true, _, false)  => { myClose() }
          case (true, _, true)   => { res = synchro.next(); pos = 0; seekNx() }
      }

      override def myClose() = synchro.close()

      override def myHasNext = {
        seekNx()
        !res.isEmpty && pos < res.length
      }

      override def myNext() = 
        try { seekNx(); res(pos) } 
        finally { pos = pos + 1 }
        
    }



  /** Construct a SynchroEIterator, filter its result. 
   *
   *  The filter is specify by a function screen of pairs (ai, bj) and,
   *  optionally by a function grpscreen on pairs (bj, aaj). Here ai is
   *  the ith element on the ExTrack, bj is the element on the LmTrack,
   *  and ai can see bj. Also, aaj is the vector of elements on the ExTrack
   *  that can see bj. 
   */

    def filter[A, B]
      (isBefore:  (A, B) => Boolean,
       canSee:    (A, B) => Boolean,
       extrack:   EIterator[A],
       lmtrack:   EIterator[B],
       n:         Int = -1,
       convex:    Boolean = false,
       early:     Boolean = false)
      (screen:    (A, B) => Boolean,
       grpscreen: (B, Vector[A]) => Boolean = (y: B, x: Vector[A]) => true):
    SynchroEIterator[A, B, (B, Vector[A])] = {

      def iter(b: B, as: Vector[A]) = (b, as)

      new SynchroEIterator[A, B, (B, Vector[A])](
            isBefore, canSee, 
            screen, grpscreen, iter _, n, convex, early,
            extrack, lmtrack)
    }

  }  // End object SynchroEIterator



} // End SyncCollections



/*
 * Example 
 *
   {{{

import synchrony.iterators.SyncCollections._

def bf(x:Int,y:Int) = x < y
def cs(x:Int,y:Int) = (y>= x-1) && (x+1 >= y)
def fi(x:Int, y:Int) = true
def gfi(x:Int, y:Vector[Int]) = true


def aa = for ((b, as) <- SynchroEIterator(
                          isBefore = bf _,
                          canSee = cs _,
                          extrack = EIterator (Iterator(1,2,3,4,5,6,7,8,9,10)),
                          lmtrack = EIterator (Iterator(1,2,3,4,5,6,7,8,9,10)));
               a <- EIterator( b * 10 +: as))
          yield a

aa.toVector



def aa = EIterator(Vector(1,2,3,4,5,6,7,8,9,10, 100))
def bb = EIterator(Vector(1,2,3,4,5,6,7,8,9,10, 200))


def eg1(implicit n: Int = -1) = {
  val as = aa
  val bs = bb
  for (a <- as; 
       b <- bs.syncedWith(a)(isBefore = bf, canSee = cs, n = n))
  yield (a, b)
}.toVector



def eg2 = {
  val as = aa
  val bs1 = bb
  val bs2 = bb
  for (a <- as;
       u <- bs1.syncedWith(a)(isBefore = bf, canSee = cs);
       v <- bs2.syncedWith(a)(isBefore = bf, canSee = cs);
       if (u != v))
  yield (a, u, v)
}.toVector



      }}}
 *
 */



