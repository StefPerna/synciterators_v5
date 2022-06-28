
package dbmodel

/** Version 9
 *
 *  Wong Limsoon
 *  4 March 2022
 */


object DBModel {

/** Organization:
 *
 *  object CloseableIterator
 *  // Contains: CBI (* Closeable Buffered Iterator *)
 *
 *  object Synchrony
 *  // Contains: SIterator
 *
 *  object OrderedCollection
 *  // Contains: Key, OColl, OSeq, 
 *
 *  object Join  
 *
 *  object Predicates 
 *  // Contains: Pred, GenPred, Antimonotonic, AntimonotonicPred, 
 *  //           HasOrder (equal, lt, lteq, gt, gteq), 
 *  //           HasSize (szpercent), 
 *  //           HasDistance (intdist, dbldist, dl, dleq, dg, dgeq), 
 *  //           LINETOPO (overlap, near, far, inside, enclose, touch).
 *  //           DBNumeric
 *
 *  object OpG
 *  // Contains: biggest, smallest, count, sum, average, maximize, minimize
 */


  import scala.language.implicitConversions
  import scala.util.Using
  import scala.collection.BufferedIterator

  type Bool      = Boolean       
  type Ord[K]    = Ordering[K]
  type CLOSEABLE = java.lang.AutoCloseable





  object DEBUG 
  {
    var debug = true

    def message(msg: Any) = if (debug) System.err.println(msg)
  }



  trait RESOURCE extends java.lang.AutoCloseable 
  {
    var held: Seq[CLOSEABLE] = Seq()  // Resources held

    def use(resources: CLOSEABLE*): this.type = {
      held = resources ++ held 
      this
    }

    def userev(resources: CLOSEABLE*): this.type = {
      held = held ++ resources
      this
    }

    def close(): Unit = {
      held.foreach { r => 
        if (r != null) r.close() 
        else DEBUG.message("*** RESOURCE.close: null resource ***")
      }
      held = Seq()
    }
  }


  object RESOURCE {
    def apply(resource: CLOSEABLE): RESOURCE =
      new RESOURCE { held = Seq(resource) }

    def closeable(cl: () => Unit): CLOSEABLE = 
      new java.lang.AutoCloseable { def close(): Unit = cl() } 
  }
    


  object CloseableIterator {

    /** When iterators are associated with resources like files,
     *  we need to be able to release these resources when we
     *  are done with using these iterators.
     */


    trait CBI[B] extends BufferedIterator[B] with RESOURCE
    { 
      SELF =>

      /** Functions defining buffered iterator.
       */
    
      def head: B

      def hasNext: Bool

      def next(): B


      /** Functions for using this iterator that propagate its [[close()]] 
       *  function. Although one can invoke all BufferedIterator methods m
       *  on [[CBI]], it is best doing so via [[done { _.m }]],
       *  [[doit { _.m }]], etc. if you want to be sure that the [[CBI]]
       *  is closed after the operation.
       */

      def done[C](f: CBI[B] => C): C = Using.resource(this)(f)
  

      def doit[C](f: CBI[B] => IterableOnce[C]): CBI[C] = {
        val it = f(this)
        it match {
          case cbi: CBI[C] => cbi.userev(this)
          case _ => CBI(it).use(this)
        }
      }


      def step[C](f: CBI[B] => C): CBI[C] =
        CBI(() => this.hasNext, () => f(this)).use(this)


      override def filter(f: B => Bool): CBI[B] = 
      {
        var acc: Option[B] = None
        def hasnx(): Bool  = {
          while (acc == None && hasNext) { 
            val tmp = next()
            if (f(tmp)) { acc = Some(tmp) }
          }
          acc != None || { close(); false }
        }
        def nx() = { val h = acc.get; acc = None; h }
        CBI(hasnx _, nx _).use(this)
      }


      override def withFilter(f: B => Bool): CBI[B] = filter(f)
  

      override def map[C](f: B => C): CBI[C] =
        CBI(() => this.hasNext, () => f(this.next())).use(this)


      override def flatMap[C](f: B => IterableOnce[C]): CBI[C] =
      {
        var acc: CBI[C] = CBI.emptyCBI
        val clacc = RESOURCE.closeable { () => acc.close() }
        def hasnx(): Bool = {
          while (! acc.hasNext && SELF.hasNext) { 
            val it = f(SELF.next())
            acc = it match { case cbi: CBI[C] => cbi; case _ => CBI(it) }
          }
          acc.hasNext
        }
        def nx() = acc.next()
        CBI(hasnx _, nx _).use(clacc, this)
      }


      override def foreach[U](f: B => U): Unit =
        done(cbi => while (cbi.hasNext) { f(cbi.next()) })


      /** Function for convenience of converting between [[CBI]]
       * and other collection types.
       */

      def cbiterator: this.type = this

      def cbi: this.type = this

    }  // End trait CBIterator



    object CBI
    {
      /** Constructors for CBI
       */
  
      def emptyCBI[B]: CBI[B] = new CBI[B] {
        def hasNext = false
        def next()  = throw new NoSuchElementException("CBI.emptyCBI")
        def head    = throw new NoSuchElementException("CBI.emptyCBI")
      }


      def apply[B](hasnx: () => Bool, nx: () =>B): CBI[B] =
      {
        if (!hasnx()) { emptyCBI }
        else new CBI[B] {
          var isDefined: Bool = true
          var hd: B  = nx()
          def head: B = hd
          def hasNext: Bool = isDefined
          def next(): B = {
            val h = hd
            isDefined = hasnx()
            if (isDefined) { hd = nx() } else { close() }
            h
          }
        }
      }
        

      def apply[B](hasnx: () => Bool, nx: () =>B, cl: () => Unit): CBI[B] =
        CBI(hasnx, nx).use { RESOURCE.closeable(cl) }


      def apply[B](it: B*): CBI[B] = CBI[B](it)
  

      def apply[B](it: IterableOnce[B]): CBI[B] = it match {
        case cbi: CBI[B] => cbi
        case _ => 
          val ei = it.iterator
          CBI[B](() => ei.hasNext, () => ei.next())
      }


      def apply[B](it: IterableOnce[B], cl: () => Unit): CBI[B] = 
        CBI(it).use { RESOURCE.closeable(cl) }

    }  // End object CBI



    implicit class IterToCBI[B](it: IterableOnce[B]) {
      def cbiterator: CBI[B] = CBI(it)
      def cbi: CBI[B] = CBI(it)
    }

  }  // End object CloseableIterator




  object Synchrony {


  /** Synchrony iterators [[SIterator]] provide generalized synchronized
   *  iterations, e.g. performining a merge join, on multiple iterators.
   *  Synchrony iterators are much more efficient than nested loops and
   *  make efficient synchronized iterations very easy to express using
   *  comprehension syntax.  This module implements Synchrony's basic
   *  iterator structure. 
   *
   *  A Synchrony iterator-based program, such as
   *  {{{ 
   *      val si = SIterator(bs, bf, cs)
   *      for (a <- as; abs = si.syncedWith(a); if c(a, abs))
   *      yield (a,abs)
   *  }}}
   *  has the same meaning as
   *  {{{
   *      for (a <- as; abs = bs.filter(b=>cs(b,a)); if c(a,abs))
   *       yield (a,abs)
   *  }}}
   *  However, assuming each b in bs canSee (cs) only a small number of a 
   *  in as, then the former is linear time while the latter is quadratic
   *  time.
   *
   *  Conceptually, a Synchrony iterator simultaneously iterates on two 
   *  collections A and B.  Two predicates, isBefore (bf) and canSee (cs),
   *  are used to define the synchronization.
   *
   *  Intuitively, cs(b,a) means item b in collection B "can see" (i.e.,
   *  is synchronized with item a in collection A.)  And bf(b,a) means the
   *  position of b corresponds to some position strictly "before" or 
   *  "in front of" the position of of a, if we merge all A and B into one
   *  collection.
   * 
   *  Synchrony makes some assumptions to efficiently synchronize the
   *  iteration on the two collections.  These assumptions are the 
   *  monotonocity conditions (i) & (ii), and antimonotonicity
   *  conditions (iii) & (iv).
   *
   *  Let x and y be elements on a specific collection.  Let x << y means
   *  the physical position of x is in front of the physical position of y
   *  in the collection; i.e. an iteration on the collection is going to
   *  encounter x before y. The monotonicity and antimonotonicity conditions
   *  sufficient to ensure correctness of a Synchrony iterator are:
   * 
   *    (i) If a << a' in A, then for all b in B: bf(b,a) implies bf(b,a').
   * 
   *   (ii) If b' << b in B, then for all a in A: bf(b,a) implies bf(b',a).
   * 
   *  (iii) If a << a' in A, then for all b in B: bf(b,a) and not cs(b,a),
   *                                                 implies not cs(b,a').
   * 
   *   (iv) If b << b' in B, then for all a in A: not bf(b,a) and not cs(b,a),
   *                                                 implies not cs(b',a).
   *
   *  bf/cs are predicates on a pair of collections which are not necessarily
   *  of the same type. Often, these collections do have the same type however.
   *  Also, canSee is not required to be reflexive and convex. But it often is:
   *
   *    (v) reflexive:  for all x in X: cs(x, x).
   *
   *   (iv) convex:     for all b << b' << b'' in B:
   *                       cs(b,a) and cs(b'',a) implies cs(b',a);
   *                    for all a << a' << a'' in A:
   *                       cs(b,a) and cs(b,a'') implies cs(b,a');
   * 
   *  Please visit https://www.comp.nus.edu.sg/~wongls/projects/synchrony
   *  for detailed exposition.
   */

    import CloseableIterator._

    type ISBEFORE[B,A] = (B,A) => Bool   // Type of isBefore (bf) predicate
    type CANSEE[B,A]   = (B,A) => Bool   // Type of canSee (cs) predicate


    case class SIterator[A,B](
      cbi: CBI[B],                       // underlying iterator
      bf:  ISBEFORE[B,A],                // isBefore predicate 
      cs:  CANSEE[B,A])                  // canSee predicate 
    extends RESOURCE 
    { 
      use(cbi)     // Register [[cbi]] as a resource held.
  
      /** Cache for memoizing last result and rewinding.
       */

      private var ores: List[B] = List() // last result
      private var oa: Option[A] = None   // last a

      /** Set things up so that the [[SIterator]] can be used
       *  internally like an iterator equals to [[ores ++ cbi]]
       */

      private def isEmpty: Bool = !cbi.hasNext && ores.isEmpty
      private def head():  B    = if (ores.isEmpty) cbi.head else ores.head
      private def shift(): Unit = if (ores.isEmpty) cbi.next()
                                  else { ores = ores.tail }

      /** syncedWith is what really characterises the [[SIterator]].
       *  @param a is the item to synchronize items in [[cbi]] with.
       *  @return  items in [[cbi]] that can see [[a]]. 
       */

      def syncedWith(a: A): List[B] = {
        import scala.annotation.tailrec
        @tailrec def aux(zs: List[B]): List[B] = { 
          if (isEmpty) { zs }
          else {
            val b = head()
            (bf(b, a), cs(b, a)) match {
	      case (true,false)  => { shift(); aux(zs) } // Antimononicity (iii) 
	      case (false,false) => { zs }               // Antimononicity (iv)
	      case (_, true)     => { shift(); aux(b +: zs) }
            }
          }
        }
        if (oa == Some(a)) { ores } 
        else { oa = Some(a); ores = aux(List()).reverse; ores }
      }

      /** A hack to add a filter to screen synchronized items.
       *  Quite often, the canSee predicate that we want to use
       *  is not antimonotonic but can be decomposed as a conjunction
       *  of an antimonotonic predicate and a residual predicate.
       *  This residual predicate becomes the screening function.
       */

      def withFilter(screen: (B,A) => Bool): SIterator[A,B] = 
        SIterator.filter(this, screen)

      def filter(screen: (B,A) => Bool): SIterator[A,B] = withFilter(screen) 

    }  // End case class SIterator



    object SIterator {

      def apply[A,B](
        elems:    CBI[B], 
        isBefore: ISBEFORE[B,A],
        canSee:   CANSEE[B,A],
        screen:   (B,A) => Bool): SIterator[A,B] =
      {
        SIterator(elems, isBefore, canSee).withFilter(screen)
      }

  
      /** isBefore and canSee predicates are often defined not on whole
       *  entries, but on some indexing/key fields of them.  Provide 
       *  [[SIterator]] constructors for this purpose too. 
       */

      def apply[KA,KB,A,B](
        aget: A => KA,
        bget: B => KB,
        elems:    CBI[B],
        isBefore: ISBEFORE[KB,KA],
        canSee:   CANSEE[KB,KA]): SIterator[A,B] =
      {
        /** [[ek]] for synchronizing elems to [[aget(a)]] instead of [[a]].
         */
        val ek = {
          val bf: ISBEFORE[B,KA] = (b: B, ka: KA) => isBefore(bget(b), ka)
          val cs: CANSEE[B,KA]   = (b: B, ka: KA) => canSee(bget(b), ka)
          SIterator[KA,B](elems, bf, cs)
        }

        /** Override [[syncedWith(a)]] by [[ek.syncedWith(aget(a))]].
         *  In this manner, the [[SIterator]] becomes effectively
         *  defined by:
         *     cs(b, a) iff canSee(bget(b), aget(a))
         *     bf(b, a) iff isBefore(bget(b), aget(a))
         */
        new SIterator[A,B](ek.cbi, null, null) { 
          override def syncedWith(a: A): List[B] = ek.syncedWith(aget(a))
        }
      }


      def apply[KA,KB,A,B](
        aget:  A => KA,
        bget:  B => KB,
        elems:    CBI[B],
        isBefore: ISBEFORE[KB,KA],
        canSee:   CANSEE[KB,KA],
        screen:   (B,A) => Bool): SIterator[A,B] =
      {
        SIterator(aget, bget, elems, isBefore, canSee).withFilter(screen)
      }


      /** A hack to add a filter to screen synchronized items.  Quite often,
       *  the canSee predicate that is needed is not antimonotonic but can
       *  be decomposed as a conjunction of an antimonotonic predicate and
       *  a residual predicate. This residual predicate becomes a screening
       *  function for postprocessing synchronized items to select those
       *  meeting the requirement of the original canSee predicate.
       */

      def filter[A,B](
        si: SIterator[A,B], 
        screen: (B,A) => Bool): SIterator[A,B] = 
      {
        if (screen == null) { return si }
        
        new SIterator[A,B](si.cbi, null, null) {
          override def syncedWith(a: A): List[B] =
            si.syncedWith(a).filter(screen(_, a))
        }
      }

    }  // End object SIterator

  } // End object Synchrony




  object OrderedCollection {


  /** An ordered collection [[OColl]] is just a collection endowed with
   *  an explicit ordering; i.e. the collection is sorted according to
   *  an associated sorting [[Key]]. It is used mainly for producing a
   *  Synchrony iterator on the underlying collection.
   */


    import CloseableIterator._
    import Synchrony._


    case class Key[B,K](get: B => K, ord: Ord[K]) 
    {
      /** Synchrony iterator iterates on ordered collections.
       *  Class [[Key]] encapsulates ordering on collections.
       *  @param get is the indexing field which the collection is ordered by.
       *  @param ord is the ordering used. 
       */

      def reversed: Key[B,K] = Key(get, ord.reverse)
    }
  

    object Key {

      def asc[B,K](get: B => K)(implicit ord: Ord[K]): Key[B,K] =
        Key(get, ord)
  
      def dsc[B,K](get: B => K)(implicit ord: Ord[K]): Key[B,K] =
        Key(get, ord.reverse)
    }




    trait OColl[B,K] extends RESOURCE
    {
       SELF =>

      /** [[OColl]] represents ordered collection and, dually, iterator
       *  on ordered collection. We use [[OCOL]] and [[OCBI]] as aliases
       *  to keep track of these dual perspectives. The difference between
       *  the two perspectives is that the former is more permanent (e.g.,
       *  a file on disk) and the latter is more transient (e.g., a file
       *  handle), and have to be managed differently.
       */

      type OCBI[N] <: OColl[B,N]   

      type OCOL[N] = OCBI[N]

      protected def OCBI[N](es: IterableOnce[B], ky: Key[B,N]): OCBI[N]

      protected def OCOL[N](es: IterableOnce[B], ky: Key[B,N]): OCOL[N] =
        OCBI(es, ky)


      /** Additional parameters to be defined by inheriting instances:
       *
       *  @param key     is indexing key defining the order on this [[OCBI]]
       *
       *  @param elems   is the underlying collection or an iterator on
       *                 the underlying collection. If [[elems]] has
       *                 type [[CBI]], then this [[OColl]] is an [[OCBI]].
       */

      def key: Key[B,K]

      def elems: IterableOnce[B]

      def fileElems: CBI[B] = null   // A hook for [[OFile]]


      /** Initialization codes for [[OColl]] constructors to call.
       */

      protected def init = elems match { 
//      case null => DEBUG.message("*** OColl.init: null elems ***")
        case null => { }
        case ei: CBI[B] => use(ei)
        case _ => { }
      }
      

      def elemSeq: Seq[B] = elems match {
        case null            => fileElems.done { _.toSeq }
        case ei: Iterable[B] => ei.toSeq
        case ei: CBI[B]      => ei.done { _.toSeq }
        case ei: Iterator[B] => ei.toSeq
        case _               => Seq()
      }


      /** @return the n-th element in the collection.  This is not
       *  intended for normal use. It is mainly for testing/checking
       *  purpose.
       */

      def apply(n: Int): B = elems match {
        case null => 
          try fileElems.done { _.drop(n).next() }
          catch { 
            case _: Throwable => throw new NoSuchElementException("OColl.apply")
          }
        case ei: Seq[B] => ei(n)
        case ei: CBI[B] => ei.done { _.drop(n).next() }
        case ei: Iterable[B] => ei.drop(n).head
      }



      /** [[ocbi]] is constructor to produce an iterator on [[elems]].
       *  This iterator is associated with the same ordering key as
       *  [[elems]], so that operations requiring the ordering key can be
       *  performed in a self-sufficient way. This iterator also captures
       *  the dependency of the iterator on [[elems]] as a source. This
       *  makes it possible to delete [[elems]], if it is "unprotected",
       *  when the iterator is closed. This iterator correspond to the
       *  [[OCBI]] perspective of the collection.
       */

      def ocbi: OCBI[K] = OCBI[K](cbi, key).userev(this)
      

      /** [[useOnce]] is a consructor to produce an iterator on [[elems]].
       *  At the end of this iterator, this [[OColl]] is discarded.
       */

      def useOnce: OCBI[K] = (elems == null) match {
        case true  => OCBI[K](fileElems.userev(this), key).userev(this)
        case false => ocbi
      }
    

      /** [[cbi]] is an iterator on the underlying collection. It "forgets"
       *  the ordering information. But it still captures the dependency
       *  information that [[elems]] is its source, to facilitate auto-
       *  destruction of [[elems]] (if it is marked as "unprotected")
       *  when the iterator is closed.
       */

      def cbi: CBI[B] = elems match {
        case null       => fileElems
        case ei: CBI[B] => ei
        case _          => CBI(elems)
      }
  

      /** [[materialized]] converts this transient [[OCBI]] to a fully
       *  computed [[OColl]]. As such, the computation is done and
       *  resources held by this transient [[OCBI]] can be released.
       */

      def materialized: OCOL[K] = try OCOL(elemSeq, key) finally close()


      /** Methods for reordering the collection by some other sorting key
       */

      def assumeOrderedBy[N](ky: Key[B,N]): OCOL[N] = OCOL(elems, ky)


      def ordered: OCOL[K] = orderedBy[K](key)


      def orderedBy[N](ky: Key[B,N]): OCOL[N] =
        OCOL(elemSeq.sortBy(ky.get)(ky.ord), ky)


      def sortedBy[N](f: B => N)(implicit ord: Ord[N]): OCOL[N] = 
        orderedBy(Key.asc(f)(ord))


      def isOrdered: Bool = cbi.done { it =>
        if (!it.hasNext) { return true }
        var cur  = it.next()
        var flag = true
        while (flag && it.hasNext) {
          val nx = it.next()
          flag   = key.ord.lteq(key.get(cur), key.get(nx))
          cur    = nx
        }
        flag
      }


      def reversed: OCOL[K] = OCOL(elemSeq.reverse, key.reversed)


      /** Comprehension syntax, assuming order is preserved.
       *  Transient perspective, so that results are produced as iterator.
       */

      def filter(f: B => Bool): OCBI[K] = OCBI(cbi.filter(f), key)


      def withFilter(f: B => Bool): OCBI[K] = OCBI(cbi.filter(f), key)


      def map(f: B => B): OCBI[K] = OCBI(cbi.map(f), key)


      def flatMap(f: B => IterableOnce[B]): OCBI[K] = 
        OCBI(cbi.flatMap(f), key)


      def map[C](f: B => C): CBI[C] = cbi.map(f)


      def flatMap[C](f: B => IterableOnce[C]): CBI[C] = cbi.flatMap(f)


      def foreach[U](f: B => U): Unit =
        cbi.done { it => while (it.hasNext) { f(it.next()) } }


      def done[C](f: CBI[B] => C): C = Using.resource(this)(_.cbi.done(f))



      /** Method to merge this [[OCBI]] with other [[OColl]]. Assume 
       *  these [[OColl]] are sorted with the same key as this [[OCBI]].
       *  Transient perspective, so that results are produced as iterator.
       */

      def mergedWith(fs: OColl[B,K]*): OCBI[K] = {
        import scala.collection.mutable.PriorityQueue
        type BI = CBI[B]
        val cmp = key.ord.reverse.compare _
        val get = key.get
        val ord = new Ordering[BI] {
          override def compare(x: BI, y: BI) = cmp(get(x.head), get(y.head))
        }
        val cfs: Seq[BI] = this.cbi +: fs.map { _.cbi }
        val efs     = cfs filter { _.hasNext }
        val hpq     = PriorityQueue(efs: _*)(ord)
        def hasnx() = !hpq.isEmpty
        def nx(): B = {
          val ef = hpq.dequeue()
          val hd = ef.next()
          if (ef.hasNext) hpq.addOne(ef)
          hd
        }
        val cit  = CBI(() => hasnx(), () => nx())
        cit.use(this).use(fs: _*).use(cfs: _*)
        OCBI(cit, key)
      }



      /** Methods for grouping items in this [[OColl]] by their keys.
       *  Assume this [[OColl]] is sorted by its [[key]] already.
       *  Apply an aggregate function to each group.
       *  Transient perspective, so that results are produced as iterator.
       *
       *  @param e     is initial/zero value for the aggregate function.
       *  @param iter  defines the aggregate function; it is called whenever
       *               an item is added to the group being aggregated on.
       *  @param done  is function transforming the aggregation result.
       */

      def clustered: OSeq[(K,List[B]),K] = 
      {
        val e    = List[B]()
        val iter = (b: B, acc: List[B]) => b::acc  
        val done = (acc: List[B]) => acc.reverse
        clusteredFold(e, iter, done)
      }


      def clusteredFold[C](e: C, iter: (B,C) => C): OSeq[(K,C),K] =
      {
        val done = (x: C) => x
        clusteredFold(e, iter, done)
      }


      def clusteredFold[C,D](
        e:    C,  
        iter: (B,C) => C, 
        done: C => D): OSeq[(K,D),K] =
      {
        type KD = (K,D)
        val ky  = Key[KD,K](_._1, key.ord)
        val cit = clusteredFold[C,D,KD](e, iter, done, (k,d) => k->d)
        OSeq(cit, ky)
      }


      def clusteredFold[C,D,E](
        e:      C,  
        iter:   (B,C) => C, 
        done:   C => D,
        output: (K,D) => E): CBI[E] =
      {
        cbi.step { it =>
          var acc = e
          val kb  = key.get(it.head)
          while (it.hasNext && kb == key.get(it.head)) { 
            acc = iter(it.next(), acc)
          }
          output(kb, done(acc))
        }
      }



      /** @return the keys of this [[OCBI]].
       *  Transient perspective, so that results are produced as iterator.
       */

      def keys: OSeq[K,K] = {
        val ky     = Key((x: K) => x, key.ord)
        val e      = List[B]()
        val iter   = (b: B, acc: List[B]) => acc  
        val done   = (acc: List[B])       => acc
        val output = (k:K, _: List[B])    => k
        val cit    = clusteredFold(e, iter, done, output)
        OSeq(cit, ky)
      }


      /** [[OColl]]'s main purpose is to produce Synchrony iterator.
       *  @param ky  is the ordering assumed.
       *  @param cs  is the canSee predicate, assumed antimonotonic.
       *  @param ns  is some additional predicate for filtering results.
       *  @return an [[SIterator]] on this [[OCBI]].
       */

      def siterator[A](
        ky: Key[A,K],
        cs: CANSEE[K,K],
        ns: (B,A) => Bool = null): SIterator[A,B] =
      {
        if (ky.ord != key.ord) {
          // This [[OColl]] is sorted on a different key, must re-sort.
          DEBUG.message(s"** OColl.siterator: ky.ord != key.ord")
          val newkey = Key(key.get, ky.ord)
          orderedBy(newkey).siterator(ky, cs, ns)
        }
        else {
          // This [[OColl]] is in right order; can create Synchrony iterator.
          import ky.ord._
          val aget = ky.get
          val bget = key.get
          val bf   = (kb: K, ka: K) => kb < ka
          SIterator(aget, bget, cbi, bf, cs, ns)
        }
      }

    } // End trait OColl



    /** [[OSeq]] represents all in-memory ordered collections.
     */

    trait OSeq[B,K] extends OColl[B,K]
    {
      type OCBI[N] = OSeq[B,N]

      def OCBI[N](es: IterableOnce[B], ky: Key[B,N]): OCBI[N] =
        OSeq(es, ky)

      def elems: IterableOnce[B] 

      def key: Key[B,K]
    }


    object OSeq 
    {
      def apply[B,K](es: IterableOnce[B], ky: Key[B,K]): OSeq[B,K] =
        new OSeq[B,K] { val (elems, key) = (es, ky); init }


      /** Some additional constructors, so that programmers do not
       *  need to provide ordering explicitly.
       */

      def apply[B,K]
        (es: IterableOnce[B], 
         f:  B => K)
        (implicit ord: Ord[K]): OSeq[B,K] =
      {
        OSeq[B,K](es, Key.asc(f)(ord))
      }


      def apply[B](b: B*)(implicit ord: Ord[B]): OSeq[B,B] = {
        val ky: Key[B,B] = Key.asc[B,B](x => x)(ord)
        val sorted = b.sortBy(x => x)(ord)
        OSeq[B,B](sorted, ky)
      }
    }
      


    object implicits
    {
      import scala.language.implicitConversions

      implicit def OSeqToCBI[B,K](ocoll: OSeq[B,K]): CBI[B]  = ocoll.cbi

      implicit def OSeqFromIT[B](it: IterableOnce[B]): OSeq[B,Unit] = 
        OSeq[B,Unit](it, (x:B)=>())(Ordering[Unit])
    }

  } // End object OrderedCollection




  object Join {


    /** Fancy stuff so that programmers can write programs like this:
     *
     *  {{{
     *        for (
     *          (a, bs, cs, ds, es) <- A join B on CondAB
     *                                   join C using condAC
     *                                   join D on CondAD where condAD'
     *                                   join E on condAE where condAE'
     *            ... blah blah ...
     *        ) yield ... more blah ...
     *  }}}
     *
     *  Here, for each [[a]] in [[A]],
     *  [[bs]] is result of [[B.filter(b => CondAB(b,a))]],
     *  [[cs]] is result of [[C.filter(c => condAC(c,a))]], 
     *  [[ds]] is result of [[D.filter(d => CondAD(d,a) && condAD'(d,a))]], 
     *  [[es]] is result of [[E.filter(e => condAE(e,a) && condAE'(e,a))]]. 
     *
     *  Moreover, the complexity is O(|A| + k|B| + k|C| + k|D| + k|E|),
     *  where k is the selectivity of the (non-equi)join predicates 
     *  condAB, condAC, and condAD.
     * 
     *  This is much much better than the naive execution complexity of 
     *  O(|A| * (|B| + |C| + |D| + |E|)). Note that usual techniques for
     *  implementing equijoins via grouping (ala Wadler and Peyton-Jones) 
     *  and indexed table (ala Gibbons) don't work here, because these
     *  are non-equijoins.
     *
     *  The supported syntax has the forms below and can be repeated
     *  up to four times in the current implementation:
     *  {{{
     *        A join B on CondAB
     *        A join B on CondAB where g 
     *        A join B using condAB
     *        A join B using condAB where g 
     *  }}}
     *  [[CondAB]] has type [[Pred[K,K]*]] and can be a mix of 
     *  [[AntimonotonicPred[K]]] and [[GenPred[K,K]] predicates;
     *  [[condAB]] has type [[(K,K) => Bool]] and is antimonotonic;
     *  [[g]] has type [[(B,A) => Bool]];
     *  where [[K]] is the type of the
     *  indexing field of [[A]] and [[B]].
     *
     *  The [[A join B on CondAB]] and [[A join B on CondAB where g]]
     *  forms are probably more suitable for an occasional user who
     *  may be less carefully about the antimonotonicity requirement.
     *  These two forms force him to either explicitly label his
     *  predicates as [[AntimonotonicPred[K]] or as [[GenPred[K,K]].
     *  And it is even possible to hid the [[AntimonotonicPred[K]]
     *  and [[GenPred[K,K]] constructor, so that he is forced to use
     *  only system-defined predicates which are pre-labelled.
     *
     *  The [[A join B using condAB]] and [[A join B using condAB where g]]
     *  forms are probably more suited for an expert user who is careful
     *  to use only antimonotonic functions as [[condAB]].
     *
     *  The implementation is a bit long winded due to limitation of
     *  Scala's type system. The acrobatics needed here is akin to that
     *  for implementing zipN, where you need to implement zip2, zip3,
     *  zip4, etc. because the type system makes zipN for zipping arbitrary
     *  number of lists of arbitrarily different types not expressible.
     */
  
    import CloseableIterator._
    import Synchrony.SIterator
    import OrderedCollection.{ OColl, Key, OSeq }
    import Predicates._



    /** Join predicates must be antimonotonic wrt the ordering on the
     *  two tables being joined.
     */

    case class SynchronyJoinException(msg: String) extends Throwable

    private def checkAntimonotonic[K](
      o: Ord[K], 
      f: AntimonotonicPred[K]*): (K,K) => Bool =
    {
      if (!f.forall(_.ord == o)) { throw SynchronyJoinException("_.ord != o") }
      if (f.length == 0) { throw SynchronyJoinException("f is empty") }
      if (f.head.ord != o) { throw SynchronyJoinException("f.head.ord != o") }

      requires(f: _*)
    }


    private def liftGenPred[B,A,K](
      getb: B => K,
      geta: A => K,
      f: Pred[K,K]*): Option[(B,A) => Bool] =
    {
      def lift(pred: (K,K) => Bool) = (b: B, a: A) => {
        val kb = getb(b)
        val ka = geta(a)
        pred(kb, ka)
      }
        
      f.length match {
        case 0 => None
        case 1 => Some(lift(f.head.apply _))
        case _ => Some(lift(requires(f: _*)))
      } 
    }


    private def splitAntimonotonic[B,A,K](
      as:  OColl[A,K],
      bs:  OColl[B,K],
      pred: Pred[K,K]*) =
    {
      var anti: List[AntimonotonicPred[K]] = List()
      var genp: List[Pred[K,K]] = List()
      var it = pred
      while (!it.isEmpty) {
        it.head match {
          case ap: AntimonotonicPred[K] => anti = ap +: anti
          case gp: GenPred[K,K] => genp = gp +: genp
        }
        it = it.tail
      }

      val cs = checkAntimonotonic(as.key.ord, anti: _*) 
      val bi  = bs.siterator(as.key, cs)
      liftGenPred(bs.key.get, as.key.get, genp: _*) match {
        case None => bi
        case Some(gp) => bi.withFilter(gp)
      }
    }


    implicit class SI0[A,K](as: OColl[A,K]) {

      def join[B](bs: OColl[B,K]) = SI0Tmp[B](as, bs)

      case class SI0Tmp[B](as: OColl[A,K], bs: OColl[B,K]) 
      {
        def on(f: Pred[K,K]*): SI1[A,B,K] = { 
          val bi1 = splitAntimonotonic(as, bs, f: _*)
          new SI1(as, bi1)
        }

        def using(f: (K,K) => Bool): SI1[A,B,K] =
          new SI1(as, bs.siterator(as.key, f))
      }
    }


    implicit class SI1toSeq[A,B1,K](si: SI1[A,B1,K])
    extends OSeq[(A,List[B1]),K]
    {
      override val elems = {
        val it      = si.as.cbi
        def hasnx() = it.hasNext
        def nx()    = { 
          val a  = it.next()
          val b1 = si.bi1.syncedWith(a)
          (a, b1)
        } 
        CBI[(A,List[B1])](() => hasnx(), () => nx())
          .use(si.bi1, it)
      }

      override val key = {
        val get = si.as.key.get
        val ord = si.as.key.ord
        Key[(A,List[B1]),K](x => get(x._1), ord)
      }
    }


    case class SI1[A,B1,K](as: OColl[A,K], bi1: SIterator[A,B1])
    {
      def where(f: (B1,A) => Bool): SI1[A,B1,K] = 
        new SI1(as, bi1.withFilter(f))
      
//      def where(f: GenPred[B1,A]*): SI1[A,B1,K] = 
//        new SI1(as, bi1.withFilter(requires(f: _*)))
      

      def join[B2](bs2: OColl[B2,K]) = SI1Tmp(as, bi1, bs2) 

      case class SI1Tmp[B2](
        as:  OColl[A,K], 
        bi1: SIterator[A,B1], 
        bs2: OColl[B2,K]) 
      {
        def on(f: Pred[K,K]*): SI2[A,B1,B2,K] = { 
          val bi2 = splitAntimonotonic(as, bs2, f: _*)
          new SI2(as, bi1, bi2)
        }

        def using(f: (K,K) => Bool): SI2[A,B1,B2,K] = 
          new SI2(as, bi1, bs2.siterator(as.key, f))
      }
    }


    implicit class SI2toSeq[A,B1,B2,K](si: SI2[A,B1,B2,K])
    extends OSeq[(A,List[B1],List[B2]),K]
    {
      override val elems = {
        val it      = si.as.cbi
        def hasnx() = it.hasNext
        def nx()    = { 
          val a  = it.next()
          val b1 = si.bi1.syncedWith(a)
          val b2 = si.bi2.syncedWith(a)
          (a, b1, b2)
        }
        CBI[(A,List[B1],List[B2])](() => hasnx(), () => nx())
        .use(si.bi2, si.bi1, it)
      }
    
      override val key = { 
        val get = si.as.key.get
        val ord = si.as.key.ord 
        Key[(A,List[B1],List[B2]),K](x => get(x._1), ord) 
      }
    }


    case class SI2[A,B1,B2,K](
      as:  OColl[A,K], 
      bi1: SIterator[A,B1], 
      bi2: SIterator[A,B2])
    {
      def where(f: (B2,A) => Bool): SI2[A,B1,B2,K] = 
        new SI2(as, bi1, bi2.withFilter(f))
      
//      def where(f: GenPred[B2,A]*): SI2[A,B1,B2,K] =
//        new SI2(as, bi1, bi2.withFilter(requires(f: _*)))

      def join[B3](bs3: OColl[B3,K]) = SI2Tmp(as, bi1, bi2, bs3) 

      case class SI2Tmp[B3](
        as:  OColl[A,K], 
        bi1: SIterator[A,B1], 
        bi2: SIterator[A,B2],
        bs3: OColl[B3,K])
      {
        def on(f: Pred[K,K]*): SI3[A,B1,B2,B3,K] = { 
          val bi3 = splitAntimonotonic(as, bs3, f: _*)
          new SI3(as, bi1, bi2, bi3)
        }

      def using(f: (K,K) => Bool): SI3[A,B1,B2,B3,K] = 
        new SI3(as, bi1, bi2, bs3.siterator(as.key, f))
      }
    } 


    implicit class SI3toSeq[A,B1,B2,B3,K](si: SI3[A,B1,B2,B3,K])
    extends OSeq[(A,List[B1],List[B2],List[B3]),K]
    {
      val elems = {
        val it      = si.as.cbi
        def hasnx() = it.hasNext
        def nx()    = { 
          val a  = it.next()
          val b1 = si.bi1.syncedWith(a)
          val b2 = si.bi2.syncedWith(a)
          val b3 = si.bi3.syncedWith(a)
          (a, b1, b2, b3)
        }
        CBI[(A,List[B1],List[B2],List[B3])](() => hasnx(), () => nx())
        .use(si.bi3, si.bi2, si.bi1, it)
      }

      override val key = { 
        val get = si.as.key.get
        val ord = si.as.key.ord 
        Key[(A,List[B1],List[B2],List[B3]),K](x => get(x._1), ord)
      }
    }


    case class SI3[A,B1,B2,B3,K](
      as:  OColl[A,K],
      bi1: SIterator[A,B1], 
      bi2: SIterator[A,B2], 
      bi3: SIterator[A,B3])
    {
      def where(f: (B3,A) => Bool): SI3[A,B1,B2,B3,K] = 
        new SI3(as, bi1, bi2, bi3.withFilter(f))
      
//      def where(f: GenPred[B3,A]*): SI3[A,B1,B2,B3,K] =
//        new SI3(as, bi1, bi2, bi3.withFilter(requires(f: _*)))

      def join[B4](bs4: OColl[B4,K]) = SI3Tmp(as, bi1, bi2, bi3, bs4) 

      case class SI3Tmp[B4](
        as:  OColl[A,K], 
        bi1: SIterator[A,B1], 
        bi2: SIterator[A,B2], 
        bi3: SIterator[A,B3], 
        bs4: OColl[B4,K])
      {
        def on(f: Pred[K,K]*): SI4[A,B1,B2,B3,B4,K] = {
          val bi4 = splitAntimonotonic(as, bs4, f: _*)
          new SI4(as, bi1, bi2, bi3, bi4)
        }

      def using(f: (K,K) => Bool): SI4[A,B1,B2,B3,B4,K] = 
        new SI4(as, bi1, bi2, bi3, bs4.siterator(as.key, f))
      }
    } 


    case class SI4[A,B1,B2,B3,B4,K](
      as:  OColl[A,K],
      bi1: SIterator[A,B1], 
      bi2: SIterator[A,B2], 
      bi3: SIterator[A,B3], 
      bi4: SIterator[A,B4])
    extends OSeq[(A,List[B1],List[B2],List[B3],List[B4]),K]
    { 
      val elems = {
        val it      = as.cbi
        def hasnx() = it.hasNext
        def nx()    = { 
          val a  = it.next()
          val b1 = bi1.syncedWith(a)
          val b2 = bi2.syncedWith(a)
          val b3 = bi3.syncedWith(a)
          val b4 = bi4.syncedWith(a)
          (a, b1, b2, b3, b4)
        }
        CBI[(A,List[B1],List[B2],List[B3],List[B4])](() => hasnx(), () => nx())
        .use(bi4, bi3, bi2, bi1, it)
      }

      override val key = {
        val get = as.key.get
        val ord = as.key.ord
        Key[(A,List[B1],List[B2],List[B3],List[B4]),K](x => get(x._1), ord)
      } 
    }

  } // End object Join





  object Predicates {
  
    /** Predicates are used in joins.  They can be antimonotonic (i.e.
     *  suitable as "canSee" predicates) or non-antimonotonic (i.e. not
     *  suitable.)
     *
     *  Some example antimonotonic predicates that can be used as "canSee"
     *  predicates in Synchrony iterators are provided below.  It is quite
     *  easy to define predicates directly when using Synchrony iterators.
     *  But, here, we set up a framework for defining predicates in a more
     *  reuseable way.
     */


    sealed trait Pred[B,A] {
      def apply(b: B, a: A): Bool 
    }

    sealed case class GenPred[B,A](pred: (B,A) => Bool) extends Pred[B,A] {
      def apply(b: B, a: A): Bool = pred(b, a)
    }


    sealed case class AntimonotonicPred[K: Ordering](pred: (K,K) => Bool)
    extends Pred[K,K]
    {
      val ord: Ord[K]             = Ordering[K]
      def apply(b: K, a: K): Bool = pred(b,a)
    }
    

    /** Function to combine a list of predicates.
     */

    def requires[B,A](pred: Pred[B,A]*): (B,A) => Bool = pred.length match {
      case 0 => (b: B, a: A) => true
      case 1 => pred.head.apply _
      case _ => (b: B, a: A) => pred.forall(_(b, a))
    }


    /** Set up infrastructure for predicates that assume some underlying
     *  ordering on the objects.  In particular, [[Antimonotonic]]
     *  predicates require an underlying ordering.
     */
     
    trait HasOrder[K] {
      val ord: Ord[K]
      def equal(b: K, a: K) = b == a
      def lt(b: K, a: K) = ord.lt(b, a)
      def lteq(b: K, a: K) = ord.lteq(b, a)
      def gt(b: K, a: K) = ord.gt(b, a)
      def gteq(b: K, a: K) = ord.gteq(b, a)
    }

    def eq[K: HasOrder]: AntimonotonicPred[K] = {
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHO.equal _)(iHO.ord)
    }

    def lt[K: HasOrder]: AntimonotonicPred[K] = {
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHO.lt _)(iHO.ord)
    }

    def lteq[K: HasOrder]: AntimonotonicPred[K] = {
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHO.lteq _)(iHO.ord)
    }
  
    def gt[K: HasOrder]: GenPred[K,K] = {
      val iHO = implicitly[HasOrder[K]]
      new GenPred[K,K](iHO.gt _)
    }

    def gteq[K: HasOrder]: GenPred[K,K] = {
      val iHO = implicitly[HasOrder[K]]
      new GenPred[K,K](iHO.gteq _)
    }



    /** Set up infrastructure for "distance" predicates.
     */
  
    trait HasDistance[K] {
      def intdist(b: K, a: K): Int
      def dl(n: Int):   (K,K) => Bool = (b: K, a: K) => intdist(b, a) < n
      def dleq(n: Int): (K,K) => Bool = (b: K, a: K) => intdist(b, a) <= n
      def dg(n: Int):   (K,K) => Bool = (b: K, a: K) => intdist(b, a) > n
      def dgeq(n: Int): (K,K) => Bool = (b: K, a: K) => intdist(b, a) >= n
    }

    def dl[K: HasDistance: HasOrder](n: Int): AntimonotonicPred[K] = {
      val iHD = implicitly[HasDistance[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHD.dl(n))(iHO.ord)
    }

    def dleq[K: HasDistance: HasOrder](n: Int): AntimonotonicPred[K] = {
      val iHD = implicitly[HasDistance[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHD.dleq(n))(iHO.ord)
    }

    def dg[K: HasDistance](n: Int): GenPred[K,K] = {
      val iHD = implicitly[HasDistance[K]]
      new GenPred[K,K](iHD.dg(n))
    }

    def dgeq[K: HasDistance](n: Int): GenPred[K,K] = {
      val iHD = implicitly[HasDistance[K]]
      new GenPred[K,K](iHD.dgeq(n))
    }

  

    /** Set up infrastructure for "size" predicates.
     */

    trait HasSize[K] {
      def sz(a: K): Double
      def szpercent(n: Double): (K,K) => Bool = (b:K, a: K) => {
        val sza = sz(a)
        val szb = sz(b)
        (sza >= 0.0) && (szb >= 0.0) && 
        { if (sza > szb) { sza < szb * n } else { szb < sza * n } }
      }
    }

    def szpercent[K: HasSize: HasOrder](n: Double): AntimonotonicPred[K] = {
      val iHS = implicitly[HasSize[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHS.szpercent(n))(iHO.ord)
    }



    /** Set up infrastructure for "line topology" predicates.
     */

    trait LINETOPO[K] {
      def overlap(n: Int): (K,K) => Bool
      def near(n: Int):    (K,K) => Bool
      def far(n: Int):     (K,K) => Bool
      def inside:          (K,K) => Bool
      def enclose:         (K,K) => Bool
      def touch:           (K,K) => Bool
      def outside: (K,K) => Bool = far(1)
    }
  
    def overlap[K: LINETOPO: HasOrder](n: Int): AntimonotonicPred[K] = {
      val iOV = implicitly[LINETOPO[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iOV.overlap(n))(iHO.ord)
    }

    def near[K: LINETOPO: HasOrder](n: Int): AntimonotonicPred[K] = {
      val iOV = implicitly[LINETOPO[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iOV.near(n))(iHO.ord)
    }

    def far[K: LINETOPO](n: Int): GenPred[K,K] = {
      val iOV = implicitly[LINETOPO[K]]
      new GenPred[K,K](iOV.far(n))
    }

    def inside[K: LINETOPO]: GenPred[K,K] = {
      val iOV = implicitly[LINETOPO[K]]
      new GenPred[K,K](iOV.inside)
    }

    def enclose[K: LINETOPO]: GenPred[K,K] = {
      val iOV = implicitly[LINETOPO[K]]
      new GenPred[K,K](iOV.enclose)
    }

    def touch[K: LINETOPO]: GenPred[K,K] = {
      val iOV = implicitly[LINETOPO[K]]
      new GenPred[K,K](iOV.touch)
    }

    def outside[K: LINETOPO]: GenPred[K,K] = {
      val iOV = implicitly[LINETOPO[K]]
      new GenPred[K,K](iOV.outside)
    }


    /** The types and values below are needed to support adhoc 
     *  polymorphism over numeric types used in the predicates above.
     *  Scala's [[Numeric[K]]] has some problems because it causes,
     *  e.g., [[Int]] to be auto/implicitly converted to another
     *  Integer type.
     */

    trait DBNumeric[K] extends HasOrder[K] {
      val MinValue: K
      val MaxValue: K
      def plus(b: K, a: K): K
      def times(b: K, a: K): K
      def fromInt(n: Int): K
      def min(b: K, a: K): K = ord.min(b, a)
      def max(b: K, a: K): K = ord.max(b, a)
    }


   

    /** As examples, instantiate predicates on [[Int]] and [[(Int,Int)]]
     */

    trait DSTRING extends HasOrder[String] with HasSize[String]

    implicit val dString: DSTRING = new DSTRING {
      val ord = Ordering[String]
      def sz(n: String) = n.length
    }


    trait DINT extends HasOrder[Int] with HasDistance[Int]
                                     with HasSize[Int] 
                                     with DBNumeric[Int]

    implicit val dInt = new DINT {
      def intdist(b: Int, a: Int) = (b - a).abs
      def sz(n: Int) = n.toDouble 
      val ord        = Ordering[Int]
      val MinValue   = Int.MinValue
      val MaxValue   = Int.MaxValue
      def fromInt(n:Int) = n
      def plus(b: Int, a: Int) = b + a
      def times(b: Int, a: Int) = b * a
    }


    trait DDBL extends HasOrder[Double] with HasDistance[Double]
                                        with HasSize[Double] 
                                        with DBNumeric[Double]

    implicit val dDbl = new DDBL {
      def intdist(b: Double, a: Double) = (b - a).abs.toInt
      def sz(n: Double) = n 
      val ord        = Ordering[Double]
      val MinValue   = Double.MinValue
      val MaxValue   = Double.MaxValue
      def fromInt(n:Int) = n.toDouble
      def plus(b: Double, a: Double) = b + a
      def times(b: Double, a: Double) = b * a
    }


    trait DINT2 extends HasOrder[(Int,Int)] with HasDistance[(Int,Int)]
                                            with HasSize[(Int,Int)] 
                                            with LINETOPO[(Int,Int)]

    implicit val dInt2 = new DINT2 {
      type Int2 = (Int,Int)
  
      val ord = Ordering[(Int,Int)]
  
      def sz(a: Int2) = {
        val (a1, a2) = a  // Assume a1 < a2
        (a2 - a1).toDouble
      }

      def intdist(b: Int2, a: Int2) = {
        val (b1, b2) = b  // Assume b1 < b2
        val (a1, a2) = a  // Assume a1 < a2
        if (a2 < b1) { b1 - a2 }
        else if (b2 < a1) { a1 - b2 }
        else 0
      }

      def overlap(n: Int) = (b: Int2, a: Int2) => {
        val (b1, b2) = b  // Assume b1 < b2
        val (a1, a2) = a  // Assume a1 < a2
        (a1 <= b2 && b1 <= a2) &&
        ((a2 min b2) - (a1 max b1) >= n) 
      }

      def near(n: Int) = dleq(n)

      def far(n: Int) = dgeq(n)

      def inside = (b: Int2, a: Int2) => {
        val (b1, b2) = b  // Assume b1 < b2
        val (a1, a2) = a  // Assume a1 < a2
        a1 <= b1 && b2 <= a2
      }
      
      def enclose = (b: Int2, a: Int2) => {
        val (b1, b2) = b  // Assume b1 < b2
        val (a1, a2) = a  // Assume a1 < a2
        b1 <= a1 && a2 <= b2
      }
      
      def touch = (b: Int2, a: Int2) => {
        val (b1, b2) = b  // Assume b1 < b2
        val (a1, a2) = a  // Assume a1 < a2
        (a1 == b2) || (b1 == a2)
      }
    }

  } // End object Predicates




  object OpG 
  {
    import Predicates._
    import CloseableIterator.CBI
    import OrderedCollection.OColl

    /** A number of aggregate functions are often needed in database
     *  queries. Some of these are provided below for illustrations.
     *  Also, some combinators [[combine]] are provided for combining
     *  aggregate functions so that multiple aggregate functions can
     *  be computed in a single pass through a collection.
     */
  
    
    /** [[Aggr(e, iter, done)]] represents an aggregate function
     * equivalent to [[done o foldLeft(e, iter)]]. This "trampolin"
     * form exposes the key component defining the aggregate function.
     * This makes it possible to later apply combinators to combine
     * multiple aggregate functions in a step-wise manner.
     */

    case class Aggr[B,C,+D](e: C, iter: (B,C) => C, done: C => D)
    {
      def apply(bs: IterableOnce[B]): D = {
        var acc: C = e
        val it = bs.iterator
        while (it.hasNext) { acc = iter(it.next(), acc) }
        done(acc)
      }

      def byPartition[K,E](bi: OColl[B,K], output: (K,D) => E): CBI[E] =
        bi.clusteredFold(e, iter, done, output)
    }


    /** In [[Aggr[B,C,D]] above, the [[C]] is the type of the intermediate
     *  data in computing an aggregate function. Users mainly want to know
     *  the input type [[B]] and output type [[D]]. So, we use a simple
     *  typecasting trick below to hide this [[C]].
     *
     *  Instead of constructing [[Aggr(e, iter, done)]], construct the
     *  aggregate function using [[AGGR(e, iter, done)]] which typecasts
     *  away the intermediate type [[C]].
     */

    type AGGR[B,+D]   = Aggr[B,Any,D]

    def AGGR[B,C,D](e:C, iter: (B,C)=>C, done: C=>D): AGGR[B,D] =
      Aggr(e, iter, done).asInstanceOf[AGGR[B,D]]



    /** Combinators to combine aggregate functions.
     */

    def combine[B,D1,D2,D]
      (f: AGGR[B,D1], g: AGGR[B,D2])
      (h: (D1,D2) => D): AGGR[B,D] =
    {
      val e        = (f.e, g.e)
      val (fi, gi) = (f.iter, g.iter)
      val (fd, gd) = (f.done, g.done)
      def iter(b: B, a: (Any,Any)) = (fi(b, a._1), gi(b, a._2))
      def done(a: (Any,Any)) = h(fd(a._1), gd(a._2))
      AGGR(e, iter, done)
    }


    def combine[B,D](aggrs: (String, AGGR[B,D])*): AGGR[B,Map[String,D]] =
    {
      val as  = aggrs.view.map { case (f, aggr) => f -> aggr.e }.toMap
      val gs  = aggrs.view.map { case (f, aggr) => f -> aggr.iter }.toMap
      val ds  = aggrs.view.map { case (f, aggr) => f -> aggr.done }.toMap
      val iter = (b: B, as: Map[String,Any]) => 
           as.map { case (f,a) => f -> gs(f)(b, a) }  
      val done = (as: Map[String,Any]) =>
         as.map { case (f,a) => f -> ds(f)(a) }
      AGGR(as, iter, done)
    }


    /** Example aggregate functions
     */

    def SMALLEST[B,C: DBNumeric](f: B => C): AGGR[B,C] = {
      val numeric = implicitly[DBNumeric[C]] 
      AGGR[B,C,C](e = numeric.MaxValue,   
               iter = (b, a) => numeric.min(f(b), a),
               done = c => c)
    }
    
    def smallest[B,C: DBNumeric](f: B => C)(bs: IterableOnce[B]): C =
      SMALLEST(f).apply(bs)

 
    def BIGGEST[B,C: DBNumeric](f: B => C): AGGR[B,C] = {
      val numeric = implicitly[DBNumeric[C]]
      AGGR[B,C,C](e = numeric.MinValue,   
               iter = (b, a) => numeric.max(f(b), a),
               done = c => c)
    }
    
    def biggest[B,C: DBNumeric](f: B => C)(bs: IterableOnce[B]): C =
      BIGGEST(f).apply(bs)


    def COUNT[B]: AGGR[B,Int] = 
      AGGR[B,Int,Int](e = 0, 
                   iter = (_, a) => a + 1, 
                   done = c => c)
  
    def count[B](bs: IterableOnce[B]): Int = COUNT.apply(bs)


    def SUM[B, C: DBNumeric](f: B => C): AGGR[B,C] = {
      val numeric = implicitly[DBNumeric[C]]
      AGGR[B,C,C](e = numeric.fromInt(0),
               iter = (b, a) => numeric.plus(f(b), a),
               done = c => c)
    }

    def sum[B, C: DBNumeric](f: B => C)(bs: IterableOnce[B]): C = 
      SUM(f).apply(bs)


    def PROD[B, C:DBNumeric](f: B => C): AGGR[B,C] = {
      val numeric = implicitly[DBNumeric[C]]
      AGGR[B,C,C](e = numeric.fromInt(1),
               iter = (b, a) => numeric.times(f(b), a),
               done = c => c)
    }
   
    def prod[B, C: DBNumeric](f: B => C)(bs: IterableOnce[B]): C = 
      PROD(f).apply(bs)


    def AVERAGE[B](f: B => Double): AGGR[B,Double] =
      combine(COUNT[B], SUM(f)) { (c, s) => if (c != 0) s/c else 0.0 }
    
    def average[B](f: B => Double)(bs: IterableOnce[B]): Double =
      AVERAGE(f).apply(bs)


    def MINIMIZE[B,C: DBNumeric](f: B => C): AGGR[B,(C,List[B])] = {
      val numeric = implicitly[DBNumeric[C]]
      val ord = numeric.ord
      val e   = (numeric.MaxValue, List[B]())
      val iter = (b: B, a: (C, List[B])) => {
        val cur = f(b)
        val min = a._1
        val acc = a._2
        ord.compare(min, cur) match {
          case -1 => a
          case  0 => (min, b +: acc)
          case  _ => (cur, List(b))
        }
      }
      val done = (min: C, acc: List[B]) => (min, acc.reverse) 
      AGGR[B,(C,List[B]),(C,List[B])](e, iter, done.tupled)
    }

    def minimize[B,C: DBNumeric](f: B => C)(bs: IterableOnce[B]): (C,List[B]) =
      MINIMIZE(f).apply(bs)


    def MAXIMIZE[B,C: DBNumeric](f: B => C): AGGR[B,(C,List[B])] = {
      val numeric = implicitly[DBNumeric[C]]
      val ord = numeric.ord
      val e   = (numeric.MinValue, List[B]())
      val iter = (b: B, a: (C, List[B])) => {
        val cur = f(b)
        val max = a._1
        val acc = a._2
        ord.compare(cur, max) match {
          case -1 => a
          case  0 => (max, b +: acc)
          case  _ => (cur, List(b))
        }
      }
      val done = (max: C, acc: List[B]) => (max, acc.reverse)
      AGGR[B,(C,List[B]),(C,List[B])](e, iter, done.tupled)
    }

    def maximize[B,C: DBNumeric](f: B => C)(bs: IterableOnce[B]): (C,List[B]) =
      MAXIMIZE(f).apply(bs)


    def STATS[B](f: B=>Double): AGGR[B,Map[String,Double]] =
    {
      def sq(n: Double) = n * n

      val aggrs = combine[B,Double](
                    "count" -> SUM[B,Double](b => 1), 
                    "sum"   -> SUM[B,Double](f),
                    "sumsq" -> SUM[B,Double](b => sq(f(b))), 
                    "min"   -> SMALLEST[B,Double](f), 
                    "max"   -> BIGGEST[B,Double](f))
  
      val Some((e, iter, done)) = Aggr.unapply(aggrs)

      def welldone(any: Any) = {
        val numbers = any.asInstanceOf[Map[String,Double]]
        val cnt = numbers("count")
        val sum = numbers("sum")
        val ssq = numbers("sumsq")
        val min = numbers("min")
        val max = numbers("max")
        val ave = if (cnt != 0) sum/cnt else 0
        val vre = if (cnt>1) (ssq + (cnt*ave*ave) - (2*sum*ave))/(cnt-1) else 0
        numbers ++ Map("average" -> ave, "variance" -> vre)
      }

      AGGR(e, iter, welldone _)
    }

    def stats[B](f: B => Double)(bs: IterableOnce[B]): Map[String,Double] = 
      STATS(f).apply(bs)

  }  // End object OpG

} // End object DBModel







/** Examples ***********************************************

{{{

import dbmodel.DBModel.{ OrderedCollection, Join, Predicates }
import OrderedCollection._
import OrderedCollection.implicits._
import Join._
import Predicates._


//==============================================
// Simple 1-D test
//==============================================


// aa is a sorted table.

val aa = OSeq(1,2,3,4,5,6,7,7,8,8,9)


// for each a <- aa, 
// return items b in aa s.t. 1 < (b - a) < 3.

for ((a, bs) <- aa join aa on dl(3) where { (b,a) => (b-a) > 1 } ) 
{
  println((a, bs))
}

// Output should be: 

(1,List(3))
(2,List(4))
(3,List(5))
(4,List(6))
(5,List(7, 7))
(6,List(8, 8))
(7,List(9))
(7,List(9))
(8,List())
(8,List())
(9,List())

// End output


// for each a <- aa,
// return items b in aa s.t. abs(b - a) < 3.
// return also a count of such b's.


for ((a, bs, cs) <- aa join aa on dl(3)
                       join aa on dl(3)) 
{
  println(a, bs, cs.length)
}

// Output should be:

(1,List(1, 2, 3),3)
(2,List(1, 2, 3, 4),4)
(3,List(1, 2, 3, 4, 5),5)
(4,List(2, 3, 4, 5, 6),5)
(5,List(3, 4, 5, 6, 7, 7),6)
(6,List(4, 5, 6, 7, 7, 8, 8),7)
(7,List(5, 6, 7, 7, 8, 8, 9),7)
(7,List(5, 6, 7, 7, 8, 8, 9),7)
(8,List(6, 7, 7, 8, 8, 9),6)
(8,List(6, 7, 7, 8, 8, 9),6)
(9,List(7, 7, 8, 8, 9),5)

// End output


// for each a <- aa, 
// return items b in aa s.t. 0 <= (b - a) < 3.
// return also a count of such b's.

for ((a, bs, cs) <- aa join aa on (dl(3), gteq)
                       join aa on (dl(3), gteq))
{
  println(a, bs, cs.length)
}

// Output should be:

(1,List(1, 2, 3),3)
(2,List(2, 3, 4),3)
(3,List(3, 4, 5),3)
(4,List(4, 5, 6),3)
(5,List(5, 6, 7, 7),4)
(6,List(6, 7, 7, 8, 8),5)
(7,List(7, 7, 8, 8, 9),5)
(7,List(7, 7, 8, 8, 9),5)
(8,List(8, 8, 9),3)
(8,List(8, 8, 9),3)
(9,List(9),1)

// End output




//==============================================
// Simple 2-D test. Non-equijoin alert!
//==============================================


// xx and yy are tables of intervals/line segments.
// Line segments are ordered by (start, end).

case class Ev(start: Int, end: Int, id: String)

val ky = Key.asc((x: Ev) => (x.start, x.end))

val xx = OSeq(Seq(Ev(60,90,"d")), ky)

val yy = OSeq(Seq(Ev(10,70,"a"), Ev(20,30,"b"), Ev(40,80,"c")), ky)


// Show overlapping line segment in xx and yy.

for ((x, ys) <- xx join yy on overlap(1)) 
{
  println(x, ys)
}

// Output should be:

(Ev(60,90,d),List(Ev(10,70,a), Ev(40,80,c)))

// End output
 


//==================================================
// Let's do some relational DB queries...
//==================================================


case class Phone(model: String, price: Int, brand: String)

val s21    = Phone("S21", 1000, "Samsung")
val a52    = Phone("A52", 550, "Samsung")
val a32    = Phone("A32", 350, "Samsung")
val n10    = Phone("N10", 360, "OnePlus")
val a94    = Phone("A94", 400, "Oppo")
val m40    = Phone("Mate 40", 1200, "Huawei")
val pix5   = Phone("Pixel 5", 1300, "Google")
val pix4   = Phone("Pixel 4", 500, "Google")
val phones = Vector(s21, a52, a32, n10,a94, m40, pix5, pix4)

val kPrice = Key.asc[Phone,Int](x => x.price)
val kBrand = Key.asc[Phone,String](x => x.brand)

// Phones, sorted by brands

val OByBrand = phones orderedBy kBrand


// Phones grouped by brands

for ((b, ps) <- OByBrand.elemSeq.groupBy(_.brand)) 
{
  println(s"brand = ${b}\n  ${ps}\n")
}

// Output should be something like below.
// Note that it is not ordered:

brand = Oppo
  Vector(Phone(A94,400,Oppo))

brand = Huawei
  Vector(Phone(Mate 40,1200,Huawei))

brand = Google
  Vector(Phone(Pixel 5,1300,Google), Phone(Pixel 4,500,Google))

brand = OnePlus
  Vector(Phone(N10,360,OnePlus))

brand = Samsung
  Vector(Phone(S21,1000,Samsung), Phone(A52,550,Samsung), Phone(A32,350,Samsung))

// End output




// Phones, re-sorted by price 

val OByPrice = OByBrand orderedBy kPrice


// Phones and their price competitors from
// other brands within +/- $150.
// Non-equijoin alert!

for ((p, cs) <- OByPrice join OByPrice on dleq(150);
     ob = cs.filter(_.brand != p.brand)) 
{ 
  println(s"${p}\n  ${ob}\n") 
}

// Output should look like:

Phone(A32,350,Samsung)
  List(Phone(N10,360,OnePlus), Phone(A94,400,Oppo), Phone(Pixel 4,500,Google))

Phone(N10,360,OnePlus)
  List(Phone(A32,350,Samsung), Phone(A94,400,Oppo), Phone(Pixel 4,500,Google))

Phone(A94,400,Oppo)
  List(Phone(A32,350,Samsung), Phone(N10,360,OnePlus), Phone(Pixel 4,500,Google), Phone(A52,550,Samsung))

Phone(Pixel 4,500,Google)
  List(Phone(A32,350,Samsung), Phone(N10,360,OnePlus), Phone(A94,400,Oppo), Phone(A52,550,Samsung))

Phone(A52,550,Samsung)
  List(Phone(A94,400,Oppo), Phone(Pixel 4,500,Google))

Phone(S21,1000,Samsung)
  List()

Phone(Mate 40,1200,Huawei)
  List(Phone(Pixel 5,1300,Google))

Phone(Pixel 5,1300,Google)
  List(Phone(Mate 40,1200,Huawei))

// End output



// Samsung phones and their price competitors 
// from Google, within 20%.
// Non-equijoin alert!

for ((p, cs) <- OByPrice.filter(_.brand == "Samsung")
           join OByPrice.filter(_.brand == "Google")
           on szpercent(1.2))
{ 
  println(s"${p}\n  ${cs}\n") 
}

// Output should be:

Phone(A32,350,Samsung)
  List()

Phone(A52,550,Samsung)
  List(Phone(Pixel 4,500,Google))

Phone(S21,1000,Samsung)
  List()

//  End output



//======================================================
// Timing study to compare the standard groupBy with
// sortedBy followed by clustered, which is our way of 
// implementing groupby using Synchrony iterator. 
//======================================================


// [[bm]] runs [[f]] as many times as possible
// within [[duration]] number of milliseconds.

def bm(duration: Long)(f: => Unit) =
{
  val end = System.currentTimeMillis + duration
  var count = 0
  while(System.currentTimeMillis < end) { f; count += 1 }
  count
}


// Standard groupBy

def test1 = (for ((b,ps) <- OByBrand.elemSeq.groupBy(_.brand))
             yield ps.length
            ).max


// Synchrony-based implementation of groupby.


def test3 = (for ((b,ps) <- OByBrand.clustered) yield ps.length).max


// Timing test.

bm(500)(test1)  // standard groupBy
bm(500)(test3)  // clustered


}}}

*************************************************************/


