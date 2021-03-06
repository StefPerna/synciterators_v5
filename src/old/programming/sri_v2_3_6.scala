
package synchrony.programming  

/** Structural recursion combinators.
 *
 *  Structural recursion combinators are great for expressing all 
 *  kinds of aggregate functions and iterations on collections.
 *  Basically turn structural recursion on "insert" into a data
 *  structure, so that such a recursion now (1) runs on the heap 
 *  instead of on the stack; and (2) is easier to manipulate, as
 *  the recursion is now a data structure rather than a function.
 *
 * Wong Limsoon
 * 28 September 2020
 */




object Sri {

  // import scala.collection.mutable.Map
  import scala.language.existentials
  import scala.reflect.ClassTag


  /** Represent structural recursion on "insert".
   *
   *  A structural recursion is intended to behave like this:
   *
   *   Sri(zero, comb) xs = {
   *     var acc = zero
   *     while (xs.hasNext) { acc = comb(acc, xs.next()) }
   *     return acc
   *   }
   *
   *
   *  It is realised as a data structure Sri3 below, instead of as
   *  a function directly. This brings two virtues. The first is 
   *  that Sri now "executes" on the heap instead of on the stack.
   *  The second, and more impt one, is that we can device many
   *  interesting combinators to manipulate the Sri3 structure;
   *  see some of these below (viz. compose, combine, combineN,
   *  withSlidingN, and withAcc), as well as many more in the 
   *  synchrony.iterators.AggrCollections module.
   */

  sealed trait Sri[C,F] { 


  /** Apply this Sri to an input iterator
   *
   *  @param it is the input iterator.
   *  @return result of applying this Sri on it.
   */

    def apply(it: Iterator[C]): F = run(this)(it)


  /** Compose a function with this Sri.
   *
   *  @param f is the function to be composed with this Sri.
   *  @return f o Sri(zero, comb) 
   */
 
    def |>[G](f: F => G): Sri[C, G] = compose(this)(f)


  /** Combine this Sri with another Sri by pairing up the
   *  iteration steps and results.
   *
   *  @param f is the Sri to be paired to this Sri.
   *  @return a new Sri that is the pairing of f's steps with
   *          the steps of this Sri, pairing also the results.
   */

    def +[G](f: Sri[C, G]): Sri[C, (F, G)] = 
      combine(this, f){ case (x, y) => (x, y) }



  /** Combine this Sri with another Sri, pairing their steps,
   *  mixing their results.
   *
   *  @param f is the Sri to be paired to this Sri.
   *  @param mx is the function for mixing the results of f and this Sri.
   *  @return a new Sri that is the pairing of f's steps with
   *          the steps of this Sri, mixing the results.
   */

    def *[G, H](f: Sri[C, G])(mx: (F, G) => H): Sri[C, H] = 
      combine(this, f)(mx)

  } /** End of trait Sri. */



  /** Constructor for Sri.
   *
   *  Sri3 is a more flexible form of Sri that includes some preprocessing
   *  and postprocessing to make it more convenient to 
   *  combine multiple Sri structures. 
   *
   *  @param zero defines the zero elem for Sri.
   *  @param iter defines the pre-processing function.
   *  @param comb defines the iteration step for Sri.
   *  @param done defines the post-processing function.
   *  @return done o Sri(zero(), comb) o map(iter). That is,
   *          pre-process an input iterator using iter,
   *          apply Sri(zero(), comb) to the pre-processing result,
   *          post-process the result using done;
   *          return this result. 
   *
   *  The intention is that:
   *
   *    Sri3(zero, iter, comb, done)(it) =
   *      done (Sri (zero(), comb) (for (i <- it) yield iter(i)))
   */

  final case class Sri3[C, D, E, F](
    zero: () => E,
    iter: C => D,
    comb: (E, D) => E,
    done: E => F
  ) extends Sri[C, F] 



  /** Combinator to construct a Sri3 when post-processing is not needed.
   *
   *  Often no post-processing is needed fro Sri3. Just set
   *  the done field of Sri3 to identity function (x => x).
   */

  def sri[C, D, F](zero: () => F, iter: C => D, comb: (F, D) => F): Sri[C, F] =
    Sri3(zero = zero, iter = iter, comb = comb, done = (x: F) => x)
  


  /** Combinator to construct a Sri3 that iterates on sliding-window basis.
   *
   *  @param n is size of sliding window.
   *  @param zero defines the zero elem for Sri.
   *  @param iter defines the pre-processing function.
   *  @param comb defines the steps for Sri.
   *  @return result of applying Sri(zero(), iter, comb) in the manner of 
   *          a sliding-window, which is a vector on the previous n items
   *          plus the current item. All items are pre-processed by iter. 
   */

  def sriWithSlidingN[C, D, F]
    (n:Int)
    (zero:() => F, iter: C => D, comb: (Vector[D], F, D) => F): Sri[C, F] = {

    Sri3[C, D, (Vector[D],F), F]( 
      zero = () => (Vector[D](), zero()),
      iter = iter,
      comb = { case ((u,v), y) => 
                 if (u.length >= n) (u.drop(1) :+ y, comb(u, v, y))
                 else (u :+ y, zero())
      },
      done = { case (u,v) => v }
    )
  }



  /** Combinator to compose a function with a Sri.
   *
   *  @param sri is an Sri.
   *  @param g is a function to be composed with sri.
   *  @return g o sri
   */

  def compose[C, F, G](sri: Sri[C, F])(g: F => G): Sri[C, G] = sri match {
    case Sri3(z, i, c, d) =>
      Sri3[C, D forSome{type D}, E forSome{type E}, G](z, i, c, x => g(d(x)))
  }



  /** Combinator to pair-up the iterations of two Sri.
   *
   *  @param sri1
   *  @param sri2  are the two Sri to be paired.
   *  @param mix defines how to combine the result.
   *  @return an Sri(it) that is equivalent to mix(sri1(it), sri2(it)).
   *          However, the steps of sri1 and sri2 are paired up so 
   *          that the input iterator it is read only once.
   *          It is crucial that the input is read only once,
   *          sinc the input (it) is an iterator.
   */
 
  def combine[C, F1, F2, F]
    (sri1: Sri[C, F1], sri2: Sri[C, F2])
    (mix: (F1, F2) => F)
  : Sri[C, F] =  {

    sri1 match { case Sri3(z1, i1, c1, d1) =>
    sri2 match { case Sri3(z2, i2, c2, d2) =>
      Sri3[C, 
           (D1 forSome{type D1}, D2 forSome{type D2}),
           (E1 forSome{type E1}, E2 forSome{type E2}),
           F](
        () => (z1(), z2()),
        y => (i1(y), i2(y)),
        { case ((a1, a2), (y1,y2)) => (c1(a1,y1), c2(a2, y2)) },
        { case (a1,a2) => mix(d1(a1), d2(a2)) }
      )
    } }
  }


  def combineM[C, S, D]
    (sri1: Sri[C, Map[S, D]],
     sri2: Sri[C, Map[S, D]])
  : Sri[C, Map[S, D]] = {
    sri1 match { case Sri3(z1, i1, c1, d1) =>
    sri2 match { case Sri3(z2, i2, c2, d2) =>
      Sri3[C,
           (D1 forSome{type D1}, D2 forSome{type D2}),
           (E1 forSome{type E1}, E2 forSome{type E2}),
           Map[S, D]](
        () => (z1(), z2()),
        y => (i1(y), i2(y)),
        { case ((a1, a2), (y1,y2)) => (c1(a1,y1), c2(a2, y2)) },
        { case (a1, a2) => d1(a1) ++ d2(a2) }
       )
    } } 
  }



  /** Combinator to combine multiple Sri. Their steps are combined
   * and run in synchrony so that the input is read only once.
   *
   *  @param sri the Sri to be combined.
   *  @param mix defines how the results are to be combined.
   *  @return an Sri(it) equivalent to mix(sri1(it), ..., srik(it)).
   *          However, the steps of sri1, ..., srik are combined
   *          so that the input iterator is traversed only once.
   */

  def combineN[C, F: ClassTag, G]
    (sri: Sri[C, F]*)
    (mix: Array[F] => G)
  : Sri[C, G] = {

    val gs = for (s <- Array[Sri[C, F]](sri:_*)) yield {
               s match { case Sri3(z, i, c, d) => (z, i, c, d) }
             }
    val zs = for ((z, i, c, d) <- gs) yield z
    val is = for ((z, i, c, d) <- gs) yield i
    val cs = for ((z, i, c, d) <- gs) yield c
    val ds = for ((z, i, c, d) <- gs) yield d

    Sri3[C, Array[D] forSome{type D}, Array[E] forSome{type E}, G](
      ()            => for (z <- zs) yield z(),
      y             => for (i <- is) yield i(y),
      {case (es, y) => for ((c, x) <- cs.zipWithIndex) yield c(es(x), y(x)) },
      es => { mix(for ((d, x) <- ds.zipWithIndex) yield d(es(x))) }
    )
  }



  /** Combinator to make an Sri return also its input.
   *
   * @param aggr is the Sri
   * @return an Sri(it) equivalent to (aggr(it), it.toVector)
   */

  def withAcc[C, F](aggr: Sri[C, F]): Sri[C, (F, Vector[C])] =
    withAccf[C, C, F](x => x)(aggr)


  /** Combinator to make an Sri return also its input after a transformation.
   *
   * @param f defines the transformation on the input to be returned.
   * @param aggr is the Sri
   * @return an Sri(it) equivalent to (aggr(it), f(it.toVector))
   */

  def withAccf[C, E, F]
    (f: C => E)
    (aggr: Sri[C, F])
  : Sri[C, (F, Vector[E])] = aggr match {

    case Sri3(zero, iter, comb, done) =>
      Sri3[C,
           (U forSome{type U}, E),
           (V forSome{type V}, Vector[E]), 
           (F, Vector[E])](
        () => (zero(), Vector[E]()),
        x => (iter(x), f(x)),
        { case (x,y) => (comb(x._1, y._1), x._2 :+ y._2) },
        { case (x,y) => (done(x), y) }
      )
  }



  /** Combinator to lift an aggregate function to a sliding window of size N+1.
   *
   *  @param n defines size of the sliding window, which is the n previous
   *           items plus the current item.
   *  @param aggr is the aggregate function, which is a Sri.
   */

  def withSlidingN[C, F](n:Int)(aggr: Sri[(Vector[C], C), F]): Sri[C, F] = {

    val (zero, iter, comb, done) = aggr match {
      case Sri3(z, i, c, d) => (z, i, c, d)
    }

    val sliding = sriWithSlidingN[C, C, Any](n)(
          zero,
          y => y, 
          { case(u, v, y) =>  comb(v, iter(u, y)) }
    )

    compose(sliding)(done)
  }


  
  /** Run a Sri on an iterator.
   */

  def apply[C, F](sri: Sri[C, F])(it: Iterator[C]): F = run(sri)(it)


  def run[C, F](sri: Sri[C, F])(it: Iterator[C]): F = sri match { 
    case Sri3(zero, iter, comb, done) => 
      var acc = zero()
      for (c <- it) { acc = comb(acc, iter(c)) }
      return done(acc)
  }

} // End of object Sri.




