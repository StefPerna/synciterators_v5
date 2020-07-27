
package synchrony
package programming  

//
// Wong Limsoon
// 8/1/2020
//
// Structural recursion combinators. They are great
// for expressing all kinds of aggregate functions
// and iterations on collections. Basically turn
// structural recursion on "insert" into a data
// structure, so that such a recursion now runs on
// the heap instead of on the stack, and make it
// easier to manipulate.
// 



object Sri {

  import scala.collection.mutable.Map
  import scala.language.existentials
  import scala.reflect.runtime.universe.{TypeTag, typeOf}  // DEBUG


  sealed trait Sri[C,F]
  { //
    // The trait Sri represents structural recursion on "insert".
    // It is intended to behave like this:
    //   Sri(zero, comb) xs = {
    //     val acc = zero
    //     while (xs.hasNext) { acc = comb(acc, xs.next()) }
    //     return acc }
    //
    //  It is implemented/realised as a data structure Sri3,
    //  instead of as a function directly. This brings two
    //  virtues. The first is that Sri now "executes" on
    //  the heap instead of on the stack (Scala has a small
    //  stack and it can overflow easily.) The second, and
    //  more impt one, is that we can device many interesting
    //  combinators to manipulate the Sri3 structure: see
    //  some of these below (viz. compose, combine, combineN,
    //  withSlidingN, and withAcc), as well as
    //  many more in the AggrCollections module.
    //

    def |>[G](f:F=>G):Sri[C,G] = compose(this, f)
    // 
    // This makes it possible to compose f in a
    // pseudo-postfix manner.  This can help in
    // reducing the number of nested brackets.

    def +[G](f:Sri[C,G]):Sri[C,(F,G)] =
      combine[C,F,G,(F,G)](this, f, {case (x,y) => (x,y)}) 
    //
    // Combine of two Sri's. Simple pairing up of their results.
 
    def *[G,H](f:Sri[C,G])(mx: (F,G)=>H):Sri[C,H] = combine(this, f, mx)
    //
    // combine two Sri's. Mix their results.

    def apply(it:Iterator[C]):F = doit(it, this)
    //
    // Turn the Sri data structure into a function!
  }


  final case class Sri3[C,D,E,F](
    zero:()=>E,
    iter:C=>D,
    comb:(E,D)=>E,
    done:E=>F)
  extends Sri[C,F] 
  { 
    println("function zero inside Sri3 instantiation")
    println(zero())//
    // Sri3 is a more flexible form of Sri that includes
    // some preprocessing (iter) and postprocessing (done)
    // to make it more convenient to combine multiple
    // Sri structures. 
  }


  def sri[C,D,F](z:()=>F, i:C=>D, c:(F,D)=>F) = {
    Sri3(z, i, c, (x:F) => x)
  }
  //
  // Often post-processing is not needed;
  // so set done to x => x.
  

  def sriWithSlidingN[C,D,F](n:Int)(z:()=>F, i:C=>D, c:(Vector[D],F,D)=>F) =
    Sri3[C,D,(Vector[D],F),F]( 
      () => (Vector[D](), z()),
      i,
      {case ((u,v), y) => 
         if (u.length >= n) (u.drop(1) :+ y, c(u, v, y))
         else (u :+ y, z()) },
      {case (u,v) => v})
  //
  // Apply Sri in the manner of sliding-window showing previous
  // n items, as well as current item. 



  def compose[C,F,G](sri:Sri[C,F], g:F=>G): Sri[C,G] = sri match {
    case Sri3(z,i,c,d) =>{
      Sri3[C,D forSome{type D},E forSome{type E},G](z,i,c, x => g(d(x)))  // This might be the problem
    }
  }
  //
  // compose another function with Sri.



  def combine[C,F1,F2,F](
    sri1: Sri[C,F1],
    sri2: Sri[C,F2],
    mix: (F1,F2)=>F): Sri[C,F] =
  {
    sri1 match { case Sri3(z1, i1, c1, d1) =>
    sri2 match { case Sri3(z2, i2, c2, d2) =>
      Sri3[C,(D1 forSome{type D1},D2 forSome{type D2}),
             (E1 forSome{type E1},E2 forSome{type E2}),F](
        () => (z1(), z2()),
        y => (i1(y), i2(y)),
        { case ((a1, a2), (y1,y2)) => (c1(a1,y1), c2(a2, y2)) },
        { case (a1,a2) => mix(d1(a1), d2(a2)) })
  }}}
  //
  // Apply two Sri on the same input in synchrony.
  // Traverses the input only once.


  def combineN[C,F,G](mix:Array[F]=>G, sri:Sri[C,F]*): Sri[C,G] =
  {
    println("I am inside Combine N")
    val gs = for(s <- Array(sri:_*))
             yield {s match {case Sri3(z,i,c,d) => (z,i,c,d)}}
    val zs = for((z,i,c,d) <- gs) yield z
    val is = for((z,i,c,d) <- gs) yield i
    val cs = for((z,i,c,d) <- gs) yield c
    val ds = for((z,i,c,d) <- gs) yield d
    val mx = mix.asInstanceOf[Array[Any]=>G]

    Sri3[C,Array[D] forSome{type D},Array[E] forSome{type E},G](
      ()            => for(z <- zs) yield z(),
      y             => for(i <- is) yield i(y),
      {case (es, y) => for((c,x) <- cs.zipWithIndex) yield c(es(x),y)},
      es => { mx(for((d,x) <- ds.zipWithIndex) yield d(es(x))) })
  }
  //
  // Apply arbitrary # of Sri's in synchrony,
  // traversing the input only once.


  def withAcc[C,F](aggr: Sri[C,F]): Sri[C,(F,Vector[C])] = withAccf(x=>x, aggr)


  def withAccf[C,E,F](f:C=>E, aggr:Sri[C,F]): Sri[C,(F,Vector[E])] =
    aggr match {
      case Sri3(zero, iter, comb, done) =>
        Sri3[C,
             (U forSome{type U}, E),
             (V forSome{type V}, Vector[E]), 
             (F, Vector[E])](
          () => (zero(), Vector[E]()),
          x => (iter(x), f(x)),
          {case (x,y) => (comb(x._1, y._1), x._2 :+ y._2)},
          {case (x,y) => (done(x), y)})
    }
  //
  // Apply Sri; at the same time, accumulate the input as well. 



  def withSlidingN[C,F](n:Int)(aggr: Sri[(Vector[C],C),F]): Sri[C,F] =
  {
     aggr match {
       case Sri3(zero, iter, comb, done) =>
         compose(
           sriWithSlidingN[C,C,Any](n)(
             zero,
             y => y, 
             {case(u, v, y) =>  comb(v, iter(u, y))}),
           done)
    }
  }
  //
  // Lift an aggregate function to a sliding window of size N+1.


  
  //
  // Functions to apply Sri on iterators and other collections.
  //

  def apply[C,F](sri:Sri[C,F])(it:Iterator[C]): F = doit(it, sri)

  def typeTag[A](implicit tt: TypeTag[A]): Unit = println(typeOf[A])

  def doit[C,F](it:Iterator[C], sri:Sri[C,F]): F =
  {
    sri match { 
      case Sri3(zero, iter, comb, done) => {
        println("function zero inside do it of Sri3")
        println(zero())
        var acc = zero() // this function is () => Any because he never knows what the types of Sri3 are supposed to be
        for(c <- it) {acc = comb(acc, iter(c)) }
        return done(acc)
  }}}

}


