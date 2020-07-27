
package synchrony.iterators

/** Shadowed iterators.
 *
 * Shadowed iterators provide arbitrarily long look-ahead.
 *
 * Wong Limsoon
 * 25 April 2020
 */



object ShadowCollections {


  import scala.collection.mutable.Queue

  type ShadowedIterator[A] = Iterator[A] with Shadowing[A]


  /** Trait of shadowed iterators.
   *
   *  Shadowing is a trait that provides a "primary" iterator X with
   *  a "shadow iterator" Y which iterates over the same elems as
   *  iterator X. However, every elem read by iterator Y is buffered
   *  for this main iterator X to read again.  On the other hand,
   *  every elem read by X itself is gone forever even if Y has not
   *  seen it yet.
   *
   *  Shadowing generalized [[BufferedIterator]] in the sense that
   *  it offers a buffer of arbitrary length, instead of just the
   *  head of the iterator. Moreover, the buffer provided by Shadowing
   *  is editable, e.g. you can delete items from it.
   *
   *  To inherit the Shadowing trait, you must define its [[itS]] field.
   */

  trait Shadowing[A] {


   /** itS is the iterator being shadowed.
    *
    *  itS is the only field that needs to be defined when
    *  the Shadowing trait is inherited.
    */
 
    protected val itS: Iterator[A]


  /** buffer keeps the elems on itS that have been read by
   *  a shadow but have not been read by the "main" iterator yet.
   */

    final protected val buffer = new Queue[A]()
  

  /** Function to edit the buffer.
   *
   *  Currently, only filtering is supported.
   *
   *  @param f is the predicating for selecting buffered items to keep.
   *  @result is the edited buffer.
   */

    final def filterBuffer(f: A => Boolean) = buffer.filterInPlace(f)


  /** Primary is the "main" iterator.
   *
   *  Primary is same as [[itS]], except that it checks the buffer first.
   */

    final val primary = new Iterator[A] {
      override def hasNext = (!buffer.isEmpty) || itS.hasNext
      override def next() =
        if (buffer.isEmpty) itS.next() 
        else buffer.dequeue().asInstanceOf[A]
    }
      


    /** Produce a "shadow" iterator.
     *
     *  A shadow iterator is essentially a copy of [[itS]], except that
     *  it checks the buffer first, and puts everything it reads
     *  into the buffer. It essentially provides an arbitrary look-ahead
     *  into [[itS]].
     */ 

    final def shadow() = new Iterator[A]
    {
      val buff = buffer.clone

      override def hasNext = (!buff.isEmpty) || itS.hasNext

      override def next() =
        if (!buff.isEmpty) buff.dequeue().asInstanceOf[A]
        else {
          buffer.enqueue(itS.next())
          buffer.last
        }
    }

  } // End trait Shadowing.

    
 
  /** Constructor for shadowed iterators.
   */

  object ShadowedIterator {

  /** Construct shadowed iterator.
   *
   *  @param it is an ordinary iterator.
   *  @return shadowed iterator of [[it]].
   */

    def apply[A](it: Iterator[A]): ShadowedIterator[A] =
      new Iterator[A] with Shadowing[A] {
          override val itS = it
          override def hasNext = primary.hasNext
          override def next() = primary.next()
      }


  /** Construct shadowed iterator.
   *
   *  @param myHasNext
   *  @param myNext together define an ordinary iterator.
   *  @return shadowed iterator of this iterator.
   */

    def apply[A](myHasNext:Boolean, myNext:()=>A): ShadowedIterator[A] =
      new Iterator[A] with Shadowing[A] {
        override val itS = new Iterator[A]{ 
          override def hasNext = myHasNext
          override def next() = myNext()
        }

        override def hasNext = primary.hasNext
        override def next() = primary.next()
    }

  } // End object ShadowedIterator

} // object ShadowCollections




