
package synchrony.programming

/** Provides constructs for making sequential steps explicit.
 *
 *
 * Wong Limsoon
 * 25 April 2020
 */



object StepContainer {


  /** Constructor for initial result for subsequent functions to apply to.
   *
   *  @constructor is the structure for keeping an initial result.
   *  @param o is the initial result for keeping.
   *
   *  Use it like this:
   *
   *  {{{
      import synchrony.programming.StepContainer._
        Step0 {
          5
        }
        .Step1 { o =>
          o + 6
        }
        .Done
      }}}
   *
   */

  case class Step0[A](o: A) extends AnyVal {


  /** Perform next step on result of previous step.
   *
   * @param f is the next step to be done.
   * @return result of applying f to result of previous step.
   */

    def Step1[B](f: A => B): Step0[B] = Step0[B](f(o))

    def Step2[B](f: A => B): Step0[B] = Step0[B](f(o))

    def Step3[B](f: A => B): Step0[B] = Step0[B](f(o))

    def Step4[B](f: A => B): Step0[B] = Step0[B](f(o))

    def Step5[B](f: A => B): Step0[B] = Step0[B](f(o))


  /** Perform final step on result of previous step.
   *
   * @param f is the next step to be done.
   * @return result of applying f to result of previous step.
   */
 
    def Return[B](f: A => B): B = f(o)


  /** Return final result
   */

    def Done = o
  }
}




