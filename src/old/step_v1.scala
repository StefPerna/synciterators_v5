
package synchrony
package programming

//
// Wong Limsoon
// 27/12/2019
//


object StepContainer {

  case class Step0[A](val value: A) extends AnyVal {

    def Step1[B](f:A=>B):Step0[B] = Step0[B](f(value))

    def Step2[B](f:A=>B):Step0[B] = Step0[B](f(value))

    def Step3[B](f:A=>B):Step0[B] = Step0[B](f(value))

    def Step4[B](f:A=>B):Step0[B] = Step0[B](f(value))

    def Step5[B](f:A=>B):Step0[B] = Step0[B](f(value))

    def Step6[B](f:A=>B):Step0[B] = Step0[B](f(value))

    def Step7[B](f:A=>B):Step0[B] = Step0[B](f(value))

    def Step8[B](f:A=>B):Step0[B] = Step0[B](f(value))

    def Step9[B](f:A=>B):Step0[B] = Step0[B](f(value))

    def Return[B](f:A=>B):B = f(value)

    def Done = value
  }
}


/*
 * Just some ways to make a sequential program more natural-looking
 *

 import synchrony.programming.StepContainer._

def f() =
   Step0 { 1 } 
  .Step1 { o => o + 1} 
  .Step2 { o => o + 2} 
  .Return { o => o + 3}

def g() =
   Step0 { 1 } 
  .Step1 { o => o + 1} 
  .Step2 { o => o + 2} 
  .Done

 *
 */




