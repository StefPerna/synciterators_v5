package synchrony 
package co 

//
// Wong Limsoon
// 31 Dec 2019
//
// Support for a simple self-describing complex object data exchange format.
// A complex object is self-described according to the grammar below:
//
// Term   ::= Number | String | Bool | Tuple | List | Set | Record
// Number ::= ... follows the format of Scala doubles...
// String ::= ... follows the format of Scala strings...
// Bool   ::= true | false
// Tuple  ::= (Term, ..., Term)
// Record ::= Rec(Label: Term, ..., Label: Term)
// List   ::= List(Term, ..., Term)
// Set    ::= Set(Term, ..., Term)
// Label  ::= ... any string of alphanumeric letters
// with the constraint that labels in a record are not repeated.
//
// TODO: This implementation currently does not support Record and also
// does not support Tuples that are too long. Also, this implementation
// assumes lists and sets are homogeneous.
//
// Each complex object has a type. Types are written in the same
// way as Scala structure types. To wit:
//
// T     ::= Double | String | Boolean | Rec(Label: T, ..., Label: T)
//        | (T, ..., T) | List[T] | Set[T] 
// with the constraint that labels in a record type are not repeated.
//

object SimpleCO {

  import scala.util.parsing.combinator._
  import scala.reflect.runtime.universe.TypeTag

  //
  // RawCO is the representation of complex objects.
  // It is a self-describing representation that
  // allows arbitrary complex objects to be composed.

  sealed trait RawCO {
    def co: RawCO
    def mk[A<:RawCO]: A = co.asInstanceOf[A]
  }

  final case class RawDouble(x: Double) extends RawCO {
    override def co: RawCO = this
    def it: Double = x
  }

  final case class RawString(x: String) extends RawCO {
    override def co: RawCO = this
    def it: String = x
  } 

  final case class RawBool(x: Boolean) extends RawCO {
    override def co: RawCO = this 
    def it: Boolean = x
  }

  trait RawTuple extends RawCO {
    def p1: RawCO = ???
    def p2: RawCO = ???
    def p3: RawCO = ???
    def p4: RawCO = ???
    def p5: RawCO = ???
    def p6: RawCO = ???
    def p7: RawCO = ???
    def p8: RawCO = ???
    def p9: RawCO = ???
    def p10: RawCO = ???
    def p11: RawCO = ???
    def p12: RawCO = ???
    def p13: RawCO = ???
    def p14: RawCO = ???
    def p15: RawCO = ???
  }

  final case class RawTuple2(a: RawCO, b: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b 
  }
  
  final case class RawTuple3(a: RawCO, b: RawCO, c: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b 
    override def p3: RawCO = c
  }

  final case class RawTuple4(
    a: RawCO, b: RawCO, c: RawCO, d: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
  }

  final case class RawTuple5(
    a: RawCO, b: RawCO, c: RawCO, d: RawCO, e: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b 
    override def p3: RawCO = c
    override def p4: RawCO = d
    override def p5: RawCO = e
  }

  final case class RawTuple6(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
  }

  final case class RawTuple7(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO,
    g: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
    override def p7: RawCO = g
  }

  final case class RawTuple8(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO,
    g: RawCO, h: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
    override def p7: RawCO = g
    override def p8: RawCO = h
  }

  final case class RawTuple9(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO,
    g: RawCO, h: RawCO, i: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
    override def p7: RawCO = g
    override def p8: RawCO = h
    override def p9: RawCO = i
  }

  final case class RawTuple10(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO,
    g: RawCO, h: RawCO, i: RawCO,
    j: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
    override def p7: RawCO = g
    override def p8: RawCO = h
    override def p9: RawCO = i
    override def p10: RawCO = j
  }

  final case class RawTuple11(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO,
    g: RawCO, h: RawCO, i: RawCO,
    j: RawCO, k: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
    override def p7: RawCO = g
    override def p8: RawCO = h
    override def p9: RawCO = i
    override def p10: RawCO = j
    override def p11: RawCO = k
  }

  final case class RawTuple12(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO,
    g: RawCO, h: RawCO, i: RawCO,
    j: RawCO, k: RawCO, l: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
    override def p7: RawCO = g
    override def p8: RawCO = h
    override def p9: RawCO = i
    override def p10: RawCO = j
    override def p11: RawCO = k
    override def p12: RawCO = l
  }

  final case class RawTuple13(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO,
    g: RawCO, h: RawCO, i: RawCO,
    j: RawCO, k: RawCO, l: RawCO,
    m: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
    override def p7: RawCO = g
    override def p8: RawCO = h
    override def p9: RawCO = i
    override def p10: RawCO = j
    override def p11: RawCO = k
    override def p12: RawCO = l
    override def p13: RawCO = m
  }

  final case class RawTuple14(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO,
    g: RawCO, h: RawCO, i: RawCO,
    j: RawCO, k: RawCO, l: RawCO,
    m: RawCO, n: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
    override def p7: RawCO = g
    override def p8: RawCO = h
    override def p9: RawCO = i
    override def p10: RawCO = j
    override def p11: RawCO = k
    override def p12: RawCO = l
    override def p13: RawCO = m
    override def p14: RawCO = n
  }

  final case class RawTuple15(
    a: RawCO, b: RawCO, c: RawCO, 
    d: RawCO, e: RawCO, f: RawCO,
    g: RawCO, h: RawCO, i: RawCO,
    j: RawCO, k: RawCO, l: RawCO,
    m: RawCO, n: RawCO, o: RawCO) extends RawTuple {
    override def co: RawCO = this
    override def p1: RawCO = a
    override def p2: RawCO = b
    override def p3: RawCO = c 
    override def p4: RawCO = d
    override def p5: RawCO = e
    override def p6: RawCO = f
    override def p7: RawCO = g
    override def p8: RawCO = h
    override def p9: RawCO = i
    override def p10: RawCO = j
    override def p11: RawCO = k
    override def p12: RawCO = l
    override def p13: RawCO = m
    override def p14: RawCO = n
    override def p15: RawCO = o
  }

  final case class RawList(x: List[RawCO]) extends RawCO {
    override def co: RawCO = this
    def it: List[RawCO] = x
  }

  final case class RawSet(x: Set[RawCO]) extends RawCO {
    override def co: RawCO = this
    def it: Set[RawCO] = x
  }

  //
  // RawCOParser is a simple parser for the self-describing
  // complex object format. It uses the standard Scala parsing
  // combinators. TODO: Re-implement using more efficient
  // parsing combinators, e.g. fastparse2. Also, trap errors
  // when parsing input that does not conform to the format.

  class RawCOParser extends JavaTokenParsers { 

    def rawDouble: Parser[RawDouble] = floatingPointNumber ^^ { 
      x => RawDouble(x.toDouble) }

    def rawString: Parser[RawString] = stringLiteral ^^ {
      x => RawString(x) }

    def rawBool: Parser[RawBool] =
      "true" ^^ { _ => RawBool(true) } |
      "false" ^^ { _ => RawBool(false) }

    def rawTuple2: Parser[RawTuple2] =
      "("~rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~")" => RawTuple2(t1, t2)
      case _ => ??? 
    }

    def rawTuple3: Parser[RawTuple3] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~")" => RawTuple3(t1, t2, t3)
      case _ => ???
    }

    def rawTuple4: Parser[RawTuple4] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~")" => RawTuple4(t1, t2, t3, t4)
      case _ => ???
    }

    def rawTuple5: Parser[RawTuple5] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~")" ^^ 
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~")" => 
        RawTuple5(t1, t2, t3, t4, t5)
      case _ => ???
    }

    def rawTuple6: Parser[RawTuple6] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~ rawTerm~","~
          rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~")" => 
        RawTuple6(t1, t2, t3, t4, t5, 
          t6)
      case _ => ???
    }

    def rawTuple7: Parser[RawTuple7] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~")" => 
        RawTuple7(t1, t2, t3, t4, t5,
          t6, t7)
      case _ => ???
    }

    def rawTuple8: Parser[RawTuple8] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~")" => 
        RawTuple8(t1, t2, t3, t4, t5,
          t6, t7, t8)
      case _ => ???
    }

    def rawTuple9: Parser[RawTuple9] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~")" => 
        RawTuple9(t1, t2, t3, t4, t5,
          t6, t7, t8, t9)
      case _ => ???
    }

    def rawTuple10: Parser[RawTuple10] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~","~ rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~")" => 
        RawTuple10(t1, t2, t3, t4, t5, 
          t6, t7, t8, t9, t10)
      case _ => ???
    }

    def rawTuple11: Parser[RawTuple11] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~","~
               t11~")" => 
        RawTuple11(t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10,
          t11)
      case _ => ???
    }

    def rawTuple12: Parser[RawTuple12] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~","~
               t11~","~t12~")" =>
        RawTuple12(t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10,
          t11, t12)
      case _ => ???
    }

    def rawTuple13: Parser[RawTuple13] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~","~
               t11~","~t12~","~t13~")" =>
        RawTuple13(t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10,
          t11, t12, t13)
      case _ => ???
    }

    def rawTuple14: Parser[RawTuple14] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~","~
               t11~","~t12~","~t13~","~t14~")" =>
        RawTuple14(t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10, 
          t11, t12, t13, t14)
      case _ => ???
    }

    def rawTuple15: Parser[RawTuple15] = 
      "("~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~
          rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~","~rawTerm~")" ^^
    { 
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~t6~","~
               t7~","~t8~","~t9~","~t10~","~t11~","~t12~","~
               t13~","~t14~","~t15~")" =>
        RawTuple15(t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10,
          t11, t12, t13, t14, t15)
      case _ => ???
    }


    def rawList: Parser[RawList] = "List("~repsep(rawTerm, ",")~")" ^^ {
      case "List("~ts~")" => RawList(ts)
      case _ => ??? }

    def rawSet: Parser[RawSet] = "Set("~repsep(rawTerm, ",")~")" ^^ {
      case "Set("~ts~")" => RawSet(ts.toSet)
      case _ => ??? }

   def rawTerm = rawDouble | rawString | rawBool |
     rawTuple2 | rawTuple3 | rawTuple4 | rawTuple5 |
     rawTuple6 | rawTuple7 | rawTuple8 | rawTuple9 | rawTuple10 |
     rawTuple11 | rawTuple12 | rawTuple13 | rawTuple14 | rawTuple15 |
     rawList | rawSet 
  }

  object RawCOParser {
    def apply(s: String): RawCO = {
      val parser = new RawCOParser
      return (parser parseAll (parser.rawTerm, s)).get.co
    }
  }

  //
  // COT is a representation for complex object type.
  // It also provides a mapping from complex objects
  // to Scala native objects/values

  sealed trait COT {

    type CT <: COT

    type T

    def cook(raw: RawCO): T
    //
    // CT is a complex object type.
    // T is the Scala native type corresponding to CT.
    // cook is a function to transform a RawCO of type CT
    // into a Scala native object/value of type T.
    // TODO: Trap errors when cooking a RawCO whose type
    // does not conform to CT/T.
  }

  final case class COTDouble() 
  extends COT {
    override type CT = COTDouble
    override type T = Double
    override def cook(raw: RawCO): T = raw.mk[RawDouble].it
  }

  final case class COTString()
  extends COT {
    override type CT = COTString
    override type T = String
    override def cook(raw: RawCO): T = raw.mk[RawString].it
  }

  final case class COTBool()
  extends COT {
    override type CT = COTBool
    override type T = Boolean
    override def cook(raw: RawCO): T = raw.mk[RawBool].it
  }

  final case class COTList[A<:COT](x: A)
  extends COT {
    override type CT = COTList[A]
    override type T = List[x.T]
    override def cook(raw: RawCO): T = (raw.mk[RawList].it) map (x cook _)
  }

  final case class COTSet[A<:COT](x: A)
  extends COT {
    override type CT = COTSet[A]
    override type T = Set[x.T]
    override def cook(raw: RawCO): T = (raw.mk[RawSet].it) map (x cook _)
  }

  final case class COTTuple2[A<:COT,B<:COT](a: A, b: B)
  extends COT {
    override type CT = COTTuple2[A,B]
    override type T = (a.T, b.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple2 = raw.mk[RawTuple2]
      return (a.cook(r.p1), b.cook(r.p2))
    }
  }

  final case class COTTuple3[A<:COT,B<:COT,C<:COT](a:A, b:B, c:C)
  extends COT {
    override type CT = COTTuple3[A,B,C]
    override type T = (a.T, b.T, c.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple3 = raw.mk[RawTuple3]
      return (a.cook(r.p1), b.cook(r.p2), c.cook(r.p3))
    }
  }

  final case class COTTuple4[A<:COT,B<:COT,C<:COT,D<:COT](a:A, b:B, c:C, d:D)
  extends COT {
    override type CT = COTTuple4[A,B,C,D]
    override type T = (a.T, b.T, c.T, d.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple4 = raw.mk[RawTuple4]
      return (a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4))
    }
  }

  final case class COTTuple5[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT](
                             a:A, b:B, c:C, d:D, e:E)
  extends COT {
    override type CT = COTTuple5[A,B,C,D,E]
    override type T = (a.T, b.T, c.T, d.T, e.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple5 = raw.mk[RawTuple5]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5))
    }
  }

  final case class COTTuple6[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                             F<:COT](
                             a:A, b:B, c:C, d:D, e:E,
                             f:F)
  extends COT {
    override type CT = COTTuple6[A,B,C,D,E,F]
    override type T = (a.T, b.T, c.T, d.T, e.T,
                       f.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple6 = raw.mk[RawTuple6]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6))
    }
  }

  final case class COTTuple7[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                             F<:COT,G<:COT](
                             a:A, b:B, c:C, d:D, e:E,
                             f:F, g:G)
  extends COT {
    override type CT = COTTuple7[A,B,C,D,E,F,G]
    override type T = (a.T, b.T, c.T, d.T, e.T,
                       f.T, g.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple7 = raw.mk[RawTuple7]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6), g.cook(r.p7))
    }
  }

  final case class COTTuple8[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                             F<:COT,G<:COT,H<:COT](
                             a:A, b:B, c:C, d:D, e:E,
                             f:F, g:G, h:H)
  extends COT {
    override type CT = COTTuple8[A,B,C,D,E,F,G, H]
    override type T = (a.T, b.T, c.T, d.T, e.T,
                       f.T, g.T, h.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple8 = raw.mk[RawTuple8]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6), g.cook(r.p7), h.cook(r.p8))
    }
  }

  final case class COTTuple9[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                             F<:COT,G<:COT,H<:COT,I<:COT](
                             a:A, b:B, c:C, d:D, e:E,
                             f:F, g:G, h:H, i:I)
  extends COT {
    override type CT = COTTuple9[A,B,C,D,E,F,G, H,I]
    override type T = (a.T, b.T, c.T, d.T, e.T,
                       f.T, g.T, h.T, i.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple9 = raw.mk[RawTuple9]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6), g.cook(r.p7), h.cook(r.p8), i.cook(r.p9))
    }
  }

  final case class COTTuple10[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                              F<:COT,G<:COT,H<:COT,I<:COT,J<:COT](
                              a:A, b:B, c:C, d:D, e:E,                                                        f:F, g:G, h:H, i:I, j:J)
  extends COT {
    override type CT = COTTuple10[A,B,C,D,E,F,G,H,I,J]
    override type T = (a.T, b.T, c.T, d.T, e.T,
                       f.T, g.T, h.T, i.T, j.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple10 = raw.mk[RawTuple10]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6), g.cook(r.p7), h.cook(r.p8), i.cook(r.p9), j.cook(r.p10))
    }
  }

  final case class COTTuple11[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                              F<:COT,G<:COT,H<:COT,I<:COT,J<:COT,
                              K<:COT](
                              a:A, b:B, c:C, d:D, e:E,
                              f:F, g:G, h:H, i:I, j:J,
                              k:K)
  extends COT {
    override type CT = COTTuple11[A,B,C,D,E,F,G,H,I,J,K]
    override type T = (a.T, b.T, c.T, d.T, e.T, 
                       f.T, g.T, h.T, i.T, j.T,
                       k.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple11 = raw.mk[RawTuple11]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6), g.cook(r.p7), h.cook(r.p8), i.cook(r.p9), j.cook(r.p10),
        k.cook(r.p11))
    }
  }

  final case class COTTuple12[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                              F<:COT,G<:COT,H<:COT,I<:COT,J<:COT,
                              K<:COT,L<:COT](
                              a:A, b:B, c:C, d:D, e:E,
                              f:F, g:G, h:H, i:I, j:J,
                              k:K, l:L)
  extends COT {
    override type CT = COTTuple12[A,B,C,D,E,F,G,H,I,J,K,L]
    override type T = (a.T, b.T, c.T, d.T, e.T, 
                       f.T, g.T, h.T, i.T, j.T,
                       k.T, l.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple12 = raw.mk[RawTuple12]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6), g.cook(r.p7), h.cook(r.p8), i.cook(r.p9), j.cook(r.p10),
        k.cook(r.p11), l.cook(r.p12))
    }
  }

  final case class COTTuple13[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                              F<:COT,G<:COT,H<:COT,I<:COT,J<:COT,
                              K<:COT,L<:COT,M<:COT](
                              a:A, b:B, c:C, d:D, e:E,
                              f:F, g:G, h:H, i:I, j:J,
                              k:K, l:L, m:M)
  extends COT {
    override type CT = COTTuple13[A,B,C,D,E,F,G,H,I,J,K,L,M]
    override type T = (a.T, b.T, c.T, d.T, e.T,
                       f.T, g.T, h.T, i.T, j.T,
                       k.T, l.T, m.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple13 = raw.mk[RawTuple13]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6), g.cook(r.p7), h.cook(r.p8), i.cook(r.p9), j.cook(r.p10), 
        k.cook(r.p11), l.cook(r.p12), m.cook(r.p13))
    }
  }
  
  final case class COTTuple14[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                              F<:COT,G<:COT,H<:COT,I<:COT,J<:COT,
                              K<:COT,L<:COT,M<:COT,N<:COT](
                              a:A, b:B, c:C, d:D, e:E,
                              f:F, g:G, h:H, i:I, j:J,
                              k:K, l:L, m:M, n:N)
  extends COT {
    override type CT = COTTuple14[A,B,C,D,E,F,G,H,I,J,K,L,M,N]
    override type T = (a.T, b.T, c.T, d.T, e.T,
                       f.T, g.T, h.T, i.T, j.T,
                       k.T, l.T, m.T, n.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple14 = raw.mk[RawTuple14]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6), g.cook(r.p7), h.cook(r.p8), i.cook(r.p9), j.cook(r.p10), 
        k.cook(r.p11), l.cook(r.p12), m.cook(r.p13), n.cook(r.p14))
    }
  }
 
  final case class COTTuple15[A<:COT,B<:COT,C<:COT,D<:COT,E<:COT,
                              F<:COT,G<:COT,H<:COT,I<:COT,J<:COT,
                              K<:COT,L<:COT,M<:COT,N<:COT,O<:COT](
                              a:A, b:B, c:C, d:D, e:E,
                              f:F, g:G, h:H, i:I, j:J,
                              k:K, l:L, m:M, n:N, o:O)
  extends COT {
    override type CT = COTTuple15[A,B,C,D,E,F,G,H,I,J,K,L,M,N,O]
    override type T = (a.T, b.T, c.T, d.T, e.T,
                       f.T, g.T, h.T, i.T, j.T,
                       k.T, l.T, m.T, n.T, o.T)
    override def cook(raw: RawCO): T = {
      val r: RawTuple15 = raw.mk[RawTuple15]
      return (
        a.cook(r.p1), b.cook(r.p2), c.cook(r.p3), d.cook(r.p4), e.cook(r.p5),
        f.cook(r.p6), g.cook(r.p7), h.cook(r.p8), i.cook(r.p9), j.cook(r.p10), 
        k.cook(r.p11),l.cook(r.p12),m.cook(r.p13),n.cook(r.p14),o.cook(r.p15))
    }
  }


  //
  // COTParser is a simple parser to parse strinsg representing
  // complex object types. TODO: Trap errors when parsing input
  // that does not conform to type syntax.

  class COTParser extends JavaTokenParsers {

    def coT: Parser[COT] = cotDouble | cotString | cotBool |
      cotList | cotSet | cotTuple | cotTypeTag

    def cotTypeTag: Parser[COT] = "TypeTag["~coT~"]" ^^ {
      case "TypeTag["~t~"]" => t
      case _ => ???  }
    //
    // If the input string was produced by Scala's type reflection
    // routines, the actual type string would be enclosed by
    // a TypeTag[]. So just strip this.

    def cotDouble: Parser[COT] = "Double" ^^ { _ => COTDouble() }

    def cotString: Parser[COT] = "String" ^^ {_ => COTString() }

    def cotBool: Parser[COT] = "Boolean" ^^{ _ => COTBool() }

    def cotTuple: Parser[COT] = 
      cotTuple2 | cotTuple3 | cotTuple4 | cotTuple5 |
      cotTuple6 | cotTuple7 | cotTuple8 | cotTuple9 | cotTuple10 | 
      cotTuple11 | cotTuple12 | cotTuple13 | cotTuple14 | cotTuple15

    def cotTuple2: Parser[COT] = "("~coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~")" => COTTuple2(t1, t2)
      case _ => ???
    }  

    def cotTuple3: Parser[COT] = "("~coT~","~coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~")" => COTTuple3(t1, t2, t3)
      case _ => ???
    }  

    def cotTuple4: Parser[COT] = "("~coT~","~coT~","~coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~")" => COTTuple4(t1, t2, t3, t4)
      case _ => ???
    }  

    def cotTuple5 = "("~coT~","~coT~","~coT~","~coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~")" =>
        COTTuple5(t1, t2, t3, t4, t5)
      case _ => ???
    }  

    def cotTuple6 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
      coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
              t6~")" =>
        COTTuple6(
          t1, t2, t3, t4, t5, 
          t6)
      case _ => ???
    }  

    def cotTuple7 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
                        coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~")" =>
        COTTuple7(
          t1, t2, t3, t4, t5,
          t6, t7)
      case _ => ???
    }  

    def cotTuple8 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
                        coT~","~coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~")" => 
        COTTuple8(
          t1, t2, t3, t4, t5,
          t6, t7, t8)
      case _ => ???
    }  

    def cotTuple9 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
                        coT~","~coT~","~coT~","~coT~")" ^^ 
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~")" =>
        COTTuple9(
          t1, t2, t3, t4, t5,
          t6, t7, t8, t9)
      case _ => ???
    }  

    def cotTuple10 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~","~coT~","~coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~")" => 
        COTTuple10(
          t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10)
      case _ => ???
    }  

    def cotTuple11 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~","~
               t11~")" => 
        COTTuple11(
          t1, t2, t3, t4, t5, 
          t6, t7, t8, t9, t10, 
          t11)
      case _ => ???
    }  

    def cotTuple12 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~","~
               t11~","~t12~")" => 
        COTTuple12(
          t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10, 
          t11, t12)
      case _ => ???
    }  

    def cotTuple13 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~","~
               t11~","~t12~","~t13~")" => 
        COTTuple13(
          t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10, 
          t11, t12, t13)
      case _ => ???
    }  

    def cotTuple14 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~","~coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~","~
               t11~","~t12~","~t13~","~t14~")" => 
        COTTuple14(
          t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10, 
          t11, t12, t13, t14)
      case _ => ???
    }  

    def cotTuple15 = "("~coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~","~coT~","~coT~","~coT~","~
                         coT~","~coT~","~coT~","~coT~","~coT~")" ^^
    {
      case "("~t1~","~t2~","~t3~","~t4~","~t5~","~
               t6~","~t7~","~t8~","~t9~","~t10~","~
               t11~","~t12~","~t13~","~t14~","~t15~")" => 
        COTTuple15(
          t1, t2, t3, t4, t5,
          t6, t7, t8, t9, t10, 
          t11, t12, t13, t14, t15)
      case _ => ???
    }  

    def cotList: Parser[COT] = "List["~coT~"]" ^^ {
      case "List["~t~"]" => COTList(t)
      case _ => ??? }

    def cotSet: Parser[COT] = "Set["~coT~"]" ^^ {
      case "Set["~t~"]" => COTSet(t)
      case _ => ??? } 
  }

  object COTParser {
    def apply(s: String): COT = {
      val parser = new COTParser
      return (parser parseAll (parser.coT, s)).get
    }
  }

  //
  // SimpleCO[aType](aRawCO) converts aRawCO complex object
  // of type aType into a Scala native object/value of the
  // corresponding Scala type.

  def apply[A](raw: RawCO)(implicit ev: TypeTag[A]) =
    COTParser(ev.toString).cook(raw).asInstanceOf[A]

  def apply[A](c: String)(implicit ev: TypeTag[A]) =
    COTParser(ev.toString).cook(RawCOParser(c)).asInstanceOf[A]

  //
  // Try some examples.
  //
  // SimpleCO[List[(Double,Double)]]("List((1,2),(3,4),(5,6))")
  // SimpleCO[List[(Double,Double)]]("List()")
  // SimpleCO[List[Double],Double)]("(List(1,2,3,4),6)")
  // SimpleCO[(List[Double],List[Double])]("(List(1,2),List(3,4))")
  //
}

//
// SimpleCOFile is an incremental parser to read complex
// objects incrementally into Scala.
// 
// It assumes a simple "model" of a data file:
// Every "object" is on a separate line.
// All lines have the same kind/type of objects.
// Objects are in the self-describing format above.
// So a data file is just a sequence or list of objects.
// 
// To create a data file object, the user provides a filename
// and a parser. The parser sees a line, which it parses and
// returns the object in that line.  The created data file
// object can then be made into iterators, streams, etc.
//
// TODO: Trap errors when input file is non-compliant.
//

object SimpleCOFile {

  // import scala.language.existentials
  import scala.reflect.runtime.universe.TypeTag
  import scala.io.Source
  import SimpleCO._

  //
  // A file is "untyped" if it does not explicitly provide
  // the type of the complex objects it contains. In this
  // case, user must provide the type explicity.
  //
  // TODO: Trap errors when user provides wrong type, 

  class UntypedCOFile[A](
    file: String, 
    parser: String => A,
    chkFormat: String => Boolean = (_ => true)) {

    def iterator: Iterator[A] = 
      Source.fromFile(file).getLines.filter(chkFormat).map(parser)
  }

  object UntypedCOFile {
    def apply[A](file: String)(implicit ev: TypeTag[A]) = 
      new UntypedCOFile[A](file, (c: String) => SimpleCO[A](c)(ev))

    def apply[A](file:String, chk:String => Boolean)(implicit ev: TypeTag[A]) = 
      new UntypedCOFile[A](file, (c: String) => SimpleCO[A](c)(ev), chk)

    def apply(file:String) = new UntypedCOFile[RawCO](file, x => RawCOParser(x))

    def apply(file:String, chk: String => Boolean) =
      new UntypedCOFile[RawCO](file, x => RawCOParser(x), chk)

    def apply[A](file: String, parser: String => A, chk: String => Boolean) =
      new UntypedCOFile[A](file, parser, chk)
  }
  
  //
  // A file is "typed" if its first line explicitly provides
  // the type of the complex objects it contains. In this
  // case, the user does not provide the type.

  class TypedCOFile {
    //
    // TODO
    //
  }
}

//
// Implicit conversion functions from RawCO to Scala natives.
// Use these with caution as they effectively bypasses
// Scala's static typechecking.
//
// Example:
//
// val a = RawCOParser("(1,2,List((3,4),(5,6)), 7)")
// for(x <- a.p3) println((x.p2 : Double) + 10)

object Conversions {

    import scala.language.implicitConversions
    import SimpleCO._

    implicit def raw2Double(x: RawCO): Double = x.mk[RawDouble].it
    implicit def raw2String(x: RawCO): String = x.mk[RawString].it
    implicit def raw2Boolean(x: RawCO): Boolean = x.mk[RawBool].it
    implicit def raw2List(x: RawCO): List[RawCO] = x.mk[RawList].it
    implicit def raw2Tuple(x: RawCO): RawTuple = x.mk[RawTuple]

    /*
     *
    implicit def raw2Set(x: RawCO): Set[RawCO] = x.mk[RawSet].it

    implicit def raw2Tuple2(x: RawCO): (RawCO,RawCO) = {
      val r = x.mk[RawTuple]
      (r.p1, r.p2)
    }

    implicit def raw2Tuple3(x: RawCO): (RawCO,RawCO,RawCO) = {
      val r = x.mk[RawTuple]
      (r.p1, r.p2, r.p3)
    }

    implicit def raw2Tuple4(x: RawCO): (RawCO,RawCO,RawCO,RawCO) = {
      val r = x.mk[RawTuple]
      (r.p1, r.p2, r.p3, r.p4)
    }

    implicit def raw2Tuple5(x: RawCO): (RawCO,RawCO,RawCO,RawCO,RawCO) = {
      val r = x.mk[RawTuple]
      (r.p1, r.p2, r.p3, r.p4, r.p5)
    }

    implicit def raw2Tuple6(x: RawCO): (RawCO,RawCO,RawCO,RawCO,RawCO,RawCO) = {
      val r = x.mk[RawTuple]
      (r.p1, r.p2, r.p3, r.p4, r.p5, r.p6)
    }
    *
    */
  }




