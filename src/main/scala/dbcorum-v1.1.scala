

package proteomics

/** Version 1, to be used with dbmodel.DBFile Version 9.
 *
 *  Wong Limsoon
 *  28 February 2022
 */


object CORUMModel {

/** A simple model to turn CORUM protein complex files into ordered collection.
 */

  import dbmodel.DBModel.CloseableIterator.CBI
  import dbmodel.DBModel.OrderedCollection.{ OColl, Key }
  import dbmodel.DBFile.{ OFile, OTransient, PARSER, FORMATTER }
  import dbmodel.TSVModel.{ TSV, TSVFile, TSVSupport, HasSchema }


  type ID   = Int
  type Bool = Boolean

  def TMP   = dbmodel.DBFile.TMP    // Default folder to use.


  type CORUMITERATOR  = CBI[CorumEntry]
  type CORUMFILE      = OFile[CorumEntry,ID]
  type CORUM[K]       = OFile[CorumEntry,K]



  case class GO(id: String, description: String)



  case class FunCat(id: String, description: String)



  case class SubUnit(
    uniprot: String, entrez: String, 
    gene: String, geneSynonyms: String, protein: String)



  case class CorumEntry(
    complexID:         ID,
    complexName:       String,
    organism:          String,
    synonyms:          String,
    cellLine:          String,
    subunits:          List[SubUnit],
    purification:      String,
    go:                List[GO],
    funcat:            List[FunCat],
    pubmed:            Int,
    subunitComment:    String,
    complexComment:    String,
    diseaseComment:    String,
    swissprotOrganism: String)
  extends HasSchema
  {
     def goIDs              = go map { _.id }
     def goDescriptions     = go map { _.description }

     def funcatIDs          = funcat map { _.id }
     def funcatDescriptions = funcat map { _.description }

     def subunitsUniprot    = subunits map { _.uniprot }
     def subunitsEntrez     = subunits map { _.entrez }
     def subunitsGene       = subunits map { _.gene }
     def subunitsSynonyms   = subunits map { _.geneSynonyms }
     def subunitsProtein    = subunits map { _.protein }

     val schema = CorumSupport.schema
  }



  val CorumFile = {
    import CorumSupport.{ schema, encoder, decoder }
    TSVFile(encoder _, decoder _, schema)
  }



  /** Sometimes, it is handy to have a transient CORUM file for
   *  temporary use without writing it to disk.
   */

  def transientCorumFile(entries: IterableOnce[CorumEntry]): CORUMFILE = {
    import CorumSupport.{ corumFileKey, schema }
    CorumFile.transientTSVFile(entries, corumFileKey, schema)
  }



  implicit class OCollToCorumFile[K](entries: OColl[CorumEntry,K])
  {
    def toCorumFile: CORUM[K] =
      CorumFile.transientTSVFile(entries.cbi, entries.key, CorumSupport.schema)
  }



  object CorumSupport
  {
    /** Schema for CORUM file
     */

    val schema = Vector(
      "ComplexID"                           -> "Int",
      "ComplexName"                         -> "String", 
      "Organism"                            -> "String",
      "Synonyms"                            -> "String", 
      "Cell line"                           -> "String",
      "subunits(UniProt IDs)"               -> "String", 
      "subunits(Entrez IDs)"                -> "String",
      "Protein complex purification method" -> "String",
      "GO ID"                               -> "String", 
      "GO description"                      -> "String", 
      "FunCat ID"                           -> "String", 
      "FunCat description"                  -> "String",
      "subunits(Gene name)"                 -> "String", 
      "Subunits comment"                    -> "String",
      "PubMed ID"                           -> "Int", 
      "Complex comment"                     -> "String", 
      "Disease comment"                     -> "String",
      "SWISSPROT organism"                  -> "String",
      "subunits(Gene name syn)"             -> "String", 
      "subunits(Protein name)"              -> "String"
    )


    /** Encode and decode GO info
     */

    def encodeGO(corum: CorumEntry): (String,String) = {
      import corum._
      (goIDs.mkString(";"), 
       goDescriptions.mkString(";"))
    }


    def decodeGO(i: String, d: String): List[GO] = {
      val (is, ds) = (i split ";", d split ";")
      val zipped   = is zip ds
      val tupled   = zipped map { GO.tupled(_) }
      tupled.toList
    }


    /** Encode and decode FunCat info
     */

    def encodeFunCat(corum: CorumEntry): (String,String) = {
      import corum._
      (funcatIDs.mkString(";"), 
       funcatDescriptions.mkString(";"))
    }


    def decodeFunCat(i: String, d: String): List[FunCat] = {
      val (is, ds) = (i split ";", d split ";")
      val zipped   = is zip ds
      val tupled   = zipped map { FunCat.tupled(_) }
      tupled.toList
    }


    /** Encode and decode subunit info
     */

    def encodeSub(corum: CorumEntry): (String,String,String,String,String) = {
      import corum._ 
      (subunitsUniprot.mkString(";"),
       subunitsEntrez.mkString(";"),
       subunitsGene.mkString(";"), 
       subunitsSynonyms.mkString(";"),
       subunitsProtein.mkString(";"))
    }


    def decodeSub(u: String, e: String, g: String, s: String, p: String)
    : List[SubUnit] = {
      val uniprot = u split ";"
      val entrez  = e split ";"
      val gene    = g split ";"
      val synonym = s split ";"
      val protein = p split ";"
      val mn = List(uniprot, entrez, gene, synonym, protein).map(_.length).max
      def get(xs: Array[String], i: Int) = if (xs.isDefinedAt(i)) xs(i) else ""
      for (
        i <- (0 to mn - 1).toList;
        u = get(uniprot, i);
        e = get(entrez, i);
        g = get(gene, i);
        s = get(synonym, i);
        p = get(protein, i))
      yield SubUnit(u, e, g, s, p)
    }


    /** CORUM-to-TSV encoder
     */

    def encoder(corum: CorumEntry): TSV = {
      import corum._
      val (u, e, g, s, p) = encodeSub(corum)
      val (fi, fd)        = encodeFunCat(corum)
      val (gi, gd)        = encodeGO(corum)
      Array(complexID, complexName, organism, synonyms, cellLine, u, e, 
            purification, gi, gd, fi, fd, g, subunitComment, pubmed, 
            complexComment, diseaseComment, swissprotOrganism, s, p)
    }


    /** TSV-to-CORUM decoder
     */

    def decoder(tsv: TSV, position: Int): CorumEntry = 
    {
      import TSVSupport.TSVCAST
      
      CorumEntry(
        complexID         = tsv.int(0),
        complexName       = tsv.str(1),
        organism          = tsv.str(2),
        synonyms          = tsv.str(3),
        cellLine          = tsv.str(4),
//      subunitsUniprot   = tsv.str(5),
//      subunitsEntrez    = tsv.str(6),
        purification      = tsv.str(7),
//      goID              = tsv.str(8),
//      goDescription     = tsv.str(9),
//      funcatID          = tsv.str(10),
//      funcatDescription = tsv.str(11),
//      subunitsGene      = tsv.str(12),
        go                = decodeGO(tsv.str(8), tsv.str(9)),
        funcat            = decodeFunCat(tsv.str(10), tsv.str(11)),
        subunitComment    = tsv.str(13),
        pubmed            = tsv.int(14),
        complexComment    = tsv.str(15),
        diseaseComment    = tsv.str(16),
        swissprotOrganism = tsv.str(17),
//      subunitsSynonym   = tsv.str(18),
//      subunitsProtein   = tsv.str(19)
        subunits = decodeSub(tsv.str(5), tsv.str(6), tsv.str(12),
                             tsv.str(18), tsv.str(19))
      )
    }


    /** Key for Corum file
     */

    val corumFileKey: Key[CorumEntry,ID] = Key(_.complexID, Ordering[Int])

  }  // end object CorumSupport


  val implicits = dbmodel.DBFile.implicits

}  // end object CORUMModel




/** Examples ***********************************************
 *
{{{

  import proteomics.CORUMModel._
  import proteomics.CORUMModel.implicits._

  val corum = CorumFile("allComplexes.txt")

  corum(9).subunits

  corum.length

  corum.filter( _.subunits.length > 5 ).saveAs("xx")

  val xx = CorumFile("xx")

  xx(8).subunits.length

  xx(8).goIDs

  xx(8).subunitsUniprot

  xx(8).subunitsEntrez

  xx.protection(false).close()

}}}
 *
 */



