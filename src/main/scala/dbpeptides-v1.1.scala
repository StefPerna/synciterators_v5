

package proteomics

/** Version 1, to be used with dbmodel.DBFile Version 9.
 *
 *  Wong Limsoon
 *  27 February 2022
 */


object PEPTIDEModel {

/** A simple model to turn DDA peptide files into ordered collection.
 */

  import dbmodel.DBModel.CloseableIterator.CBI
  import dbmodel.DBModel.OrderedCollection.{ OColl, Key }
  import dbmodel.DBFile.{ OFile, OTransient, PARSER, FORMATTER }
  import dbmodel.TSVModel.{ SCHEMA, TSV, TSVSupport, TSVdbc, FlexibleTSVdbc }


  type ID   = Int
  type Bool = Boolean


  def TMP = dbmodel.DBFile.TMP    // Default folder to use.



/** Peptide file
 */


  type PEPTIDEITERATOR = CBI[PeptideEntry]
  type PEPTIDEFILE     = OFile[PeptideEntry,ID]
  type PEPTIDE[K]      = OFile[PeptideEntry,K]

  case class PeptideHit(accession: String, start: Int, end: Int)

  case class PeptideEntry(
    id: ID, 
    rt: Double,         mz: Double,
    score: Double,      rank: Int,
    sequence: String,   charge: Int,
    aa_before: String,  aa_after: String,
    score_type: String, search_id: String,
    predicted_rt: Int,  predicted_pt: Int,
    hits: List[PeptideHit])
  {
    def seq: String = sequence.replaceAll("\\(Oxidation\\)", "")
                              .replaceAll("\\(Carbamidomethyl\\)", "")
                              .replaceAll("\\.\\(Glu->pyro-Glu\\)", "")
                              .replaceAll("\\.\\(Gln->pyro-Glu\\)", "")
                              .replaceAll("\\.\\(Acetyl\\)", "")
                              .replaceAll("\\.\\(Ammonia-loss\\)", "")

    def accessions: List[String] = hits map { _.accession }

    def starts: List[Int]        = hits map { _.start }

    def ends: List[Int]          = hits map { _.end }

  }  // end case class PeptideEntry
  


  implicit class OCollToPeptideFile[K](entries: OColl[PeptideEntry,K])
  {
    def toPeptideFile: PEPTIDE[K] = 
      PeptideFile.transientPeptideFile(entries.cbi)
                 .assumeOrderedBy(entries.key)
  }



  object PeptideFile
  {
    /** Schema for Peptide file
     */

    val schema = Vector(
      "#PEPTIDE"          -> "String", 
      "rt"                -> "Double", "mz"                -> "Double", 
      "score"             -> "Double", "rank"              -> "Int", 
      "sequence"          -> "String", "charge"            -> "Int", 
      "aa_before"         -> "String", "aa_after"          -> "String",
      "score_type"        -> "String", "search_identifier" -> "String", 
      "accessions"        -> "String", "start"             -> "String", 
      "end"               -> "String", "predicted_rt"      -> "Int",
      "predicted_pt"      -> "Int"
    )

  
    /** Decode and encode hits info
     */

    def decodeHits(a: String, s: String, e: String) = {
      // Some peptide is mapped to several target/decoy.
      // the hits, accessions, start, end fields must be kept in sync.
      val (as, ss, es) = (a.split(";"), s.split(";"), e.split(";"))
      val starts = ss map { _.toInt }
      val ends   = es map { _.toInt }
      val zipped = as.lazyZip(starts).lazyZip(ends).toList
      zipped map { PeptideHit.tupled(_) }
    } 


    def encodeHits(hits: List[PeptideHit]): (String, String, String) = {
      // Some peptide is mapped to several target/decoy.
      // the hits, accessions, start, end fields must be kept in sync.
      val (as, ss, es) = hits.map(PeptideHit.unapply(_).get).unzip3
      val accessions   = as.mkString(";") 
      val start        = ss.map(_.toString).mkString(";")
      val end          = es.map(_.toString).mkString(";")
      (accessions, start, end)
    }


    /** Peptide file-to-TSV encoder
     */

    def encoder(peptide: PeptideEntry): TSV = {
      import peptide._
      val (accessions, start, end) = encodeHits(hits)
      Array("PEPTIDE", rt, mz, score, rank, sequence, charge,
            aa_before, aa_after, score_type, search_id,
            accessions, start, end, predicted_rt, predicted_pt)
    }
 

    /** TSV-to-Peptide file decoder
     */

    def decoder(tsv: TSV, position: Int): PeptideEntry =
    {
      import TSVSupport.TSVCAST

      PeptideEntry(
        id           = position,
        rt           = tsv.dbl(1),   mz           = tsv.dbl(2),
        score        = tsv.dbl(3),   rank         = tsv.int(4),
        sequence     = tsv.str(5),   charge       = tsv.int(6),
        aa_before    = tsv.str(7),   aa_after     = tsv.str(8),
        score_type   = tsv.str(9),   search_id    = tsv.str(10),
//      accessions   = tsv.str(11),
//      start        = tsv.str(12),
//      end          = tsv.str(13),
        predicted_rt = tsv.int(14),  predicted_pt = tsv.int(15),
        hits         = decodeHits(tsv.str(11), tsv.str(12), tsv.str(13))
      )
    }


    /** TSV databae connectivity
     */

    val header = List(
      "#PEPTIDE", "rt", "mz", "score", "rank", "sequence", 
      "charge", "aa_before", "aa_after", "score_type", 
      "search_identifier", "accessions", "start", "end",
      "predicted_rt", "predicted_pt"
    ).mkString("\t")


    val dbc = {
      import FlexibleTSVdbc._
      val genHeader = (schema: SCHEMA, b: PeptideEntry) => header
      val guard = (line: String, l: Int, p: Int) => line startsWith "PEPTIDE"
      val formatter = genFormatter(schema, Some(genHeader), encoder) 
      val parser    = genParser(schema, Some(guard), decoder)
      FlexibleTSVdbc(schema, formatter, parser)
    }
  

    /** Key for PeptideFile
     */

    val peptideFileKey: Key[PeptideEntry,ID] = Key(_.id, Ordering[Int])


    /** Constructors
     */

    def apply(filename: String): PEPTIDEFILE = 
      dbc.connect(peptideFileKey, filename, TMP)


    def apply(filename: String, folder: String): PEPTIDEFILE =
      dbc.connect(peptideFileKey, filename, folder)



    /** Sometimes, it is handy to have a transient Peptide file for
     *  temporary use without writing it to disk.
     */

    def transientPeptideFile(entries: IterableOnce[PeptideEntry]): PEPTIDEFILE =
      OTransient[PeptideEntry,ID](peptideFileKey, CBI(entries),
                                  dbc.parser, dbc.formatter, 
                                  OFile.destructorOFile _)

    def emptyPeptideFile: PEPTIDEFILE =
      OTransient[PeptideEntry,ID](peptideFileKey, CBI(),
                                  dbc.parser, dbc.formatter,
                                  OFile.destructorOFile _)

  }  // End object PeptideFile



/** Protein file
 */


  type PROTEINITERATOR = CBI[ProteinEntry]
  type PROTEINFILE     = OFile[ProteinEntry,ID]
  type PROTEIN[K]      = OFile[ProteinEntry,K]


  case class ProteinEntry(
    id: ID,
    score: Double,          rank: Int,
    accession: String,      protein_description: String,
    coverage: Double,       sequence: String,
    target_decoy: String)


  implicit class OCollToProteinFile[K](entries: OColl[ProteinEntry,K])
  {
    def toProteinFile: PROTEIN[K] = 
      ProteinFile.transientProteinFile(entries.cbi)
                 .assumeOrderedBy(entries.key)
  }


  object ProteinFile
  {
    /** Schema for Protein file
     */

    val schema = Vector(
      "#PROTEIN"            -> "String", "score"               -> "Double",
      "rank"                -> "Int",    "accession"           -> "String",
      "protein_description" -> "String", "coverage"            -> "Double",
      "sequence"            -> "String", "target_decoy"        -> "String"
    )


    /** Protein file-to-TSV encoder
     */

    def encoder(protein: ProteinEntry): TSV = {
      import protein._
      Array("PROTEIN", score, rank, accession, protein_description,
            coverage, sequence, target_decoy)
    }


    /** TSV-to-Protein file decoder
     */

    def decoder(tsv: TSV, position: Int): ProteinEntry =
    {
      import TSVSupport.TSVCAST 

      ProteinEntry(
        id                  = position,
        score               = tsv.dbl(1), rank                = tsv.int(2),
        accession           = tsv.str(3), protein_description = tsv.str(4),
        coverage            = tsv.dbl(5), sequence            = tsv.str(6),
        target_decoy        = tsv.str(7)
      )
    }


    /** TSV database connectivity
     */

    val header = List(
      "#PROTEIN", "score", "rank", "accession",
      "protein_description", "coverage", "sequence", "target_decoy"
    ).mkString("\t")


    val dbc = {
      import FlexibleTSVdbc._
      val genHeader = (schema: SCHEMA, b: ProteinEntry) => header
      val guard = (line: String, l: Int, p: Int) => line startsWith "PROTEIN"
      val formatter = genFormatter(schema, Some(genHeader), encoder) 
      val parser    = genParser(schema, Some(guard), decoder)
      FlexibleTSVdbc(schema, formatter, parser)
    }


    /** Key for ProteinFile
     */

    val proteinFileKey: Key[ProteinEntry,ID] = Key(_.id, Ordering[Int])


    /** Constructors
     */

    def apply(filename: String): PROTEINFILE =
      dbc.connect(proteinFileKey, filename, TMP)

    def apply(filename: String, folder: String): PROTEINFILE =
      dbc.connect(proteinFileKey, filename, folder)


    /** Sometimes, it is handy to have a transient Protein file for
     *  temporary use without writing it to disk.
     */

    def transientProteinFile(entries: IterableOnce[ProteinEntry]): PROTEINFILE =
      OTransient[ProteinEntry,ID](proteinFileKey, CBI(entries),
                                  dbc.parser, dbc.formatter,
                                  OFile.destructorOFile _)
    

    def emptyProteinFile: PROTEINFILE = 
      OTransient[ProteinEntry,ID](proteinFileKey, CBI(),
                                  dbc.parser, dbc.formatter,
                                  OFile.destructorOFile _)

  }  // end object ProteinFile



/** Run information
 */

  type RUNFILE = RunFile

  case class RunFile(
    filename: String,
    run_id: String,
    score_type: String,
    score_direction: String,
    date_time: String,
    search_engine_version: String,
    parameters: String,
    protein_file: PROTEINFILE,
    peptide_file: PEPTIDEFILE)
  {
    var protect = true

    def protection(flag: Bool = true) = { protect = flag; this }

    def close(): Unit = {
      import dbmodel.DBModel.DEBUG.message
      import java.nio.file.{ Files, Paths }
      val err = "**** PEPTIDEModel.RunFile.close: Cannot delete " + filename
      if (!protect && filename != "")
        try { Files.deleteIfExists(Paths.get(filename)) }
        catch { case _: Throwable => message(err) }
    } 

    def toFile(filename: String, folder: String = TMP): String = 
      RunFile.toFile(this, filename, folder)
  }


  object RunFile
  {
    /** Constructors
     */

    def apply(filename: String): RUNFILE = RunFile(filename, TMP)

    def apply(filename: String, folder: String): RUNFILE =
    {
      // Assume filename is a Run file
      val proteinFile = ProteinFile(filename, folder)
      val peptideFile = PeptideFile(filename, folder)
      val fname = OFile.mkFileName(filename, folder).toString
      val file  = scala.io.Source.fromFile(fname)
      try {
        val line = file.getLines().find(_.startsWith("RUN"))
        line match {
          case None => 
            RunFile(filename = fname, run_id = "", score_type = "", 
                    score_direction = "", date_time = "",
                    search_engine_version = "", parameters = "",
                    protein_file = proteinFile, peptide_file = peptideFile)
          case Some(l) =>
            val en = l.trim.split("\t")
            RunFile(filename = fname, run_id = en(1), score_type = en(2), 
                    score_direction = en(3), date_time = en(4),
                    search_engine_version = en(5), parameters = en(6),
                    protein_file = proteinFile, peptide_file = peptideFile)
        }
      } finally file.close()
    }



    /** Formatter for Run file.
     */

    val header = List("#RUN", "run_id", "score_type", "score_directiom",
                      "date_time", "search_engine_version", "parameters" 
                 ).mkString("\t")


    def entry(b: RunFile) = {
      import b._
      List("RUN", s"$run_id", s"$score_type", s"$score_direction", 
           s"$date_time", s"$search_engine_version", s"$parameters"
      ).mkString("\t")
    }


    def toFile(
      run:      RunFile, 
      filename: String = "", 
      folder:   String = TMP): String =
    {
      import java.io.{ BufferedWriter, PrintWriter, File }

      val fname = OFile.mkFileName(filename, folder).toString
      val oos   = new BufferedWriter(new PrintWriter(new File(fname)))
      var n     = 1

      def println(s: String) = { oos.write(s); oos.write('\n') }

      // Writer headers
      println(header)
      println(ProteinFile.header)
      println(PeptideFile.header)

      // Write Run info
      println(entry(run))

      // Write protein info
      val proteins      = run.protein_file.cbi
      val formatProtein = ProteinFile.dbc.formatter(fname)
      n = 1
      while (proteins.hasNext) {
        println(formatProtein(proteins.next(), n))
        n = n + 1
        if (n % 10000 == 0) oos.flush()
      }
      oos.flush()
      proteins.close()

      // Write peptide info
      val peptides      = run.peptide_file.cbi
      val formatPeptide = PeptideFile.dbc.formatter(fname)
      n = 1
      while (peptides.hasNext) {
        println(formatPeptide(peptides.next(), n))
        n = n + 1
        if (n % 10000 == 0) oos.flush()
      }
      oos.flush()
      peptides.close()
      oos.close()

      // Return the filename
      return fname
    }


    def destructorRunFile(run: RunFile): Unit = run.close()

  }  // end object RunFile


  val implicits = dbmodel.DBFile.implicits

}  // end object PEPTIDEModel



/** Examples *****************************************************
 *
{{{

  import proteomics.PEPTIDEModel._
  import proteomics.PEPTIDEModel.implicits._


  val proteins = ProteinFile("DDA1.tsv")

  proteins(8)

  proteins(8).accession

  proteins.length

  val xx = proteins.filter(_.target_decoy == "target").saveAs("xx")

  xx(8).accession

  xx.length

  xx.protection(false).close()



  val peptides = PeptideFile("DDA1.tsv")

  peptides.length

  val yy = peptides.filter( _.hits.length > 2 ).saveAs("yy")

  yy(0).hits

  yy.length

  yy.protection(false).close()

  


  val run = RunFile("DDA1.tsv")

  run.run_id

  run.parameters

  run.peptide_file(7)

  run.protein_file(10)

  run.toFile("zz")

  val zz = RunFile("zz")

  zz.peptide_file.length

  zz.protection(false).close()


}}}
 *
 */



