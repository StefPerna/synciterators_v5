

package proteomics


//
// ProInfer
// Reimplemented from the original Python codes of Peng Hui.
//
// Wong Limsoon
// 3 March 2022



object PROINFER
{

  import dbmodel.DBModel.RESOURCE
  import dbmodel.DBModel.OrderedCollection.Key
  import dbmodel.DBModel.Join._
  import dbmodel.DBModel.OpG.{ sum, prod, average, smallest }
  import dbmodel.DBModel.Predicates.{ eq => EQ }
  import dbmodel.DBFile.OFile
  import dbmodel.TSVModel.REMYFile.{ Remy, RemyFile, transientRemyFile }
  import proteomics.FASTAModel.{ FASTAFile, FASTAFILE }
  import proteomics.PEPTIDEModel.{ PeptideFile, PEPTIDEFILE }
  import proteomics.CORUMModel.{ CorumFile, CORUMFILE }



  //
  // Default values
  //


  var REFPROTEINS  = "Human_database_including_decoys_(cRAP_added).fasta"
  var THRESHOLD    = 0.999
  var REFCOMPLEXES = "allComplexes.txt"
  var ORGANISM     = "Human"

  var INPUT        = "DDA1.tsv"
  var OUTPUT       = "DDA1-called-proteins.tsv"
  var QVAL         = 0.01



  //
  // Use named type for readability
  //


  type PROTEIN = String
  type PEPTIDE = String
  type CPXID   = Int
  type CONFrev = Double



  //
  // Remy record Hit(pep, pro, score, falseProb)
  // represents a PSM
  //


  type HIT        = Remy
  type HITFILE[K] = OFile[HIT,K]

  val HitS = Vector("pep" -> "String", "pro" -> "String",
                    "score" -> "Double", "falseProb" -> "String")

  def Hit(pep: PEPTIDE, pro: PROTEIN, score: Double, falseProb: Double = 1.0) = 
    Remy(HitS, Array(pep, pro, score, falseProb))



  //
  // Remy record Call(pro, accPEP, conf, label, fdr, qval) 
  // represents a call on a protein
  //


  type CALL        = Remy
  type CALLFILE[K] = OFile[CALL,K]

  val CallS  = Vector(
                  "pro"  -> "String", "accPEP" -> "Double",
                  "conf" -> "Double", "label"  -> "Int",
                  "fdr"  -> "Double", "qval"   -> "Double")

  def Call(pro: PROTEIN, 
           accPEP: Double,
           conf:   Double,
           label:  Int,
           fdr:    Double = 1.0,
           qval:   Double = 1.0) =
  {
    Remy(CallS, Array(pro, accPEP, conf, label, fdr, qval))
  }



  //
  // Remy record Cpx(cpxId, pro, targetProb, decoyProb) 
  // represents a protein complex and its member proteins.
  // I.e., Cpx(x,y) iff complex x has protein y as a member protein.
  //


  type CPX        = Remy
  type CPXFILE[K] = OFile[CPX,K]

  val CpxS = Vector(
               "cpxId" -> "Int", "pro" -> "String",
               "targetProb" -> "Double", "decoyProb" -> "Double")

  def Cpx(cpxId:      Int,
          pro:        String, 
          targetProb: Double  = 1.0, 
          decoyProb:  Double  = 1.0) =
  { 
    Remy(CpxS, Array(cpxId, pro, targetProb, decoyProb))
  }




  //
  // Define typed projections to access the fields of a PSM
  //


  implicit class RemyProjections(hit: HIT) {
    def pep: String       = hit.str("pep")
    def pro: String       = hit.str("pro")
    def score: Double     = hit.dbl("score")
    def factor: Double    = hit.dbl("factor")
    def falseProb: Double = hit.dbl("falseProb")
    def accPEP: Double    = hit.dbl("accPEP")
    def conf: Double      = hit.dbl("conf")
    def label: Int        = hit.int("label")
    def fdr: Double       = hit.dbl("fdr")
    def qval: Double      = hit.dbl("qval")
    def cpxId: Int        = hit.int("cpxId")
    def targetProb: Double= hit.dbl("targetProb")
    def decoyProb: Double = hit.dbl("decoyProb")

    def uniprot: String = {
      // Deal with heterogeneous naming formats of Uniprot ID.
      val u = pro
      val p = u.split('|')
      if (p.length > 1) p(1) else u
    } 
  }




  //
  // Allow hits/PSM file to be sorted by pep and/or by pro
  // Allow Call file to be sorted by conf, in descending order.
  // Allow Complex file to be sorted by cpxId or pro.
  //


  val kPep     = Key.asc[HIT,String](_.pep)   
  val kPro     = Key.asc[HIT,String](_.pro)
  val kPepPro  = Key.asc[HIT,(String,String)](h => (h.pep, h.pro))
  val kConfrev = Key.dsc[CALL,Double](_.conf)
  val kCpxId   = Key.asc[CPX,Int](_.cpxId)
  val kUP      = Key.asc[HIT,String](_.uniprot)




  //
  // PSMs & protein calls are mapped to an on-disk TSV file, 
  // via Remy database connectivity encoder/decoder.
  //


  implicit class CBItoHitFile(hits: IterableOnce[HIT]) {
    def kPepFile     = transientRemyFile(hits, kPep)
    def kProFile     = transientRemyFile(hits, kPro)
    def kUPFile      = transientRemyFile(hits, kUP)
    def kPepProFile  = transientRemyFile(hits, kPepPro)
    def kConfrevFile = transientRemyFile(hits, kConfrev)
    def kCpxIdFile   = transientRemyFile(hits, kCpxId)
  }


  implicit class OFileToHitFile(hits: OFile[HIT,_])
  extends CBItoHitFile(hits.cbi)





  //
  // Reference proteins and decoys
  //


  case class ProteinDB(filename: String = REFPROTEINS)
  extends RESOURCE
  {
    // Read reference proteins and decoys
    // Extract protein name, length.

    val proteins    = FASTAFile(filename)

    val proteinLen = {
      proteins
      .map(p => (p.name.split(" ")(0) -> p.seq.length))
      .toMap
    }

    // Endow proteins with len methods

    implicit class Protein(pro: PROTEIN) {
      def len: Int = proteinLen(pro)
    }

    // Initialization codes

    use(proteins)

  }




  //
  // Reference protein complexes
  //


  case class ComplexDB(
    filename: String = REFCOMPLEXES,
    organism: String = ORGANISM)
  extends RESOURCE
  {
    // Read reference complexes

    val complexes = CorumFile(filename)

    // Extract protein-complex associations

    val cpxPro  = {
      complexes
      .filter(_.organism == organism)
      .flatMap(c => c.subunitsUniprot.map(p => Cpx(c.complexID, p)))
      .kUPFile
      .ordered
      .materialized
    }  
  }




  //
  // Select peptides with score below a threshold.
  // For (pep, pro) matching a few times, keep the lowest-score one.
  //


  implicit class SelectHits(peptides: PEPTIDEFILE)
  {
    def selected(threshold: Double = THRESHOLD): HITFILE[PEPTIDE] =
    {
      peptides
      .filter(_.score <= threshold)
      .flatMap(p => p.hits.map(h => Hit(p.seq, h.accession, p.score)))
      .kPepProFile
      .ordered
      .clustered
      .map( { case ((pep, pro), ps) => Hit(pep, pro, ps.map(_.score).min) } )
      .kPepFile
      .serialized.use(peptides)
    }
  }




  //
  // Compute false prob to be used for each PSM, i.e., the prob 
  // that this PSM is not a hit of the peptide to the protein.
  //


  implicit class NormalizeHits(hits: HITFILE[PEPTIDE])
  {
    def normalized(proteinDB: ProteinDB): HITFILE[PEPTIDE] =
    {
      import proteinDB._

      def normalizeHits(hs: List[HIT]): List[HIT] = 
      {
        def weight(h: HIT) = 1.0 / h.pro.len.toDouble
        val totalWt        = sum(weight(_))(hs)
        def factor(h: HIT) = weight(h) / totalWt
        def falseProb(h: HIT) = 1.0 - ((1.0 - h.score) * factor(h)) 

        hs.map(h => Hit(h.pep, h.pro, h.score, falseProb(h)))
      }

      hits
      .kPepFile    // Assume hits are already sorted on kPep
      .clustered
      .flatMap({ case (p, hs) => normalizeHits(hs) })
      .kPepFile
      .serialized.use(hits)
    }
  }




  //
  // Compute false reporting prob for proteins
  // Sorted in descending order of their conf values.
  //


  implicit class ScoreProteins(hits: HITFILE[PEPTIDE])
  {
    def scoredProteins: CALLFILE[CONFrev] =
    {
      def confScore(p: PROTEIN, hs: List[HIT]): CALL = {
        import scala.math.log10
        val accPEP = prod[HIT,Double](_.falseProb)(hs)
        val conf   = -10 * log10(accPEP + 1E-14)
        val label  = if (p startsWith "DECOY") -1 else 1
        Call(p, accPEP, conf, label)
      }

      hits
      .orderedBy(kPro)
      .clustered
      .map({ case (p, hs) => confScore(p, hs) })
      .kConfrevFile
      .ordered
      .serialized.use(hits)
    }
  }




  //
  // Compute local FDR. The local FDR of a protein with conf = s is
  // computed as (1 + # decoy with conf >= s)/(1 + # target with conf >= s)
  // Sorted in ascending order of their FDR values.
  // NB. This defn of FDR is not the usual statistical FDR; but it is
  // used widely in proteomics, e.g., EPIFANY and Fido on the openMS platform.
  //


  implicit class ComputeFDR(calls: CALLFILE[CONFrev])
  {
    def fdrComputed: CALLFILE[CONFrev] =
    {
      var ndecoy  = 0
      var ntarget = 0
  
      def lfdr(sc: (Double,List[CALL])): List[CALL] = {
        val (s, calls) = sc
        val decoys     = calls.filter(_.label == -1).length
        val targets    = calls.filter(_.label == 1).length
        ndecoy         = ndecoy + decoys
        ntarget        = ntarget + targets
        val fdr        = (1.0 + ndecoy.toDouble) / (1.0 + ntarget.toDouble)
        calls.map(c => Call(c.pro, c.accPEP, c.conf, c.label, fdr))
      }
  
      calls
      .kConfrevFile  // assume protein calls in descending order of conf values
      .clustered
      .flatMap(lfdr(_))
      .kConfrevFile
      .serialized.use(calls)
    }
  }




  //
  // Compute Q value. Q value of a protein X is the best FDR among
  // proteins whose conf value is no better than X's. 
  //


  implicit class ComputeQVAL(calls: CALLFILE[CONFrev])
  {
    def qvalComputed: CALLFILE[CONFrev] =
    {
      var qval = 1.0

      calls
      .kConfrevFile // assume protein calls in descending order of conf values
      .reversed     // make protein calls in ascending order of conf values.
      .useOnce      // make this use-once to ensure it is garbage collected.
      .map(c => {
          qval = qval min c.fdr
          Call(c.pro, c.accPEP, c.conf, c.label, c.fdr, qval) })
      .kConfrevFile
      .reversed     // put protein calls into descending order of conf values.
      .serialized.use(calls)
    }
  }




  //
  // Compute prob of a protein complex being present.
  // This is computed as the mean accPEP of its protein members.
  // Smaller better.
  //


  implicit class AnnotateComplex(cpxDB: ComplexDB)
  {
    def cpxAnnotated(calls: CALLFILE[CONFrev]): CPXFILE[CPXID] =
    {
      import cpxDB._
 
      val saved = calls.serialized
      val targets = saved.filter(_.label == 1).kUPFile.ordered
      val decoys  = saved.filter(_.label == -1).kUPFile.ordered

      val canSee  = (y: String, x: String) => y == x
      val targetSI = targets.siterator(kUP, canSee)
      val decoySI = decoys.siterator(kUP, canSee)

      val annotated =
        for (cp <- cpxPro)
        yield {
          val ts = targetSI.syncedWith(cp);
          val ds = decoySI.syncedWith(cp);
          val tprob = if (ts.isEmpty) 1.0 else ts.map(_.accPEP).min
          val dprob = if (ds.isEmpty) 1.0 else ds.map(_.accPEP).min
          Cpx(cp.cpxId, cp.pro, tprob, dprob)
        }

//      val annotated =
//        for ((cp, ts, ds) <- cpxPro join targets on EQ
//                                    join decoys  on EQ)
//        yield {
//          val targetProb = if (ts.isEmpty) 1.0 else ts.map(_.accPEP).min
//          val decoyProb  = if (ds.isEmpty) 1.0 else ds.map(_.accPEP).min
//          Cpx(cp.cpxId, cp.pro, targetProb, decoyProb)
//        }

      try annotated
          .kCpxIdFile
          .ordered
          .useOnce
          .clustered
          .flatMap({ case (cpxId, ps) => 
             val pt = ps.filter(_.targetProb < 1.0)
             val pd = ps.filter(_.decoyProb < 1.0)
             val t = if (pt.length == 0) 1.0 else average[CPX](_.targetProb)(pt)
             val d = if (pd.length == 0) 1.0 else average[CPX](_.decoyProb)(pd)
             ps.map(p => Cpx(cpxId, p.pro, t, d)) }) 
          .kCpxIdFile
          .serialized.use(calls)
      finally { 
          annotated.close();
          targetSI.close(); decoySI.close();
          targets.close(); decoys.close(); saved.close()
      }
    }
  }  




  //
  // Update prob of a protein present
  // as     min(p.accPEP, min(c.targetProb | p is in complex c))
  // and as min(p.accPEP, min(c.decoyProb  | p is in complex c))
  //  


  implicit class ComputeQVALcpx(calls: CALLFILE[CONFrev])
  {
    def cpxComputed(complexDB: ComplexDB): CALLFILE[CONFrev] =
    {
      import scala.math.log10

      val saved = calls.serialized
      val qs = saved.kUPFile.ordered
      val cs = complexDB.cpxAnnotated(saved).useOnce.kUPFile.ordered
      val canSee  = (y: String, x: String) => y == x 
      val cSI = cs.siterator(kUP, canSee)

      def cProb(l: Int) = (c: CPX) => if (l== 1) c.targetProb else c.decoyProb 

      val scored =
        for (q <- qs)
        yield {
          val cps   = cSI.syncedWith(q)
          val cPEP  = if (cps.length == 0) q.accPEP
                      else smallest(cProb(q.label))(cps)
          val cConf = -10 * log10(cPEP + 1E-14)
          val pep   = if (q.accPEP >= cPEP) cPEP else q.accPEP
          val conf  = if (q.accPEP >= cPEP) cConf else q.conf
          Call(q.pro, pep, conf, q.label)
        }

//      val scored =
//        for ((q, cps) <- qs join cs on EQ)
//        yield {
//          def cProb(cp: CPX) = if (q.label == 1) cp.targetProb
//                               else cp.decoyProb 
//
//          val cPEP  = if (cps.length==0) q.accPEP else smallest(cProb(_))(cps)
//          val cConf = -10 * log10(cPEP + 1E-14)
//          val pep   = if (q.accPEP >= cPEP) cPEP else q.accPEP
//          val conf  = if (q.accPEP >= cPEP) cConf else q.conf
//          Call(q.pro, pep, conf, q.label)
//        }

      try scored
          .kConfrevFile
          .ordered
          .fdrComputed
          .qvalComputed
          .serialized.use(calls)
      finally { cSI.close(); cs.close(); qs.close(); saved.close() }
    }
  }





  //
  // ProInfer
  //


  case class ProInfer(proteinDB: ProteinDB, complexDB: ComplexDB)
  {
    // Without using protein complex info

    def runNoCpx(
      peptides:  PEPTIDEFILE,
      threshold: Double      = THRESHOLD): CALLFILE[CONFrev] =
    {
      peptides
      .selected(threshold)
      .normalized(proteinDB)
      .scoredProteins
      .fdrComputed
      .qvalComputed
    }


    // Use protein complex info to refine protein calls

    def runCpx(
      peptides:  PEPTIDEFILE,
      threshold: Double      = THRESHOLD): CALLFILE[CONFrev] =
    {
      runNoCpx(peptides, threshold)
      .cpxComputed(complexDB)
    }


    // Iterate runCpx until too few extra proteins are called.

    def iterateRunCpx(
      peptides:      PEPTIDEFILE,
      prefix:        String      = "run-",
      threshold:     Double      = THRESHOLD,
      qvalThreshold: Double      = QVAL): CALLFILE[CONFrev] =
    {

      def check(rno: Int, runA: CALLFILE[CONFrev], runB: CALLFILE[CONFrev]) = {
        val a = runA.filter(p => p.label == 1 && p.qval < qvalThreshold)
        val b = runB.filter(p => p.label == 1 && p.qval < qvalThreshold)
        val aLen = a.done { _.length }
        val bLen = b.done { _.length }
println(s"* rno = $rno * aLen = $aLen * bLen = $bLen * diff = ${bLen - aLen}")
        (bLen - aLen) < 10
      }

      var finished = false
      var rno = 0
      var runA = runNoCpx(peptides, threshold).saveAs(s"${prefix}-${rno}")

      while (!finished) {
        rno = rno + 1
        val runB = runA.cpxComputed(complexDB).saveAs(s"${prefix}-${rno}")
        finished = check(rno, runA, runB)
        runA = runB
      }

      runA       
    }
  }



  object ProInfer
  {
    //
    // Call proteins in a PSM file (peptideFile), 
    // using a reference protein/decoy database (proteinFile)
    // and a reference protein complex database (complexFile).
    //


    def apply(
      peptideFile:   String = INPUT, 
      outFile:       String = OUTPUT,
      proteinFile:   String = REFPROTEINS, 
      threshold:     Double = THRESHOLD,
      complexFile:   String = REFCOMPLEXES,
      organism:      String = ORGANISM,
      prefix:        String = "run-",
      qvalThreshold: Double = QVAL): CALLFILE[CONFrev] =
    {
      val proteins  = ProteinDB(proteinFile)
      val peptides  = PeptideFile(peptideFile)
      val complexes = ComplexDB(complexFile, organism)

      try ProInfer(proteins, complexes)
          .iterateRunCpx(peptides, prefix, threshold, qvalThreshold)
          .saveAs(outFile)
      finally { proteins.close(); peptides.close(); complexes.close() }
    }

    

    def runCpx(
      peptideFile: String = INPUT, 
      outFile:     String = OUTPUT,
      proteinFile: String = REFPROTEINS, 
      threshold:   Double = THRESHOLD,
      complexFile: String = REFCOMPLEXES,
      organism:    String = ORGANISM): CALLFILE[CONFrev] =
    {
      val proteins  = ProteinDB(proteinFile)
      val peptides  = PeptideFile(peptideFile)
      val complexes = ComplexDB(complexFile, organism)

      try ProInfer(proteins, complexes)
          .runCpx(peptides, threshold)
          .saveAs(outFile)
      finally { proteins.close(); peptides.close(); complexes.close() }
    }


    //
    // Call proteins in a PSM file (peptideFile), 
    // using a reference protein/decoy database (proteinFile).
    // Not using a reference protein complex database (complexFile).
    //


    def runNoCpx(
      peptideFile: String      = INPUT, 
      outFile:     String      = OUTPUT,
      proteinFile: String      = REFPROTEINS, 
      threshold:   Double      = THRESHOLD): CALLFILE[CONFrev] =
    {
      val proteins = ProteinDB(proteinFile)
      val peptides = PeptideFile(peptideFile)

      try ProInfer(proteins, null)
          .runNoCpx(peptides, threshold)
          .saveAs(outFile)
      finally { proteins.close(); peptides.close() }
    }



    // Analysis of runCpx vs runNoCpx

    def analysis(
      cpxFile:       String,        // results of runCpx 
      nocpxFile:     String,        // results of runNoCpx
      prefix:        String,        // prefix for saving analysis results
      qvalThreshold: Double) =
    {
      val cpx = {
        RemyFile(cpxFile)
        .filter(p => p.label == 1 && p.qval < qvalThreshold)
        .kProFile
        .ordered
        .saveAs(s"${prefix}-cpx.result")
      }

      val nocpx = { 
        RemyFile(nocpxFile)
        .filter(p => p.label == 1 && p.qval < qvalThreshold)
        .kProFile
        .ordered
        .saveAs(s"${prefix}-nocpx.result")
      }

      val cpxOnly   = {
        val tmp = for ((c, ns) <- cpx join nocpx on EQ; if ns.isEmpty) yield c
        tmp.kProFile.saveAs(s"${prefix}-cpx-only.result")
      }

      val nocpxOnly = {
        val tmp = for ((n, cs) <- nocpx join cpx on EQ; if cs.isEmpty) yield n
        tmp.kProFile.saveAs(s"${prefix}-nocpx-only.result")
      }

      println("")
      println(s"***        cpx: ${cpx.done { _.length }}")
      println(s"***      nocpx: ${nocpx.done { _.length }}")
      println(s"***   cpx excl: ${cpxOnly.done { _.length }}")
      println(s"*** nocpx excl: ${nocpxOnly.done { _.length }}")
    } 

  }

}



