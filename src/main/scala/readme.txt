
**** IMPORT NOTE ****


Depending on which version of Scala you have, some parallel operations
may hang Scala, esp. the REPL.  This is apparent due to Scala's default
lazy initialization of static functions and objects.  This can usually 
be solved using this Scala REPL option:

scala -Yrepl-class-based

Consult https://github.com/scala/scala-parallel-collections/issues/34
for a discussion.




**** EXPLANATIONS OF THE FILES ****


The first two files (dbmodel-v9.1.scala, dbfile-v9.1.scala) are for general use.


dbmodel-v9.1.scala  (cloc: 993 lines)

	This file provides all the basic infrastructure for ordered collections
	and Synchrony iterator. An ordered collection is any collection with
	an ordering key. An ordered collection is endowed with a method for
	creating a Synchrony iterator on itself. The Synchrony iterator enables
	this collection to be iterated in a manner that is synchronized to
	the iteration on another collection. This file also provides other
	bells and whistles such as (i) a database-like join operator to be
	used with comprehension syntax, (ii) a library of useful non-equijoin
	predicates, and (iii) a library of aggregate functions.
	Useful examples are provided at end of the file.


dbfile-v9.2.scala  (cloc: 350 lines)

	This file builds on dbmodel-v9.scala to provide a framework for
	turning text files into ordered collections. Users can provide some
	parser/unparser for their text files. Thereafter, a text file can
	be used like a collection, with its items being move into memory or
	written to disk transparent as needed. All operations available on
	ordered collections are available, including Synchrony iterator.
	Useful examples are provided at end of the file.



Next three files (genomelocus-v5.scala, simplebed-v5.scala, bedfileops-v5.scala)
provide the parser/unparser for BED-formatted files (which are widely used in
bioinformatics), as well as powerful operations for manipulating BED files,
similar to those provided by the popular bedtools package and the GMQL system.


genomelocus-v5.1.scala  (cloc: 159 lines)

	This file defines the GenomeLocus.Locus data type that represents
	loci information on a genome. 


simplebed-v5.1.scala  (cloc: 266 lines)

	This file provides parser/unparser for BED files, as well as a
	BED data type for representing BED file entries. Unlike plain
	BED files where the data fields are accessed by positions, 
	named fields are supported in this implementation.

bedfileops-v5.1.scala  (cloc: 571 lines)

	This file provides high-level methods for manipulating BED files.
	The methods are implemented using Synchrony iterators. These
	methods are able to expressed easily operations in the popular
	bedtools package and also those in GMQL.  Usage examples are
	provided at end of the file.



Next two files (sample-v5.scala, samplefileops-v5.scala) provide 
parser/unparser for Sample files and methods for emulating GMQL-like
queries on Sample files. A Sample file is basically a collection of BED
files along with metadata of each of these BED files.


sample-v5.1.scala  (cloc: 258 lines)
	
	This file provides the parser/unparser for Sample file, as well as
	the Sample data type that represents entries of Sample file. It
	supports the Sample file format of GMQL, as well as a simpler
	Sample file format designed by Limsoon.

samplefileops-v5.1.scala  (cloc: 339 lines)

	This file implements GMQL-like methods for manipulating Sample files
	and their component BED files. Sequential and sample-parallel query
	modes are both supported. Usage examples are provided at end of
        the file.

 

Last set of files contain example GMQL queries on sample files. The queries
are in the file mm-test.scala. The data are in the test folder.


Wong Limsoon
10 March 2022




**** UPDATE, SUPPORT FOR PROINFER, 10 MARCH 2022 ****

In this update, some files for supporting proteomics protein calling
are added. In particular, the ProInfer method originally developed by
PENG Hui is implemented. 

 
dbtsv-v1.scala  (cloc: 274 lines)

	This file builds on dbfile-v9.scala to provide "database connectivity"
	to TSV files (i.e. tsb-delimited files). It provides a simple
	framework for encoding/decoding user-defined data types into TSV
	files. It also provide Remy-style record types with named-field
	access.


dbpeptides-v1.1.scala  (cloc: 281 lines)

	This file implements the main input file type to ProInfer. 
	It captures PSM (protein-sequence matches) from proteomics
	mass-spec runs.  It builds on dbtsv-v1.scala.


dbfasta-v1.scala  (cloc: 199 lines)

	This file implements the FASTA file format that ProInfer uses
	for its reference proteins and decoys. It builds on dbtsv-v1.scala.


dbcorum-v1.1.scala  (cloc: 164 lines)

	This file implements the CORUM file format that ProInfer uses
	for its reference protein complexes. It builds on dbtsv-v1.scala.

proinfer-v2.1.scala  (cloc: 420 lines)

	This file implements ProInfer. (* Thanks, PENG Hui, for providing
	the original Python codes of ProInfer and explaining many details
	and testing this re-implementation. *)



Wong Limsoon
18 Mar 2022


