
=================================================

Synchrony Iterators

Wong Limsoon
22/3/2021

=================================================


These set of files implements Synchrony iterators and 
also uses Synchrony iterators to emulate the
GenoMetric Query Language (GMQL).

The files are organized into the following folders:

1/ "iterators" folder. The files in this folder are: 

- synciterators - This is the implementation of Synchrony iterators.

- aggriterators - This file provides various aggregate functions.
                  
- fileiterators - This implements the EFile class. EFile is used
                  for representing objects on disk. 

- serializers   - This provides the mechanisms used by EFile to map
                  on-disk objects to in-memory objects, and vice
                  versa as needed.

2/ "genomeannot" folder.  The files in this folder are:

- genomelocus   - This file defines a class for representing genomic loci.
                  It also implements many topological predicates on
                  genomic loci.

- simplebed     - This file provides the in-memory representation of
                  BED file. Functions for serializig and deserializing
                  BED files are provided as well. I.e. EFile[Bed].

- sample        - This file provides the in-memory representation of
                  samples.  Functions for serializig and deserializing
                  sample files are provided as well. I.e. EFile[Sample].
-

3/ "gmql-sequential" folder.  The two main files in this folder are:

- bedfileops    - GMQL operators on EFile[Bed].
- samplefileops - GMQL operators on EFile[Sample].
                  These pair of files provides a sequential emulation
                  of GMQL query operators. 

- encode        - This file provides functions for ENCODE formats.


4/ "gmql-parallel" folder.  The two main files in this folder are:

- execution-contexts - Provides thread pool managers for running
                       the parallel emulation of GMQL operators.

- samplefileparops   - This is the parallel counter part of
                       samplefileops. 


