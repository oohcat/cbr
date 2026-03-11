# The Universal Quotient of Banking APIs

This repository contains the reproducible measurement pipeline and empirical
datasets for the ACT 2026 conference submission. The title is The 
Empirical Universal Quotient of Banking APIs: Empirical Category Theory and
Complexity Collapse.

## Overview

The pipeline computes the dimensional rank of the global banking API manifold. 
It extracts semantic signals from four independently developed regulatory and
institutional standards. It applies a strict pattern matching decomposition to 
project these signals onto a candidate semantic basis. It constructs a binary 
activation matrix over GF(2). It calculates the exact rank via Gaussian elimination.
Its results are output in the cat_banking_activation_detail_bian_12_v4_4.tsv file.

The pipeline processes the following corpora.
* OBIE v3.1 UK Open Banking
* AU CDR v1.28 Australian Consumer Data Right
* PSD2 NextGenPSD2 v1.3.16 Berlin Group
* BIAN Service Landscape v12.0 Wholesale Institutional

## Prerequisites

The pipeline requires Python 3.8 or higher. The execution environment requires the 
following dependencies.

```bash
pip install requests pyyaml numpy
