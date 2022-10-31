/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 ******************************************************************************
 * Copyright (c) 2020 - 2022
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

#ifndef _HINT_H_
#define _HINT_H_

#include "../def_global.h"
#include "../containers/relation.h"
#include "../indices/hierarchicalindex.h"



// Base comparison-free HINT, no optimizations activated
class HINT : public HierarchicalIndex
{
private:
    RelationId **pOrgs, **pReps;
    RecordId   **pOrgs_sizes;
    size_t     **pReps_sizes;

    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);

public:
    // Construction
    HINT(const Relation &R, const unsigned int maxBits);
    void print(const char c);
    void getStats();
    ~HINT();
    
    // Querying
    size_t execute_gOverlaps(StabbingQuery Q);
    size_t execute_gOverlaps(RangeQuery Q);
};



// Comparison-free HINT with the skewness & sparsity optimization activated
class HINT_SS : public HierarchicalIndex
{
private:
    RelationId *pOrgs, *pReps;
    RecordId   **pOrgs_sizes;
    size_t     **pReps_sizes;
    RecordId   **pOrgs_offsets;
    size_t     **pReps_offsets;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> > *pOrgs_ioffsets, *pReps_ioffsets;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
    // Querying
    // Auxiliary functions to scan a partition.
    inline void scanPartitions_Orgs_gOverlaps(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanPartition_Reps_gOverlaps(unsigned int level, Timestamp t, PartitionId &next_from, size_t &result);
    
public:
    // Construction
    HINT_SS(const Relation &R, const unsigned int maxBits);
    void getStats();
    ~HINT_SS();
    
    // Querying
    // Basic predicates of Allen's algebra

    // Generalized predicates, ACM SIGMOD'22 gOverlaps
    size_t execute_gOverlaps(StabbingQuery Q);
    size_t execute_gOverlaps(RangeQuery Q);
};
#endif // _HINT_H_
