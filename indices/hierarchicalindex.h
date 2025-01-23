/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   George Christodoulou
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

#ifndef _HIERARCHICALINDEX_H_
#define _HIERARCHICALINDEX_H_

#include "../def_global.h"
#include "../containers/relation.h"



// Framework
class HierarchicalIndex
{
protected:
    size_t numIndexedRecords;
    unsigned int numBits;
    unsigned int maxBits;
    unsigned int height;
    
    // Construction
    virtual inline void updateCounters(const Record &r) {};
    virtual inline void updatePartitions(const Record &r) {};

public:
    // Statistics
    size_t numPartitions;
    size_t numEmptyPartitions;
    float avgPartitionSize;
    size_t numOriginals, numReplicas;
    size_t numOriginalsIn, numOriginalsAft, numReplicasIn, numReplicasAft;


    // Construction
    HierarchicalIndex(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    virtual void print(const char c) {};
    virtual void getStats() {};
    virtual ~HierarchicalIndex() {};
    

    // Querying
    // HINT
    // Basic predicates of Allen's algebra
    virtual size_t execute_Equals(RangeQuery Q) {return 0;};
    virtual size_t execute_Starts(RangeQuery Q) {return 0;};
    virtual size_t execute_Started(RangeQuery Q) {return 0;};
    virtual size_t execute_Finishes(RangeQuery Q) {return 0;};
    virtual size_t execute_Finished(RangeQuery Q) {return 0;};
    virtual size_t execute_Meets(RangeQuery Q) {return 0;};
    virtual size_t execute_Met(RangeQuery Q) {return 0;};
    virtual size_t execute_Overlaps(RangeQuery Q) {return 0;};
    virtual size_t execute_Overlapped(RangeQuery Q) {return 0;};
    virtual size_t execute_Contains(RangeQuery Q) {return 0;};
    virtual size_t execute_Contained(RangeQuery Q) {return 0;};
    virtual size_t execute_Precedes(RangeQuery Q) {return 0;};
    virtual size_t execute_Preceded(RangeQuery Q) {return 0;};
    
    // Generalized predicate, ACM SIGMOD'22 gOverlaps
    virtual size_t execute_gOverlaps(StabbingQuery Q) {return 0;};
    virtual size_t execute_gOverlaps(RangeQuery Q) {return 0;};

    
    // HINT^m
    // Basic predicates of Allen's algebra
    virtual size_t executeBottomUp_Equals(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Starts(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Started(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Finishes(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Finished(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Meets(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Met(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Overlaps(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Overlapped(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Contains(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Contained(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Precedes(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_Preceded(RangeQuery Q) {return 0;};

    // Generalized predicate, ACM SIGMOD'22 gOverlaps
    virtual size_t executeTopDown_gOverlaps(StabbingQuery Q) {return 0;};
    virtual size_t executeTopDown_gOverlaps(RangeQuery Q) {return 0;};
    virtual size_t executeBottomUp_gOverlaps(StabbingQuery Q) {return 0;};
    virtual size_t executeBottomUp_gOverlaps(RangeQuery Q) {return 0;};
};
#endif // _HIERARCHICALINDEX_H_
