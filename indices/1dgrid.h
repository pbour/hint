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

#ifndef _1D_GRID_H_
#define _1D_GRID_H_

#include "../def_global.h"
#include "../containers/relation.h"



class OneDimensionalGrid
{
private:
    PartitionId numPartitions;
    PartitionId numPartitionsMinus1;
    Timestamp gstart;
    Timestamp gend;
    Timestamp partitionExtent;
    RecordId numIndexedRecords;

    Relation *pRecs;
    size_t *pRecs_sizes;

    // Construction
    inline void updateCounters(const Record &rec);
    inline void updatePartitions(const Record &rec);

public:
    // Statistics
    PartitionId numEmptyPartitions;
    float avgPartitionSize;
    size_t numReplicas;
    
    // Construction
    OneDimensionalGrid(const Relation &R, const PartitionId numPartitions);
    void print(char c);
    void getStats();
    ~OneDimensionalGrid();

    // Querying
    // Basic predicates of Allen's algebra
    size_t execute_Equals(RangeQuery Q);
    size_t execute_Starts(RangeQuery Q);
    size_t execute_Started(RangeQuery Q);
    size_t execute_Finishes(RangeQuery Q);
    size_t execute_Finished(RangeQuery Q);
    size_t execute_Meets(RangeQuery Q);
    size_t execute_Met(RangeQuery Q);
    size_t execute_Overlaps(RangeQuery Q);
    size_t execute_Overlapped(RangeQuery Q);
    size_t execute_Contains(RangeQuery Q);
    size_t execute_Contained(RangeQuery Q);
    size_t execute_Precedes(RangeQuery Q);
    size_t execute_Preceded(RangeQuery Q);

    // Generalized predicate, ACM SIGMOD'22 gOverlaps
    size_t execute_gOverlaps(StabbingQuery Q);
    size_t execute_gOverlaps(RangeQuery Q);
};
#endif // _1D_GRID_H_
