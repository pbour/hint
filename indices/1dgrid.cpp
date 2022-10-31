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

#include "1dgrid.h"



inline void OneDimensionalGrid::updateCounters(const Record &rec)
{
    auto sP = (rec.start == this->gend) ? this->numPartitionsMinus1 : rec.start/this->partitionExtent;
    auto eP = (rec.end   == this->gend) ? this->numPartitionsMinus1 : rec.end/this->partitionExtent;
    
    this->pRecs_sizes[sP]++;
    while (sP != eP)
    {
        sP++;
        this->pRecs_sizes[sP]++;
    }
}


inline void OneDimensionalGrid::updatePartitions(const Record &rec)
{
    auto sP = (rec.start == this->gend) ? this->numPartitionsMinus1 : rec.start/this->partitionExtent;
    auto eP = (rec.end   == this->gend) ? this->numPartitionsMinus1 : rec.end/this->partitionExtent;
    
    this->pRecs[sP][this->pRecs_sizes[sP]] = rec;
    this->pRecs_sizes[sP]++;
    while (sP != eP)
    {
        sP++;
        this->pRecs[sP][this->pRecs_sizes[sP]] = rec;
        this->pRecs_sizes[sP]++;
    }
}


OneDimensionalGrid::OneDimensionalGrid(const Relation &R, const PartitionId numPartitions)
{
    // Initialize statistics.
    this->numIndexedRecords   = R.size();
    this->numPartitions       = numPartitions;
    this->numPartitionsMinus1 = this->numPartitions-1;
    this->numEmptyPartitions  = 0;
    this->avgPartitionSize    = 0;
    this->gstart              = R.gstart;
    this->gend                = R.gend;
    this->partitionExtent     = (Timestamp)ceil((double)(this->gend-this->gstart)/this->numPartitions);
    this->numReplicas = 0;

    // Step 1: one pass to count the contents inside each partition.
    this->pRecs_sizes = (size_t*)calloc(this->numPartitions, sizeof(size_t));
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    // Step 2: allocate necessary memory.
    this->pRecs = new Relation[this->numPartitions];
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        this->pRecs[pId].gstart = this->gstart            + pId*this->partitionExtent;
        this->pRecs[pId].gend   = this->pRecs[pId].gstart + this->partitionExtent;
        this->pRecs[pId].resize(this->pRecs_sizes[pId]);
    }
    memset(this->pRecs_sizes, 0, this->numPartitions*sizeof(size_t));
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    // Free auxiliary memory.
    free(pRecs_sizes);
}


void OneDimensionalGrid::print(char c)
{
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        Relation &p = this->pRecs[pId];
        
        cout << "Partition " << pId << " [" << p.gstart << ".." << p.gend << "] (" << p.size() << "):";
        for (size_t i = 0; i < p.size(); i++)
            cout << " r" << p[i].id;
        cout << endl;
    }
}


OneDimensionalGrid::~OneDimensionalGrid()
{
    delete[] this->pRecs;
}


void OneDimensionalGrid::getStats()
{
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        this->numReplicas += this->pRecs[pId].size();

        if (this->pRecs[pId].empty())
            this->numEmptyPartitions++;
    }
    
    this->avgPartitionSize = (float)(this->numReplicas)/this->numPartitions;
    this->numReplicas -= this->numIndexedRecords;
}


size_t OneDimensionalGrid::execute_Equals(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    
    // Handle the first partition.
    return this->pRecs[s_pId].execute_Equals(Q);
}


size_t OneDimensionalGrid::execute_Starts(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    
    // Handle the first partition.
    return this->pRecs[s_pId].execute_Starts(Q);
}


size_t OneDimensionalGrid::execute_Started(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    
    // Handle the first partition.
    return this->pRecs[s_pId].execute_Started(Q);
}


size_t OneDimensionalGrid::execute_Finishes(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto e_pId = (Q.end == this->gend)? this->numPartitionsMinus1: Q.end/this->partitionExtent;

    // Handle the last partition.
    return this->pRecs[e_pId].execute_Finishes(Q);
}


size_t OneDimensionalGrid::execute_Finished(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto e_pId = (Q.end == this->gend)? this->numPartitionsMinus1: Q.end/this->partitionExtent;

    // Handle the last partition.
    return this->pRecs[e_pId].execute_Finished(Q);
}


size_t OneDimensionalGrid::execute_Meets(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto e_pId = (Q.end == this->gend)? this->numPartitionsMinus1: Q.end/this->partitionExtent;

    // Handle the last partition.
    return this->pRecs[e_pId].execute_Meets(Q);
}


size_t OneDimensionalGrid::execute_Met(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    
    // Handle the first partition.
    return this->pRecs[s_pId].execute_Met(Q);
}


size_t OneDimensionalGrid::execute_Overlaps(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    auto e_pId = (Q.end   == this->gend)? this->numPartitionsMinus1: Q.end/this->partitionExtent;
    
    
    // Handle the first partition.
    iterStart = this->pRecs[s_pId].begin();
    iterEnd   = this->pRecs[s_pId].end();
    for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
    {
        if ((iter->start < Q.end) && (Q.start < iter->end) && (Q.start < iter->start) && (Q.end < iter->end))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }

    // Handle partitions completely contained inside the query range.
    for (auto pId = s_pId+1; pId < e_pId; pId++)
    {
        Relation &p = this->pRecs[pId];
        iterStart = p.begin();
        iterEnd = p.end();
        for (RelationIterator iter = p.begin(); iter != iterEnd; iter++)
        {
            // Perform de-duplication test.
//            if (max(Q.start, iter->start) >= p.gstart)
            if ((Q.end < iter->end) && (iter->start >= p.gstart))
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
        }
    }

    // Handle the last partition.
    if (e_pId != s_pId)
    {
        iterStart = this->pRecs[e_pId].begin();
        iterEnd = this->pRecs[e_pId].end();
        for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
        {
            if ((iter->start < Q.end) && (Q.end < iter->end) && (iter->start >= this->pRecs[e_pId].gstart))
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
        }
    }


    return result;
}


size_t OneDimensionalGrid::execute_Overlapped(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    auto e_pId = (Q.end   == this->gend)? this->numPartitionsMinus1: Q.end/this->partitionExtent;
    
    
    // Handle the first partition.
    iterStart = this->pRecs[s_pId].begin();
    iterEnd   = this->pRecs[s_pId].end();
    for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
    {
        if ((Q.start < iter->end) && (Q.start > iter->start) && (Q.end > iter->end))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }

    return result;
}


size_t OneDimensionalGrid::execute_Contains(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    auto e_pId = (Q.end   == this->gend)? this->numPartitionsMinus1: Q.end/this->partitionExtent;
    
    
    // Handle the first partition.
    iterStart = this->pRecs[s_pId].begin();
    iterEnd   = this->pRecs[s_pId].end();
    for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
    {
        if ((iter->start > Q.start) && (Q.end > iter->end))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }

    // Handle partitions completely contained inside the query range.
    for (auto pId = s_pId+1; pId < e_pId; pId++)
    {
        Relation &p = this->pRecs[pId];
        iterStart = p.begin();
        iterEnd = p.end();
        for (RelationIterator iter = p.begin(); iter != iterEnd; iter++)
        {
            // Perform de-duplication test.
//            if (max(Q.start, iter->start) >= p.gstart)
            if ((iter->start >= p.gstart && iter->end < Q.end))
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result^= iter->id;
#endif
            }
        }
    }

    // Handle the last partition.
    if (e_pId != s_pId)
    {
        iterStart = this->pRecs[e_pId].begin();
        iterEnd = this->pRecs[e_pId].end();
        for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
        {
//            if ((max(Q.start, iter->start) >= this->pRecs[e_pId].gstart) && (iter->start <= Q.end && Q.start <= iter->end))
            if ((iter->start >= this->pRecs[e_pId].gstart) && (iter->start > Q.start) && (Q.end > iter->end))
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
        }
    }

    return result;

}


size_t OneDimensionalGrid::execute_Contained(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    
    return this->pRecs[s_pId].execute_Contained(Q);
}



size_t OneDimensionalGrid::execute_Precedes(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.end == this->gend)? this->numPartitionsMinus1: Q.end/this->partitionExtent;
    auto e_pId = this->numPartitionsMinus1;
    
    
    // Handle the first partition.
    iterStart = this->pRecs[s_pId].begin();
    iterEnd   = this->pRecs[s_pId].end();
    for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
    {
        if (iter->start > Q.end)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }

    for (auto pId = s_pId+1; pId <= e_pId; pId++)
    {
        Relation &p = this->pRecs[pId];
        iterStart = p.begin();
        iterEnd = p.end();
        for (RelationIterator iter = p.begin(); iter != iterEnd; iter++)
        {
            if (iter->start >= p.gstart)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
        }
    }

    return result;
}

size_t OneDimensionalGrid::execute_Preceded(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = 0;
    auto e_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    
    
    // Handle the first partition.
    iterStart = this->pRecs[s_pId].begin();
    iterEnd   = this->pRecs[s_pId].end();
    for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
    {
        if (iter->end < Q.start)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }

    // Handle partitions completely contained inside the query range.
    for (auto pId = s_pId+1; pId <= e_pId; pId++)
    {
        Relation &p = this->pRecs[pId];
        iterStart = p.begin();
        iterEnd = p.end();
        for (RelationIterator iter = p.begin(); iter != iterEnd; iter++)
        {
            if ((iter->start >= p.gstart) && (iter->end <  Q.start))
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
        }
    }

    return result;
}


size_t OneDimensionalGrid::execute_gOverlaps(StabbingQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto pId = (Q.point == this->gend)? this->numPartitionsMinus1: Q.point/this->partitionExtent;
    
    
    // Handle the first partition.
    return this->pRecs[pId].execute_gOverlaps(Q);
}


size_t OneDimensionalGrid::execute_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    auto e_pId = (Q.end   == this->gend)? this->numPartitionsMinus1: Q.end/this->partitionExtent;
    
    
    // Handle the first partition.
    iterStart = this->pRecs[s_pId].begin();
    iterEnd   = this->pRecs[s_pId].end();
    for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
    {
        if ((iter->start <= Q.end) && (Q.start <= iter->end))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }

    // Handle partitions completely contained inside the query range.
    for (auto pId = s_pId+1; pId < e_pId; pId++)
    {
        Relation &p = this->pRecs[pId];
        iterStart = p.begin();
        iterEnd = p.end();
        for (RelationIterator iter = p.begin(); iter != iterEnd; iter++)
        {
            // Perform de-duplication test.
//            if (max(Q.start, iter->start) >= p.gstart)
            if (iter->start >= p.gstart)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
        }
    }

    // Handle the last partition.
    if (e_pId != s_pId)
    {
        iterStart = this->pRecs[e_pId].begin();
        iterEnd = this->pRecs[e_pId].end();
        for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
        {
//            if ((max(Q.start, iter->start) >= this->pRecs[e_pId].gstart) && (iter->start <= Q.end && Q.start <= iter->end))
            if ((iter->start >= this->pRecs[e_pId].gstart) && (iter->start <= Q.end) && (Q.start <= iter->end))
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
        }
    }


    return result;
}
