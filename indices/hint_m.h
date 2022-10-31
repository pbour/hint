/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   Nikos Mamoulis
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

#ifndef _HINT_M_H_
#define _HINT_M_H_

#include "../def_global.h"
#include "../containers/relation.h"
#include "../containers/offsets.h"
#include "../containers/offsets_templates.cpp"
#include "../indices/hierarchicalindex.h"
#include <boost/dynamic_bitset.hpp>



// Base HINT^m, no optimizations activated
class HINT_M : public HierarchicalIndex
{
private:
    Relation **pOrgs, **pReps;
    RecordId **pOrgs_sizes;
    size_t   **pReps_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
public:
    // Construction
    HINT_M(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void print(const char c);
    void getStats();
    ~HINT_M();
    
    // Querying
    size_t executeTopDown_gOverlaps(RangeQuery Q);
    size_t executeBottomUp_gOverlaps(RangeQuery Q);
};



// HINT^m with subs+sort optimization activated
class HINT_M_SubsSort : public HierarchicalIndex
{
private:
    Relation **pOrgsIn;
    Relation **pOrgsAft;
    Relation **pRepsIn;
    Relation **pRepsAft;
    RecordId **pOrgsIn_sizes, **pOrgsAft_sizes;
    size_t   **pRepsIn_sizes, **pRepsAft_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
public:
    // Construction
    HINT_M_SubsSort(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void getStats();
    ~HINT_M_SubsSort();
    
    // Querying
    size_t executeBottomUp_gOverlaps(RangeQuery Q);
};



// HINT^m with subs+sopt optimizations activated
class HINT_M_SubsSopt : public HierarchicalIndex
{
private:
    Relation      **pOrgsIn;
    RelationStart **pOrgsAft;
    RelationEnd   **pRepsIn;
    RelationId    **pRepsAft;
    RecordId      **pOrgsIn_sizes, **pOrgsAft_sizes;
    size_t        **pRepsIn_sizes, **pRepsAft_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
public:
    // Construction
    HINT_M_SubsSopt(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void print(const char c);
    void getStats();
    ~HINT_M_SubsSopt();
    
    // Querying
    size_t executeBottomUp_gOverlaps(RangeQuery Q);
};



// HINT^m with subs+sort+sopt optimizations activated
class HINT_M_SubsSortSopt : public HierarchicalIndex
{
private:
    Relation      **pOrgsIn;
    RelationStart **pOrgsAft;
    RelationEnd   **pRepsIn;
    RelationId    **pRepsAft;
    RecordId      **pOrgsIn_sizes, **pOrgsAft_sizes;
    size_t        **pRepsIn_sizes, **pRepsAft_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
public:
    // Construction
    HINT_M_SubsSortSopt(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void print(const char c);
    void getStats();
    ~HINT_M_SubsSortSopt();
    
    // Querying
    size_t executeBottomUp_gOverlaps(RangeQuery Q);
};



// HINT^m with subs+sort+sopt and skewness & sparsity optimizations activated
class HINT_M_SubsSortSopt_SS : public HierarchicalIndex
{
private:
    Relation      *pOrgsIn;
    RelationStart *pOrgsAft;
    RelationEnd   *pRepsIn;
    RelationId    *pRepsAft;
    RecordId      **pOrgsIn_sizes, **pOrgsAft_sizes;
    size_t        **pRepsIn_sizes, **pRepsAft_sizes;
    RecordId      **pOrgsIn_offsets, **pOrgsAft_offsets;
    size_t        **pRepsIn_offsets, **pRepsAft_offsets;
    Offsets_SS_OrgsIn  *pOrgsIn_ioffsets;
    Offsets_SS_OrgsAft *pOrgsAft_ioffsets;
    Offsets_SS_RepsIn  *pRepsIn_ioffsets;
    Offsets_SS_RepsAft *pRepsAft_ioffsets;

    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
    // Querying
    // Auxiliary functions to determine exactly how to scan a partition.
    inline bool getBounds_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, RelationIterator &iterStart, RelationIterator &iterEnd);
    inline bool getBounds_OrgsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationIterator &iterStart, RelationIterator &iterEnd);
    inline bool getBounds_OrgsAft(unsigned int level, Timestamp t, PartitionId &next_from, RelationStartIterator &iterStart, RelationStartIterator &iterEnd);
    inline bool getBounds_OrgsAft(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationStartIterator &iterStart, RelationStartIterator &iterEnd);
    inline bool getBounds_RepsIn(unsigned int level, Timestamp t, PartitionId &next_from, RelationEndIterator &iterStart, RelationEndIterator &iterEnd);
    inline bool getBounds_RepsAft(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterStart, RelationIdIterator &iterEnd);
    
    // Auxiliary functions to scan partitions.
    inline void scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp qstart, Record qdummyE, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn_gOverlaps(unsigned int level, Timestamp a, RecordEnd qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsAft_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanPartitions_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanPartitions_OrgsAft_gOverlaps(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanLastPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp b, Record qdummyE, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsAft_gOverlaps(unsigned int level, Timestamp b, RecordStart qdummySE, PartitionId &next_from, size_t &result);
    
public:
    // Construction
    HINT_M_SubsSortSopt_SS(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void getStats();
    ~HINT_M_SubsSortSopt_SS();
    
    // Querying
    size_t executeBottomUp_gOverlaps(RangeQuery Q);
};



// HINT^m with subs+sort+sopt and cash misses optimizations activated
class HINT_M_SubsSortSopt_CM : public HierarchicalIndex
{
private:
    Relation      **pOrgsInTmp;
    RelationStart **pOrgsAftTmp;
    RelationEnd   **pRepsInTmp;
    RelationId    **pOrgsInIds;
    vector<pair<Timestamp, Timestamp> > **pOrgsInTimestamps;
    RelationId    **pOrgsAftIds;
    vector<Timestamp> **pOrgsAftTimestamp;
    RelationId    **pRepsInIds;
    vector<Timestamp> **pRepsInTimestamp;
    RelationId    **pRepsAft;
    RecordId      **pOrgsIn_sizes, **pOrgsAft_sizes;
    size_t        **pRepsIn_sizes, **pRepsAft_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
public:
    // Construction
    HINT_M_SubsSortSopt_CM(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void getStats();
    ~HINT_M_SubsSortSopt_CM();
    
    // Querying
    size_t executeBottomUp_gOverlaps(RangeQuery Q);
};



// HINT^m with subs+sort, skewness & sparsity optimizations and cash misses activated, from VLDB Journal
class HINT_M_SubsSort_SS_CM : public HierarchicalIndex
{
private:
    Relation      *pOrgsInTmp;
    Relation      *pOrgsAftTmp;
    Relation      *pRepsInTmp;
    Relation      *pRepsAftTmp;
    
    RelationId    *pOrgsInIds;
    vector<pair<Timestamp, Timestamp> > *pOrgsInTimestamps;
    RelationId    *pOrgsAftIds;
    vector<pair<Timestamp, Timestamp> > *pOrgsAftTimestamps;
    RelationId    *pRepsInIds;
    vector<pair<Timestamp, Timestamp> > *pRepsInTimestamps;
    RelationId    *pRepsAftIds;
    vector<pair<Timestamp, Timestamp> > *pRepsAftTimestamps;
    
    RecordId      **pOrgsIn_sizes, **pOrgsAft_sizes;
    size_t        **pRepsIn_sizes, **pRepsAft_sizes;
    RecordId      **pOrgsIn_offsets, **pOrgsAft_offsets;
    size_t        **pRepsIn_offsets, **pRepsAft_offsets;
    Offsets_SS_CM *pOrgsIn_ioffsets;
    Offsets_SS_CM *pOrgsAft_ioffsets;
    Offsets_SS_CM *pRepsIn_ioffsets;
    Offsets_SS_CM *pRepsAft_ioffsets;

    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
    // Querying
    // Auxiliary functions to determine exactly how to scan a partition.
    inline bool getBounds(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, vector<pair<Timestamp, Timestamp> >::iterator &iterStart, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI);
    inline bool getBounds(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, RelationId *ids, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBoundsS(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBoundsE(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, RelationId *ids, vector<pair<Timestamp, Timestamp> > *timestamps, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBounds(unsigned int level, Timestamp ts, Timestamp te, PartitionId &next_from, PartitionId &next_to, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, vector<pair<Timestamp, Timestamp> >::iterator &iterStart, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI);
    inline bool getBounds(unsigned int level, Timestamp ts, Timestamp te, PartitionId &next_from, PartitionId &next_to, Offsets_SS_CM *ioffsets, RelationId *ids, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    
    // Auxiliary functions to scan a partition.
    inline void scanPartition_CheckBothTimestamps_Equals(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckBothTimestamps_Starts(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckStart_Starts(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckBothTimestamps_Started(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckBothTimestamps_Finishes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckBothTimestamps2_Finishes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckEnd_Finishes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckBothTimestamps_Finished(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckBothTimestamps2_Finished(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckEnd_Finished(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckEnd_Met(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckAllConditions_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckTwoConditions1_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckTwoConditions2_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckOneCondition_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamp, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckTwoConditions_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckTwoConditions3_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartitions_CheckOneCondition_Overlaps(unsigned int level, Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanPartition_CheckOneCondition2_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result);
  inline void scanPartition_CheckAllConditions_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckTwoConditions1_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckTwoConditions2_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckTwoConditions3_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckTwoConditions4_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckOneCondition_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckOneCondition_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckEnd_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartitions_CheckOneCondition_Overlapped(unsigned int level, Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanPartition_CheckBothTimestamps_Contains(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckStart_Contains(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartitions_CheckEnd_Contains(unsigned int level, Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanPartition_CheckBothTimestamps_Contained(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckStart_Contained(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckEnd_Contained(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);

    inline void scanPartition_Precedes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartitions_Precedes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, PartitionId &next_from, size_t &result);
    inline void scanPartition_Preceded(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_Preceded(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartitions_Preceded(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, RelationId *ids, vector<pair<Timestamp, Timestamp> > *timestamps, PartitionId &next_from, size_t &result);

    inline void scanPartition_CheckBothTimestamps_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckEnd_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckEnd_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_CheckStart_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result);
    inline void scanPartition_NoChecks_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, size_t &result);
    inline void scanPartitions_NoChecks_gOverlaps(unsigned int level, Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, PartitionId &next_to, size_t &result);

public:
    // Construction
    HINT_M_SubsSort_SS_CM(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void getStats();
    ~HINT_M_SubsSort_SS_CM();
    
    // Querying
    // Basic predicates of Allen's algebra
    size_t executeBottomUp_Equals(RangeQuery Q);
    size_t executeBottomUp_Starts(RangeQuery Q);
    size_t executeBottomUp_Started(RangeQuery Q);
    size_t executeBottomUp_Finishes(RangeQuery Q);
    size_t executeBottomUp_Finished(RangeQuery Q);
    size_t executeBottomUp_Meets(RangeQuery Q);
    size_t executeBottomUp_Met(RangeQuery Q);
    size_t executeBottomUp_Overlaps(RangeQuery Q);
    size_t executeBottomUp_Overlapped(RangeQuery Q);
    size_t executeBottomUp_Contains(RangeQuery Q);
    size_t executeBottomUp_Contained(RangeQuery Q);
    size_t executeBottomUp_Precedes(RangeQuery Q);
    size_t executeBottomUp_Preceded(RangeQuery Q);

    // Generalized predicates, ACM SIGMOD'22 gOverlaps
    size_t executeBottomUp_gOverlaps(RangeQuery Q);
};



// HINT^m with all optimizations activated, from ACM SIGMOD'22 extend to answer all predicates
class HINT_M_ALL : public HierarchicalIndex
{
private:
    Relation      *pOrgsInTmp;
    RelationStart *pOrgsAftTmp;
    RelationEnd   *pRepsInTmp;
    
    RelationId    *pOrgsInIds;
    vector<pair<Timestamp, Timestamp> > *pOrgsInTimestamps;
    RelationId    *pOrgsAftIds;
    vector<Timestamp> *pOrgsAftTimestamp;
    RelationId    *pRepsInIds;
    vector<Timestamp> *pRepsInTimestamp;
    RelationId    *pRepsAft;
    
    RecordId      **pOrgsIn_sizes,   **pOrgsAft_sizes;
    size_t        **pRepsIn_sizes,   **pRepsAft_sizes;
    RecordId      **pOrgsIn_offsets, **pOrgsAft_offsets;
    size_t        **pRepsIn_offsets, **pRepsAft_offsets;
    Offsets_ALL_OrgsIn  *pOrgsIn_ioffsets;
    Offsets_ALL_OrgsAft *pOrgsAft_ioffsets;
    Offsets_ALL_RepsIn  *pRepsIn_ioffsets;
    Offsets_ALL_RepsAft *pRepsAft_ioffsets;
    
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
    // Querying
    // Auxiliary functions to determine exactly how to scan a partition.
    inline bool getBounds_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBounds_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<pair<Timestamp, Timestamp> >::iterator &iterStart, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI);
    inline bool getBoundsS_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<pair<Timestamp, Timestamp> >::iterator &iterStart, RelationIdIterator &iterI);
    inline bool getBoundsE_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI);
    inline bool getBounds_OrgsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBounds_OrgsAft(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBounds_OrgsAft(unsigned int level, Timestamp t, PartitionId &next_from, vector<Timestamp>::iterator &iterStart, vector<Timestamp>::iterator &iterEnd, RelationIdIterator &iterI);
    inline bool getBoundsS_OrgsAft(unsigned int level, Timestamp t, PartitionId &next_from, vector<Timestamp>::iterator &iterStart, RelationIdIterator &iterI);
    inline bool getBounds_OrgsAft(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBounds_OrgsAft(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, vector<Timestamp>::iterator &iterStart, vector<Timestamp>::iterator &iterEnd, RelationIdIterator &iterI);
    inline bool getBounds_RepsIn(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBounds_RepsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<Timestamp>::iterator &iterStart, vector<Timestamp>::iterator &iterEnd, RelationIdIterator &iterI);
    inline bool getBoundsE_RepsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<Timestamp>::iterator &iterEnd, RelationIdIterator &iterI);
    inline bool getBounds_RepsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd);
    inline bool getBounds_RepsAft(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterStart, RelationIdIterator &iterEnd);
    
    // Auxiliary functions to scan a partition.
    inline void scanFirstPartition_OrgsIn_Equals(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Equals(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand);
    inline void scanLastPartition_RepsIn_Equals(unsigned int level, Timestamp a, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanFirstPartition_OrgsIn_Starts(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Starts(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Starts(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanLastPartition_RepsIn_Starts(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanLastPartition_RepsAft_Starts(unsigned int level, Timestamp a, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanFirstPartition_OrgsIn_Started(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_Started(unsigned int level, Timestamp a, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanPartitions_RepsIn_Started(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanLastPartition_RepsIn_Started(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanLastPartition_OrgsIn_Finishes(unsigned int level, Timestamp b, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Finishes(unsigned int level, Timestamp a, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanFirstPartition_OrgsAft_Finishes(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanLastPartition_RepsIn_Finishes(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_RepsIn_Finishes(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanLastPartition_OrgsIn_Finished(unsigned int level, Timestamp b, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsIn_Finished(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Finished(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanPartitions_OrgsAft_Finished(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanFirstPartition_OrgsIn_Meets(unsigned int level, Timestamp a, Timestamp qstart, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Meets(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsIn_Met(unsigned int level, Timestamp a, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_RepsIn_Met(unsigned int level, Timestamp a, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_Overlaps(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Overlaps(unsigned int level, Timestamp a, Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsIn_Overlaps(unsigned int level, Timestamp b, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsAft_Overlaps(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_Overlapped(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_Overlapped(unsigned int level, Timestamp a, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_Overlapped(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn_Overlapped(unsigned int level, Timestamp b, Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn_Overlapped(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_RepsIn_Overlapped(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanFirstPartition_OrgsIn_Contains(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_Contains(unsigned int level, Timestamp a, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsIn_Contains(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Contains(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanFirstPartition_OrgsIn_Contained(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Contained(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_Contained(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanFirstPartition_RepsAft_Contained(unsigned int level, Timestamp a, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result);
    inline void scanLastPartition_OrgsIn_Precedes(unsigned int level, Timestamp b, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsAft_Precedes(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result);
    inline void scanPartitions_OrgsIn_Precedes(unsigned int level, Timestamp b, PartitionId &next_from, size_t &result);
    inline void scanPartitions_OrgsAft_Precedes(unsigned int level, Timestamp b, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_Preceded(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn_Preceded(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanPartitions_OrgsIn_Preceded(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanPartitions_RepsIn_Preceded(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp qstart, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsAft_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn_gOverlaps(unsigned int level, Timestamp a, RecordEnd qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsAft_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanPartitions_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanPartitions_OrgsAft_gOverlaps(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanLastPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp b, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsAft_gOverlaps(unsigned int level, Timestamp b, RecordStart qdummySE, PartitionId &next_from, size_t &result);

public:
    // Construction
    HINT_M_ALL(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void getStats();
    ~HINT_M_ALL();
    
    // Querying
    // Basic predicates of Allen's algebra
    size_t executeBottomUp_Equals(RangeQuery Q);
    size_t executeBottomUp_Starts(RangeQuery Q);
    size_t executeBottomUp_Started(RangeQuery Q);
    size_t executeBottomUp_Finishes(RangeQuery Q);
    size_t executeBottomUp_Finished(RangeQuery Q);
    size_t executeBottomUp_Meets(RangeQuery Q);
    size_t executeBottomUp_Met(RangeQuery Q);
    size_t executeBottomUp_Overlaps(RangeQuery Q);
    size_t executeBottomUp_Overlapped(RangeQuery Q);
    size_t executeBottomUp_Contains(RangeQuery Q);
    size_t executeBottomUp_Contained(RangeQuery Q);
    size_t executeBottomUp_Precedes(RangeQuery Q);
    size_t executeBottomUp_Preceded(RangeQuery Q);

    // Generalized predicates, ACM SIGMOD'22 gOverlaps
    size_t executeBottomUp_gOverlaps(StabbingQuery Q);
    size_t executeBottomUp_gOverlaps(RangeQuery Q);
};



// Comparators
inline bool CompareTimestampPairsByStart(const pair<Timestamp, Timestamp> &lhs, const pair<Timestamp, Timestamp> &rhs)
{
    return (lhs.first < rhs.first);
}

inline bool CompareTimestampPairsByEnd(const pair<Timestamp, Timestamp> &lhs, const pair<Timestamp, Timestamp> &rhs)
{
    return (lhs.second < rhs.second);
}
#endif // _HINT_M_H_
