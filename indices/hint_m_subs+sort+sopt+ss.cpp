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

#include "hint_m.h"



inline void HINT_M_SubsSortSopt_SS::updateCounters(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0, lastfound = 0;
    
    
    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
                if ((a == b) && (!lastfound))
                {
                    this->pRepsIn_sizes[level][a]++;
                    lastfound = 1;
                }
                else
                    this->pRepsAft_sizes[level][a]++;
            }
            else
            {
                if ((a == b) && (!lastfound))
                    this->pOrgsIn_sizes[level][a]++;
                else
                    this->pOrgsAft_sizes[level][a]++;
                firstfound = 1;
            }
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            b--;
            if ((!firstfound) && b < a)
            {
                if (!lastfound)
                    this->pOrgsIn_sizes[level][prevb]++;
                else
                    this->pOrgsAft_sizes[level][prevb]++;
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsIn_sizes[level][prevb]++;
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAft_sizes[level][prevb]++;
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


inline void HINT_M_SubsSortSopt_SS::updatePartitions(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0, lastfound = 0;
    
    
    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
                if ((a == b) && (!lastfound))
                {
                    this->pRepsIn[level][this->pRepsIn_offsets[level][a]++] = RecordEnd(r.id, r.end);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAft[level][this->pRepsAft_offsets[level][a]++] = r.id;
                }
            }
            else
            {
                if ((a == b) && (!lastfound))
                {
                    this->pOrgsIn[level][this->pOrgsIn_offsets[level][a]++] = Record(r.id, r.start, r.end);
                }
                else
                {
                    this->pOrgsAft[level][this->pOrgsAft_offsets[level][a]++] = RecordStart(r.id, r.start);
                }
                firstfound = 1;
            }
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            b--;
            if ((!firstfound) && b < a)
            {
                if (!lastfound)
                {
                    this->pOrgsIn[level][this->pOrgsIn_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
                }
                else
                {
                    this->pOrgsAft[level][this->pOrgsAft_offsets[level][prevb]++] = RecordStart(r.id, r.start);
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsIn[level][this->pRepsIn_offsets[level][prevb]++] = RecordEnd(r.id, r.end);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAft[level][this->pRepsAft_offsets[level][prevb]++] = r.id;
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSortSopt_SS::HINT_M_SubsSortSopt_SS(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    OffsetEntry_SS_OrgsIn  dummySE;
    OffsetEntry_SS_OrgsAft dummyS;
    OffsetEntry_SS_RepsIn  dummyE;
    OffsetEntry_SS_RepsAft dummyI;
    Offsets_SS_OrgsIn_Iterator  iterSEO, iterSEOStart, iterSEOEnd;
    Offsets_SS_OrgsAft_Iterator iterSO, iterSOStart, iterSOEnd;
    Offsets_SS_RepsIn_Iterator  iterEO, iterEOStart, iterEOEnd;
    Offsets_SS_RepsAft_Iterator iterIO, iterIOStart, iterIOEnd;
    PartitionId tmp = -1;
    

    // Initialize statistics
    this->numOriginalsIn = this->numOriginalsAft = this->numReplicasIn = this->numReplicasAft = 0;


    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsAft_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsAft_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pOrgsIn_offsets  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsAft_offsets = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_offsets  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsAft_offsets = (size_t **)malloc(this->height*sizeof(size_t *));

    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));

        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsAft_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsAft_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
        this->pOrgsIn_offsets[l]  = (RecordId *)calloc(cnt+1, sizeof(RecordId));
        this->pOrgsAft_offsets[l] = (RecordId *)calloc(cnt+1, sizeof(RecordId));
        this->pRepsIn_offsets[l]  = (size_t *)calloc(cnt+1, sizeof(size_t));
        this->pRepsAft_offsets[l] = (size_t *)calloc(cnt+1, sizeof(size_t));
    }

    for (const Record &r : R)
        this->updateCounters(r);


    // Step 2: allocate necessary memory.
    this->pOrgsIn  = new Relation[this->height];
    this->pOrgsAft = new RelationStart[this->height];
    this->pRepsIn  = new RelationEnd[this->height];
    this->pRepsAft = new RelationId[this->height];

    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOaft = 0, sumRin = 0, sumRaft = 0;

        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsIn_offsets[l][pId]  = sumOin;
            this->pOrgsAft_offsets[l][pId] = sumOaft;
            this->pRepsIn_offsets[l][pId]  = sumRin;
            this->pRepsAft_offsets[l][pId] = sumRaft;
            sumOin  += this->pOrgsIn_sizes[l][pId];
            sumOaft += this->pOrgsAft_sizes[l][pId];
            sumRin  += this->pRepsIn_sizes[l][pId];
            sumRaft += this->pRepsAft_sizes[l][pId];
        }
        this->pOrgsIn_offsets[l][cnt]  = sumOin;
        this->pOrgsAft_offsets[l][cnt] = sumOaft;
        this->pRepsIn_offsets[l][cnt]  = sumRin;
        this->pRepsAft_offsets[l][cnt] = sumRaft;

        this->pOrgsIn[l].resize(sumOin);
        this->pOrgsAft[l].resize(sumOaft);
        this->pRepsIn[l].resize(sumRin);
        this->pRepsAft[l].resize(sumRaft);
    }


    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);


    // Step 4: create offset pointers
    this->pOrgsIn_ioffsets  = new Offsets_SS_OrgsIn[this->height];
    this->pOrgsAft_ioffsets = new Offsets_SS_OrgsAft[this->height];
    this->pRepsIn_ioffsets  = new Offsets_SS_RepsIn[this->height];
    this->pRepsAft_ioffsets = new Offsets_SS_RepsAft[this->height];
    for (int l = this->height-1; l > -1; l--)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOaft = 0, sumRin = 0, sumRaft = 0;

        for (auto pId = 0; pId < cnt; pId++)
        {
            bool isEmpty = true;
            
            dummySE.tstamp = pId >> 1;
            //            get<0>(dummySE) = ((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsIn_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOStart = this->pOrgsIn_ioffsets[l+1].begin();
                    iterSEOEnd = this->pOrgsIn_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOStart, iterSEOEnd, dummySE);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOStart): -1;
                }
                this->pOrgsIn_ioffsets[l].push_back(OffsetEntry_SS<Relation>(pId, this->pOrgsIn[l].begin()+sumOin, tmp));
            }

            dummyS.tstamp = pId >> 1;
            //            get<0>(dummySE) = ((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsAft_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSOStart = this->pOrgsAft_ioffsets[l+1].begin();
                    iterSOEnd = this->pOrgsAft_ioffsets[l+1].end();
                    iterSO = lower_bound(iterSOStart, iterSOEnd, dummyS);//, CompareStartOffesetsByTimestamp);
                    tmp = (iterSO != iterSOEnd)? (iterSO-iterSOStart): -1;
                }
                this->pOrgsAft_ioffsets[l].push_back(OffsetEntry_SS<RelationStart>(pId, this->pOrgsAft[l].begin()+sumOaft, tmp));
            }

            dummyE.tstamp = pId >> 1;
            //            get<0>(dummySE) = ((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsIn_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterEOStart = this->pRepsIn_ioffsets[l+1].begin();
                    iterEOEnd = this->pRepsIn_ioffsets[l+1].end();
                    iterEO = lower_bound(iterEOStart, iterEOEnd, dummyE);//, CompareEndOffesetsByTimestamp);
                    tmp = (iterEO != iterEOEnd)? (iterEO-iterEOStart): -1;
                }
                this->pRepsIn_ioffsets[l].push_back(OffsetEntry_SS<RelationEnd>(pId, this->pRepsIn[l].begin()+sumRin, tmp));
            }

            dummyI.tstamp = pId >> 1;
            //            get<0>(dummySE) = ((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsAft_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterIOStart = this->pRepsAft_ioffsets[l+1].begin();
                    iterIOEnd = this->pRepsAft_ioffsets[l+1].end();
                    iterIO = lower_bound(iterIOStart, iterIOEnd, dummyI);//, CompareIdOffesetsByTimestamp);
                    tmp = (iterIO != iterIOEnd)? (iterIO-iterIOStart): -1;
                }
                this->pRepsAft_ioffsets[l].push_back(OffsetEntry_SS<RelationId>(pId, this->pRepsAft[l].begin()+sumRaft, tmp));
            }

            this->pOrgsIn_offsets[l][pId]  = sumOin;
            this->pOrgsAft_offsets[l][pId] = sumOaft;
            this->pRepsIn_offsets[l][pId]  = sumRin;
            this->pRepsAft_offsets[l][pId] = sumRaft;

            sumOin += this->pOrgsIn_sizes[l][pId];
            sumOaft += this->pOrgsAft_sizes[l][pId];
            sumRin += this->pRepsIn_sizes[l][pId];
            sumRaft += this->pRepsAft_sizes[l][pId];
            
            if (isEmpty)
                this->numEmptyPartitions++;
        }
        this->pOrgsIn_offsets[l][cnt]  = sumOin;
        this->pOrgsAft_offsets[l][cnt] = sumOaft;
        this->pRepsIn_offsets[l][cnt]  = sumRin;
        this->pRepsAft_offsets[l][cnt] = sumRaft;
    }


    // Free auxliary memory
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_sizes[l]);
        free(this->pOrgsAft_sizes[l]);
        free(this->pRepsIn_sizes[l]);
        free(this->pRepsAft_sizes[l]);
    }
    free(this->pOrgsIn_sizes);
    free(this->pOrgsAft_sizes);
    free(this->pRepsIn_sizes);
    free(this->pRepsAft_sizes);


    // Step 4: sort partition contents.
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        for (auto pId = 0; pId < cnt; pId++)
        {
            sort(this->pOrgsIn[l].begin()+this->pOrgsIn_offsets[l][pId], this->pOrgsIn[l].begin()+this->pOrgsIn_offsets[l][pId+1]);
            sort(this->pOrgsAft[l].begin()+this->pOrgsAft_offsets[l][pId], this->pOrgsAft[l].begin()+this->pOrgsAft_offsets[l][pId+1]);
            sort(this->pRepsIn[l].begin()+this->pRepsIn_offsets[l][pId], this->pRepsIn[l].begin()+this->pRepsIn_offsets[l][pId+1]);
        }
    }


    // Free auxliary memory
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_offsets[l]);
        free(this->pOrgsAft_offsets[l]);
        free(this->pRepsIn_offsets[l]);
        free(this->pRepsAft_offsets[l]);
    }
    free(this->pOrgsIn_offsets);
    free(this->pOrgsAft_offsets);
    free(this->pRepsIn_offsets);
    free(this->pRepsAft_offsets);
}


void HINT_M_SubsSortSopt_SS::getStats()
{
    size_t sum = 0;
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        this->numPartitions += cnt;

        this->numOriginalsIn  += this->pOrgsIn[l].size();
        this->numOriginalsAft += this->pOrgsAft[l].size();
        this->numReplicasIn   += this->pRepsIn[l].size();
        this->numReplicasAft  += this->pRepsAft[l].size();
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasAft)/(this->numPartitions-numEmptyPartitions);
}


HINT_M_SubsSortSopt_SS::~HINT_M_SubsSortSopt_SS()
{
    delete[] this->pOrgsIn_ioffsets;
    delete[] this->pOrgsAft_ioffsets;
    delete[] this->pRepsIn_ioffsets;
    delete[] this->pRepsAft_ioffsets;
    
    delete[] this->pOrgsIn;
    delete[] this->pOrgsAft;
    delete[] this->pRepsIn;
    delete[] this->pRepsAft;
}


// Auxiliary functions to determine exactly how to scan a partition.
inline bool HINT_M_SubsSortSopt_SS::getBounds_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, RelationIterator &iterBegin, RelationIterator &iterEnd)
{
    OffsetEntry_SS_OrgsIn qdummy;
    Offsets_SS_OrgsIn_Iterator iterIO, iterIOStart, iterIOEnd;
    PartitionId from = next_from;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    

    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterBegin = iterIO->iter;
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iter : this->pOrgsIn[level].end());

                next_from = iterIO->pid;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (this->pOrgsIn_ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while (((this->pOrgsIn_ioffsets[level][from]).tstamp < t) && (from < cnt))
                    from++;
            }
            else if (tmp > t)
            {
                while (((this->pOrgsIn_ioffsets[level][from]).tstamp > t) && (from > -1))
                    from--;
                if (((this->pOrgsIn_ioffsets[level][from]).tstamp != t) || (from == -1))
                    from++;
            }

            if ((from != cnt) && ((this->pOrgsIn_ioffsets[level][from]).tstamp == t))
            {
                iterBegin = (this->pOrgsIn_ioffsets[level][from]).iter;
                iterEnd = ((from+1 != cnt) ? (this->pOrgsIn_ioffsets[level][from+1]).iter : this->pOrgsIn[level].end());

                next_from = (this->pOrgsIn_ioffsets[level][from]).pid;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
                
        return true;
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_SubsSortSopt_SS::getBounds_OrgsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationIterator &iterBegin, RelationIterator &iterEnd)
{
    OffsetEntry_SS_OrgsIn qdummyA, qdummyB;
    Offsets_SS_OrgsIn_Iterator iterIO, iterIO2, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;


    if (cnt > 0)
    {
        from = next_from;
        to = next_to;

        // Do binary search or follow vertical pointers.
        if ((from == -1) || (to == -1))
        {
            qdummyA.tstamp = a;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummyA);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= b))
            {
                next_from = iterIO->pid;

                qdummyB.tstamp = b;
                iterBegin = iterIO->iter;

                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB);
//                iterIO2 = iterIO;
//                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
//                    iterIO2++;

                iterEnd = ((iterIO2 != iterIOEnd) ? iterEnd = iterIO2->iter: this->pOrgsIn[level].end());

                if (iterIO2 != iterIOEnd)
                    next_to = iterIO2->pid;
                else
                    next_to = -1;
                
                return true;
            }
            else
            {
                next_from = -1;
                
                return false;
            }
        }
        else
        {
            Timestamp tmp = (this->pOrgsIn_ioffsets[level][from]).tstamp;
            if (tmp < a)
            {
                while (((this->pOrgsIn_ioffsets[level][from]).tstamp < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while (((this->pOrgsIn_ioffsets[level][from]).tstamp > a) && (from > -1))
                    from--;
                if (((this->pOrgsIn_ioffsets[level][from]).tstamp != a) || (from == -1))
                    from++;
            }

            tmp = (this->pOrgsIn_ioffsets[level][to]).tstamp;
            if (tmp > b)
            {
                while (((this->pOrgsIn_ioffsets[level][to]).tstamp > b) && (to > -1))
                    to--;
                to++;
            }
            else if (tmp == b)
            {
                while (((this->pOrgsIn_ioffsets[level][to]).tstamp <= b) && (to < cnt))
                    to++;
            }

            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterBegin = (this->pOrgsIn_ioffsets[level][from]).iter;
                iterEnd   = (to != cnt)? (this->pOrgsIn_ioffsets[level][to]).iter: this->pOrgsIn[level].end();

                next_from = (this->pOrgsIn_ioffsets[level][from]).pid;
                next_to   = (to != cnt) ? (this->pOrgsIn_ioffsets[level][to]).pid: -1;
                
                return true;
            }
            else
            {
                next_from = next_to = -1;
                
                return false;
            }
        }
    }
    else
    {
        next_from = -1;
        next_to = -1;
        
        return false;
    }
}


inline bool HINT_M_SubsSortSopt_SS::getBounds_OrgsAft(unsigned int level, Timestamp t, PartitionId &next_from, RelationStartIterator &iterBegin, RelationStartIterator &iterEnd)
{
    OffsetEntry_SS_OrgsAft qdummy;
    Offsets_SS_OrgsAft_Iterator iterIO, iterIOStart, iterIOEnd;
    PartitionId from = next_from;
    size_t cnt = this->pOrgsAft_ioffsets[level].size();


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOStart = this->pOrgsAft_ioffsets[level].begin();
            iterIOEnd = this->pOrgsAft_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy);//, CompareStartOffesetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterBegin = iterIO->iter;
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iter : this->pOrgsAft[level].end());

                next_from = iterIO->pid;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (this->pOrgsAft_ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while (((this->pOrgsAft_ioffsets[level][from]).tstamp < t) && (from < cnt))
                    from++;
            }
            else if (tmp > t)
            {
                while (((this->pOrgsAft_ioffsets[level][from]).tstamp > t) && (from > -1))
                    from--;
                if (((this->pOrgsAft_ioffsets[level][from]).tstamp != t) || (from == -1))
                    from++;
            }

            if ((from != cnt) && ((this->pOrgsAft_ioffsets[level][from]).tstamp == t))
            {
                iterBegin = (this->pOrgsAft_ioffsets[level][from]).iter;
                iterEnd = ((from+1 != cnt) ? (this->pOrgsAft_ioffsets[level][from+1]).iter : this->pOrgsAft[level].end());

                next_from = (this->pOrgsAft_ioffsets[level][from]).pid;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
        
        return true;
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_SubsSortSopt_SS::getBounds_OrgsAft(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationStartIterator &iterBegin, RelationStartIterator &iterEnd)
{
    OffsetEntry_SS_OrgsAft qdummyA, qdummyB;
    Offsets_SS_OrgsAft_Iterator iterIO, iterIO2, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsAft_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (to == -1))
        {
            qdummyA.tstamp = a;
            iterIOStart = this->pOrgsAft_ioffsets[level].begin();
            iterIOEnd = this->pOrgsAft_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummyA);//, CompareStartOffesetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= b))
            {
                next_from = iterIO->pid;

                qdummyB.tstamp = b;
                iterBegin = iterIO->iter;

                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB);//, CompareStartOffesetsByTimestamp);
//                iterIO2 = iterIO;
//                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
//                    iterIO2++;

                iterEnd = ((iterIO2 != iterIOEnd) ? iterEnd = iterIO2->iter: this->pOrgsAft[level].end());

                if (iterIO2 != iterIOEnd)
                    next_to = iterIO2->pid;
                else
                    next_to = -1;
                
                return true;
            }
            else
            {
                next_from = -1;
                
                return false;
            }
        }
        else
        {
            Timestamp tmp = (this->pOrgsAft_ioffsets[level][from]).tstamp;
            if (tmp < a)
            {
                while (((this->pOrgsAft_ioffsets[level][from]).tstamp < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while (((this->pOrgsAft_ioffsets[level][from]).tstamp > a) && (from > -1))
                    from--;
                if (((this->pOrgsAft_ioffsets[level][from]).tstamp != a) || (from == -1))
                    from++;
            }

            tmp = (this->pOrgsAft_ioffsets[level][to]).tstamp;
            if (tmp > b)
            {
                while (((this->pOrgsAft_ioffsets[level][to]).tstamp > b) && (to > -1))
                    to--;
                to++;
            }
            else if (tmp == b)
            {
                while (((this->pOrgsAft_ioffsets[level][to]).tstamp <= b) && (to < cnt))
                    to++;
            }

            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterBegin = (this->pOrgsAft_ioffsets[level][from]).iter;
                iterEnd   = (to != cnt)? (this->pOrgsAft_ioffsets[level][to]).iter: this->pOrgsAft[level].end();

                next_from = (this->pOrgsAft_ioffsets[level][from]).pid;
                next_to   = (to != cnt) ? (this->pOrgsAft_ioffsets[level][to]).pid: -1;
                
                return true;
            }
            else
            {
                next_from = next_to = -1;
                
                return false;
            }
        }
    }
    else
    {
        next_from = -1;
        next_to = -1;
        
        return false;
    }
}


inline bool HINT_M_SubsSortSopt_SS::getBounds_RepsIn(unsigned int level, Timestamp t, PartitionId &next_from, RelationEndIterator &iterBegin, RelationEndIterator &iterEnd)
{
    OffsetEntry_SS_RepsIn qdummy;
    Offsets_SS_RepsIn_Iterator iterIO, iterIOStart, iterIOEnd;
    PartitionId from = next_from;
    size_t cnt = this->pRepsIn_ioffsets[level].size();


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOStart = this->pRepsIn_ioffsets[level].begin();
            iterIOEnd = this->pRepsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy);//, CompareEndOffesetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterBegin = iterIO->iter;
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iter : this->pRepsIn[level].end());

                next_from = iterIO->pid;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (this->pRepsIn_ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while (((this->pRepsIn_ioffsets[level][from]).tstamp < t) && (from < cnt))
                    from++;
            }
            else if (tmp > t)
            {
                while (((this->pRepsIn_ioffsets[level][from]).tstamp > t) && (from > -1))
                    from--;
                if (((this->pRepsIn_ioffsets[level][from]).tstamp != t) || (from == -1))
                    from++;
            }

            if ((from != cnt) && ((this->pRepsIn_ioffsets[level][from]).tstamp == t))
            {
                iterBegin = (this->pRepsIn_ioffsets[level][from]).iter;
                iterEnd = ((from+1 != cnt) ? (this->pRepsIn_ioffsets[level][from+1]).iter : this->pRepsIn[level].end());

                next_from = (this->pRepsIn_ioffsets[level][from]).pid;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }

        return true;
    }
    else
    {
        next_from = -1;

        return false;
    }
}
    


inline bool HINT_M_SubsSortSopt_SS::getBounds_RepsAft(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterBegin, RelationIdIterator &iterEnd)
{
    OffsetEntry_SS_RepsAft qdummy;
    Offsets_SS_RepsAft_Iterator iterIO, iterIOStart, iterIOEnd;
    PartitionId from = next_from;
    size_t cnt = this->pRepsAft_ioffsets[level].size();


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOStart = this->pRepsAft_ioffsets[level].begin();
            iterIOEnd = this->pRepsAft_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy);//, CompareIdOffesetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterBegin = iterIO->iter;
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iter : this->pRepsAft[level].end());

                next_from = iterIO->pid;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (this->pRepsAft_ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while (((this->pRepsAft_ioffsets[level][from]).tstamp < t) && (from < cnt))
                    from++;
            }
            else if (tmp > t)
            {
                while (((this->pRepsAft_ioffsets[level][from]).tstamp > t) && (from > -1))
                    from--;
                if (((this->pRepsAft_ioffsets[level][from]).tstamp != t) || (from == -1))
                    from++;
            }

            if ((from != cnt) && ((this->pRepsAft_ioffsets[level][from]).tstamp == t))
            {
                iterBegin = (this->pRepsAft_ioffsets[level][from]).iter;
                iterEnd = ((from+1 != cnt) ? (this->pRepsAft_ioffsets[level][from+1]).iter : this->pRepsAft[level].end());

                next_from = (this->pRepsAft_ioffsets[level][from]).pid;
            }
            else
            {
                next_from = -1;
         
                return false;
            }
        }

        return true;
    }
    else
    {
        next_from = -1;
        
        return false;
    }
}


// Auxiliary functions to scan a partition.
inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIterator iter, iterBegin, iterEnd;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp qstart, Record qdummyE, PartitionId &next_from, size_t &result)
{
    RelationIterator iter, iterBegin, iterEnd;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd))
    {
        RelationIterator pivot = lower_bound(iterBegin, iterEnd, qdummyE);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (qstart <= iter->end)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    RelationIterator iter, iterBegin, iterEnd;

    
    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qstart <= iter->end)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_OrgsAft_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationStartIterator iter, iterBegin, iterEnd;

    
    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_RepsIn_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationEndIterator iter, iterBegin, iterEnd;


    if (this->getBounds_RepsIn(level, a, next_from, iterBegin, iterEnd))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_RepsIn_gOverlaps(unsigned int level, Timestamp a, RecordEnd qdummyS, PartitionId &next_from, size_t &result)
{
    RelationEndIterator iter, iterBegin, iterEnd;


    if (this->getBounds_RepsIn(level, a, next_from, iterBegin, iterEnd))
    {
        RelationEndIterator pivot = lower_bound(iterBegin, iterEnd, qdummyS);
        for (iter = pivot; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_RepsAft_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iter, iterBegin, iterEnd;


    if (this->getBounds_RepsAft(level, a, next_from, iterBegin, iterEnd))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iter);
#endif
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanLastPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp b, Record qdummyE, PartitionId &next_from, size_t &result)
{
    RelationIterator iter, iterBegin, iterEnd;


    if (this->getBounds_OrgsIn(level, b, next_from, iterBegin, iterEnd))
    {
        RelationIterator pivot = lower_bound(iterBegin, iterEnd, qdummyE);
        for (iter = iterBegin; iter != pivot; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanLastPartition_OrgsAft_gOverlaps(unsigned int level, Timestamp b, RecordStart qdummySE, PartitionId &next_from, size_t &result)
{
    RelationStartIterator iter, iterBegin, iterEnd;


    if (this->getBounds_OrgsAft(level, b, next_from, iterBegin, iterEnd))
    {
        RelationStartIterator pivot = lower_bound(iterBegin, iterEnd, qdummySE);
        for (iter = iterBegin; iter != pivot; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanPartitions_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIterator iter, iterBegin, iterEnd;

    
    if (this->getBounds_OrgsIn(level, a, b, next_from, next_to, iterBegin, iterEnd))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }
}


inline void HINT_M_SubsSortSopt_SS::scanPartitions_OrgsAft_gOverlaps(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationStartIterator iter, iterBegin, iterEnd;

    
    if (this->getBounds_OrgsAft(level, a, b, next_from, next_to, iterBegin, iterEnd))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }
}


// Generalized predicates, ACM SIGMOD'22 gOverlaps
size_t HINT_M_SubsSortSopt_SS::executeBottomUp_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterBegin;
    RelationIterator iter, iterEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    Record qdummyE(0, Q.end+1, Q.end+1);
    RecordStart qdummySE(0, Q.end+1);
    RecordEnd qdummyS(0, Q.start);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1, next_fromOinB = -1, next_fromOaftB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOaftAB = -1, next_toOaftAB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results

            // Handle the partition that contains a: consider both originals and replicas
            this->scanFirstPartition_RepsIn_gOverlaps(l, a, next_fromRinA, result);
            this->scanFirstPartition_RepsAft_gOverlaps(l, a, next_fromRaftA, result);

            this->scanPartitions_OrgsIn_gOverlaps(l, a, b, next_fromOinAB, next_toOinAB, result);
            this->scanPartitions_OrgsAft_gOverlaps(l, a, b, next_fromOaftAB, next_toOaftAB, result);
        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                this->scanFirstPartition_OrgsIn_gOverlaps(l, a, Q.start, qdummyE, next_fromOinA, result);
                this->scanLastPartition_OrgsAft_gOverlaps(l, a, qdummySE, next_fromOaftA, result);
            }
            else
            {
                // Lemma 1
                this->scanFirstPartition_OrgsIn_gOverlaps(l, a, Q.start, next_fromOinA, result);
                this->scanFirstPartition_OrgsAft_gOverlaps(l, a, next_fromOaftA, result);
            }

            // Lemma 1, 3
            this->scanFirstPartition_RepsIn_gOverlaps(l, a, qdummyS, next_fromRinA, result);
            this->scanFirstPartition_RepsAft_gOverlaps(l, a, next_fromRaftA, result);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_OrgsIn_gOverlaps(l, a+1, b-1, next_fromOinAB, next_toOinAB, result);
                this->scanPartitions_OrgsAft_gOverlaps(l, a+1, b-1, next_fromOaftAB, next_toOaftAB, result);

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanLastPartition_OrgsIn_gOverlaps(l, b, qdummyE, next_fromOinB, result);
                this->scanLastPartition_OrgsAft_gOverlaps(l, b, qdummySE, next_fromOaftB, result);
            }

            if (b%2) //last bit of b is 1
                foundone = 1;
            if (!(a%2)) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }

    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterBegin = this->pOrgsIn[this->numBits].begin();
        iterEnd = this->pOrgsIn[this->numBits].end();
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= iter->id;
#endif
        }
    }
    else
    {
        // Comparisons needed
        iterBegin = this->pOrgsIn[this->numBits].begin();
        iterEnd = lower_bound(iterBegin, this->pOrgsIn[this->numBits].end(), qdummyE);
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (Q.start <= iter->end)
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
