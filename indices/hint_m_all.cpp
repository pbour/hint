/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   George Christodoulou
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

#include "hint_m.h"



inline void HINT_M_ALL::updateCounters(const Record &r)
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


inline void HINT_M_ALL::updatePartitions(const Record &r)
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
                    this->pRepsInTmp[level][this->pRepsIn_offsets[level][a]++] = RecordEnd(r.id, r.end);
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
                    this->pOrgsInTmp[level][this->pOrgsIn_offsets[level][a]++] = Record(r.id, r.start, r.end);
                }
                else
                {
                    this->pOrgsAftTmp[level][this->pOrgsAft_offsets[level][a]++] = RecordStart(r.id, r.start);
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
                    this->pOrgsInTmp[level][this->pOrgsIn_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
                }
                else
                {
                    this->pOrgsAftTmp[level][this->pOrgsAft_offsets[level][prevb]++] = RecordStart(r.id, r.start);
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsInTmp[level][this->pRepsIn_offsets[level][prevb]++] = RecordEnd(r.id, r.end);
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


HINT_M_ALL::HINT_M_ALL(const Relation &R, const unsigned int numBits, const unsigned int maxBits)  : HierarchicalIndex(R, numBits, maxBits)
{
    OffsetEntry_ALL_OrgsIn  dummySE;
    OffsetEntry_ALL_OrgsAft dummyS;
    OffsetEntry_ALL_RepsIn  dummyE;
    OffsetEntry_ALL_RepsAft dummyI;
    Offsets_ALL_OrgsIn_Iterator  iterSEO, iterSEOStart, iterSEOEnd;
    Offsets_ALL_OrgsAft_Iterator iterSO, iterSOStart, iterSOEnd;
    Offsets_ALL_RepsIn_Iterator  iterEO, iterEOStart, iterEOEnd;
    Offsets_ALL_RepsAft_Iterator iterIO, iterIOBegin, iterIOEnd;
    PartitionId tmp = -1;
    
    
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
    this->pOrgsInTmp  = new Relation[this->height];
    this->pOrgsAftTmp = new RelationStart[this->height];
    this->pRepsInTmp  = new RelationEnd[this->height];
    this->pRepsAft    = new RelationId[this->height];
    
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
        
        this->pOrgsInTmp[l].resize(sumOin);
        this->pOrgsAftTmp[l].resize(sumOaft);
        this->pRepsInTmp[l].resize(sumRin);
        this->pRepsAft[l].resize(sumRaft);
    }
    
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    
    // Step 4: sort partition contents; first need to reset the offsets
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
    }
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        for (auto pId = 0; pId < cnt; pId++)
        {
            sort(this->pOrgsInTmp[l].begin()+this->pOrgsIn_offsets[l][pId], this->pOrgsInTmp[l].begin()+this->pOrgsIn_offsets[l][pId+1]);
            sort(this->pOrgsAftTmp[l].begin()+this->pOrgsAft_offsets[l][pId], this->pOrgsAftTmp[l].begin()+this->pOrgsAft_offsets[l][pId+1]);
            sort(this->pRepsInTmp[l].begin()+this->pRepsIn_offsets[l][pId], this->pRepsInTmp[l].begin()+this->pRepsIn_offsets[l][pId+1]);
        }
    }
    
    
    // Step 5: break-down data to create id- and timestamp-dedicated arrays; free auxiliary memory.
    this->pOrgsInIds  = new RelationId[this->height];
    this->pOrgsAftIds = new RelationId[this->height];
    this->pRepsInIds  = new RelationId[this->height];
    this->pOrgsInTimestamps = new vector<pair<Timestamp, Timestamp> >[this->height];
    this->pOrgsAftTimestamp = new vector<Timestamp>[this->height];
    this->pRepsInTimestamp  = new vector<Timestamp>[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pOrgsInTmp[l].size();
        
        this->pOrgsInIds[l].resize(cnt);
        this->pOrgsInTimestamps[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsInIds[l][j] = this->pOrgsInTmp[l][j].id;
            this->pOrgsInTimestamps[l][j].first = this->pOrgsInTmp[l][j].start;
            this->pOrgsInTimestamps[l][j].second = this->pOrgsInTmp[l][j].end;
        }
        
        cnt = pOrgsAftTmp[l].size();
        this->pOrgsAftIds[l].resize(cnt);
        this->pOrgsAftTimestamp[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsAftIds[l][j] = this->pOrgsAftTmp[l][j].id;
            this->pOrgsAftTimestamp[l][j] = this->pOrgsAftTmp[l][j].start;
        }
        
        
        cnt = pRepsInTmp[l].size();
        this->pRepsInIds[l].resize(cnt);
        this->pRepsInTimestamp[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pRepsInIds[l][j] = this->pRepsInTmp[l][j].id;
            this->pRepsInTimestamp[l][j] = this->pRepsInTmp[l][j].end;
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
    
    delete[] this->pOrgsInTmp;
    delete[] this->pOrgsAftTmp;
    delete[] this->pRepsInTmp;
    
    
    //    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >   *pOrgsIn_ioffsets;
    //    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> > *pOrgsAft_ioffsets;
    //    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> > *pRepsIn_ioffsets;
    //    vector<tuple<Timestamp, RelationIdIterator, PartitionId> > *pRepsAft_ioffsets;
    
    // Step 4: create offset pointers
    this->pOrgsIn_ioffsets  = new Offsets_ALL_OrgsIn[this->height];
    this->pOrgsAft_ioffsets = new Offsets_ALL_OrgsAft[this->height];
    this->pRepsIn_ioffsets  = new Offsets_ALL_RepsIn[this->height];
    this->pRepsAft_ioffsets = new Offsets_ALL_RepsAft[this->height];
    for (int l = this->height-1; l > -1; l--)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOaft = 0, sumRin = 0, sumRaft = 0;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            bool isEmpty = true;
            
            dummySE.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsIn_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOStart = this->pOrgsIn_ioffsets[l+1].begin();
                    iterSEOEnd = this->pOrgsIn_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOStart, iterSEOEnd, dummySE);//, CompareStartEndOffesetsByTimestamp_WithCM);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOStart): -1;
                }
                this->pOrgsIn_ioffsets[l].push_back(OffsetEntry_ALL_OrgsIn(pId, this->pOrgsInIds[l].begin()+sumOin, this->pOrgsInTimestamps[l].begin()+sumOin, tmp));
            }
            
            dummyS.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsAft_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSOStart = this->pOrgsAft_ioffsets[l+1].begin();
                    iterSOEnd = this->pOrgsAft_ioffsets[l+1].end();
                    iterSO = lower_bound(iterSOStart, iterSOEnd, dummyS);//, CompareStartOffesetsByTimestamp_WithCM);
                    tmp = (iterSO != iterSOEnd)? (iterSO-iterSOStart): -1;
                }
                this->pOrgsAft_ioffsets[l].push_back(OffsetEntry_ALL_OrgsAft(pId, this->pOrgsAftIds[l].begin()+sumOaft, this->pOrgsAftTimestamp[l].begin()+sumOaft, tmp));
            }
            
            dummyE.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsIn_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterEOStart = this->pRepsIn_ioffsets[l+1].begin();
                    iterEOEnd = this->pRepsIn_ioffsets[l+1].end();
                    iterEO = lower_bound(iterEOStart, iterEOEnd, dummyE);//, CompareEndOffesetsByTimestamp_WithCM);
                    tmp = (iterEO != iterEOEnd)? (iterEO-iterEOStart): -1;
                }
                this->pRepsIn_ioffsets[l].push_back(OffsetEntry_ALL_RepsIn(pId, this->pRepsInIds[l].begin()+sumRin, this->pRepsInTimestamp[l].begin()+sumRin, tmp));
            }
            
            dummyI.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsAft_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterIOBegin = this->pRepsAft_ioffsets[l+1].begin();
                    iterIOEnd = this->pRepsAft_ioffsets[l+1].end();
                    iterIO = lower_bound(iterIOBegin, iterIOEnd, dummyI);//, CompareIdOffesetsByTimestamp);
                    tmp = (iterIO != iterIOEnd)? (iterIO-iterIOBegin): -1;
                }
                this->pRepsAft_ioffsets[l].push_back(OffsetEntry_ALL_RepsAft(pId, this->pRepsAft[l].begin()+sumRaft, tmp));
            }
            
            sumOin += this->pOrgsIn_sizes[l][pId];
            sumOaft += this->pOrgsAft_sizes[l][pId];
            sumRin += this->pRepsIn_sizes[l][pId];
            sumRaft += this->pRepsAft_sizes[l][pId];
            
            if (isEmpty)
                this->numEmptyPartitions++;
        }
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
}


void HINT_M_ALL::getStats()
{
    size_t sum = 0;
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        this->numPartitions += cnt;

        this->numOriginalsIn  += this->pOrgsInIds[l].size();
        this->numOriginalsAft += this->pOrgsAftIds[l].size();
        this->numReplicasIn   += this->pRepsInIds[l].size();
        this->numReplicasAft  += this->pRepsAft[l].size();
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasAft)/(this->numPartitions-numEmptyPartitions);
}


HINT_M_ALL::~HINT_M_ALL()
{
    delete[] this->pOrgsIn_ioffsets;
    delete[] this->pOrgsAft_ioffsets;
    delete[] this->pRepsIn_ioffsets;
    delete[] this->pRepsAft_ioffsets;
    
    delete[] this->pOrgsInIds;
    delete[] this->pOrgsInTimestamps;
    delete[] this->pOrgsAftIds;
    delete[] this->pOrgsAftTimestamp;
    delete[] this->pRepsInIds;
    delete[] this->pRepsInTimestamp;
    delete[] this->pRepsAft;
}


// Auxiliary functions to determine exactly how to scan a partition.
inline bool HINT_M_ALL::getBounds_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd)
{
    OffsetEntry_ALL_OrgsIn qdummy;
    Offsets_ALL_OrgsIn_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareStartEndOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterIStart = iterIO->iterI;
                iterIEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterI : this->pOrgsInIds[level].end());

                next_from = iterIO->pid;

                return true;
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
                iterIStart = (this->pOrgsIn_ioffsets[level][from]).iterI;
                iterIEnd = ((from+1 != cnt) ? (this->pOrgsIn_ioffsets[level][from+1]).iterI : this->pOrgsInIds[level].end());

                next_from = (this->pOrgsIn_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBounds_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<pair<Timestamp, Timestamp> >::iterator &iterBegin, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI)
{
    OffsetEntry_ALL_OrgsIn qdummy;
    Offsets_ALL_OrgsIn_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareStartEndOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterI = iterIO->iterI;
                iterBegin = iterIO->iterT;
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : this->pOrgsInTimestamps[level].end());

                next_from = iterIO->pid;

                return true;
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
                iterI = (this->pOrgsIn_ioffsets[level][from]).iterI;
                iterBegin = (this->pOrgsIn_ioffsets[level][from]).iterT;
                iterEnd = ((from+1 != cnt) ? (this->pOrgsIn_ioffsets[level][from+1]).iterT : this->pOrgsInTimestamps[level].end());

                next_from = (this->pOrgsIn_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBoundsS_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<pair<Timestamp, Timestamp> >::iterator &iterBegin, RelationIdIterator &iterI)
{
    OffsetEntry_ALL_OrgsIn qdummy;
    Offsets_ALL_OrgsIn_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareStartEndOffesetsByTimestamp_WithCM);
//            if ((iterIO != iterIOEnd) && (get<0>(this->pOrgsIn_ioffsets[level][from]) >= t))
            if (iterIO != iterIOEnd)
            {
                iterI = iterIO->iterI;
                iterBegin = iterIO->iterT;

                next_from = iterIO->pid;

                return true;
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

            if ((from != cnt) && (from != -1))
            {
                iterI = (this->pOrgsIn_ioffsets[level][from]).iterI;
                iterBegin = (this->pOrgsIn_ioffsets[level][from]).iterT;

                next_from = (this->pOrgsIn_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBoundsE_OrgsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI)
{
    OffsetEntry_ALL_OrgsIn qdummy;
    Offsets_ALL_OrgsIn_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareStartEndOffesetsByTimestamp_WithCM);
            iterIO--;
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= t))
            {
                iterI = this->pOrgsInIds[level].begin();
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : this->pOrgsInTimestamps[level].end());

                next_from = iterIO->pid;

                return true;
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
                from--;
            }
            else if (tmp >= t)
            {
                while (((this->pOrgsIn_ioffsets[level][from]).tstamp >= t) && (from > -1))
                    from--;
//                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != t) || (from == -1))
//                    from++;
            }

            if ((from != -1) && (from != cnt))
            {
                iterI = this->pOrgsInIds[level].begin();
                iterEnd = ((from+1 != cnt) ? (this->pOrgsIn_ioffsets[level][from+1]).iterT : this->pOrgsInTimestamps[level].end());

                next_from = (this->pOrgsIn_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBounds_OrgsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd)
{
    OffsetEntry_ALL_OrgsIn qdummyA, qdummyB;
    Offsets_ALL_OrgsIn_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
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
            iterIOBegin = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummyA);//, CompareStartEndOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= b))
            {
                next_from = iterIO->pid;

                qdummyB.tstamp = b;
                iterIStart = iterIO->iterI;

                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB);//, CompareStartEndOffesetsByTimestamp_WithCM);
//                iterIO2 = iterIO;
//                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
//                    iterIO2++;

                iterIEnd = ((iterIO2 != iterIOEnd) ? iterIEnd = iterIO2->iterI: this->pOrgsInIds[level].end());

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
//                else if (tmp <= b)
            else if (tmp == b)
            {
                while (((this->pOrgsIn_ioffsets[level][to]).tstamp <= b) && (to < cnt))
                    to++;
            }

            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterIStart = (this->pOrgsIn_ioffsets[level][from]).iterI;
                iterIEnd   = (to != cnt) ? (this->pOrgsIn_ioffsets[level][to]).iterI : this->pOrgsInIds[level].end();

                next_from = (this->pOrgsIn_ioffsets[level][from]).pid;
                next_to   = (to != cnt) ? (this->pOrgsIn_ioffsets[level][to]).pid : -1;
                

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


inline bool HINT_M_ALL::getBounds_OrgsAft(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd)
{
    OffsetEntry_ALL_OrgsAft qdummy;
    Offsets_ALL_OrgsAft_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsAft_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pOrgsAft_ioffsets[level].begin();
            iterIOEnd = this->pOrgsAft_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareStartOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterIStart = iterIO->iterI;
                iterIEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterI : this->pOrgsAftIds[level].end());

                next_from = iterIO->pid;

                return true;
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
                iterIStart = (this->pOrgsAft_ioffsets[level][from]).iterI;
                iterIEnd = ((from+1 != cnt) ? (this->pOrgsAft_ioffsets[level][from+1]).iterI : this->pOrgsAftIds[level].end());

                next_from = (this->pOrgsAft_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBounds_OrgsAft(unsigned int level, Timestamp t, PartitionId &next_from, vector<Timestamp>::iterator &iterBegin, vector<Timestamp>::iterator &iterEnd, RelationIdIterator &iterI)
{
    OffsetEntry_ALL_OrgsAft qdummy;
    Offsets_ALL_OrgsAft_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsAft_ioffsets[level].size();
    PartitionId from = next_from;

    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pOrgsAft_ioffsets[level].begin();
            iterIOEnd = this->pOrgsAft_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareStartOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterI = iterIO->iterI;
                iterBegin = iterIO->iterT;
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : this->pOrgsAftTimestamp[level].end());

                next_from = iterIO->pid;

                return true;
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
                iterI = (this->pOrgsAft_ioffsets[level][from]).iterI;
                iterBegin = (this->pOrgsAft_ioffsets[level][from]).iterT;
                iterEnd = ((from+1 != cnt) ? (this->pOrgsAft_ioffsets[level][from+1]).iterT : this->pOrgsAftTimestamp[level].end());

                next_from = (this->pOrgsAft_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBoundsS_OrgsAft(unsigned int level, Timestamp t, PartitionId &next_from, vector<Timestamp>::iterator &iterBegin, RelationIdIterator &iterI)
{
    OffsetEntry_ALL_OrgsAft qdummy;
    Offsets_ALL_OrgsAft_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsAft_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pOrgsAft_ioffsets[level].begin();
            iterIOEnd = this->pOrgsAft_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareStartOffesetsByTimestamp_WithCM);
//            if ((iterIO != iterIOEnd) && (get<0>(this->pOrgsAft_ioffsets[level][from]) >= t))
            if (iterIO != iterIOEnd)
            {
                iterI = iterIO->iterI;
                iterBegin = iterIO->iterT;

                next_from = iterIO->pid;

                return true;
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

            if ((from != cnt) && (from != -1))
            {
                iterI = (this->pOrgsAft_ioffsets[level][from]).iterI;
                iterBegin = (this->pOrgsAft_ioffsets[level][from]).iterT;

                next_from = (this->pOrgsAft_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBounds_OrgsAft(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd)
{
    OffsetEntry_ALL_OrgsAft qdummyA, qdummyB;
    Offsets_ALL_OrgsAft_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsAft_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (to == -1))
        {
            qdummyA.tstamp = a;
            iterIOBegin = this->pOrgsAft_ioffsets[level].begin();
            iterIOEnd = this->pOrgsAft_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummyA);//, CompareStartOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= b))
            {
                next_from = iterIO->pid;

                qdummyB.tstamp = b;
                iterIStart = iterIO->iterI;

                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB);//, CompareStartOffesetsByTimestamp_WithCM);
//                iterIO2 = iterIO;
//                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
//                    iterIO2++;

                iterIEnd = ((iterIO2 != iterIOEnd) ? iterIEnd = iterIO2->iterI : this->pOrgsAftIds[level].end());

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
//                else if (tmp <= b)
            else if (tmp == b)
            {
                while (((this->pOrgsAft_ioffsets[level][to]).tstamp <= b) && (to < cnt))
                    to++;
            }

            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterIStart = (this->pOrgsAft_ioffsets[level][from]).iterI;
                iterIEnd   = (to != cnt) ? (this->pOrgsAft_ioffsets[level][to]).iterI : this->pOrgsAftIds[level].end();

                next_from = (this->pOrgsAft_ioffsets[level][from]).pid;
                next_to   = (to != cnt) ? (this->pOrgsAft_ioffsets[level][to]).pid : -1;

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


inline bool HINT_M_ALL::getBounds_OrgsAft(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, vector<Timestamp>::iterator &iterBegin, vector<Timestamp>::iterator &iterEnd, RelationIdIterator &iterI)
{
    OffsetEntry_ALL_OrgsAft qdummyA, qdummyB;
    Offsets_ALL_OrgsAft_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsAft_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (to == -1))
        {
            qdummyA.tstamp = a;
            iterIOBegin = this->pOrgsAft_ioffsets[level].begin();
            iterIOEnd = this->pOrgsAft_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummyA);//, CompareStartOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= b))
            {
                next_from = iterIO->pid;

                qdummyB.tstamp = b;
                iterI = iterIO->iterI;
                iterBegin = iterIO->iterT;

                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB);//, CompareStartOffesetsByTimestamp_WithCM);
//                iterIO2 = iterIO;
//                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
//                    iterIO2++;

                iterEnd = ((iterIO2 != iterIOEnd) ? iterEnd = iterIO2->iterT: this->pOrgsAftTimestamp[level].end());

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
//                else if (tmp <= b)
            else if (tmp == b)
            {
                while (((this->pOrgsAft_ioffsets[level][to]).tstamp <= b) && (to < cnt))
                    to++;
            }

            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterI = (this->pOrgsAft_ioffsets[level][from]).iterI;
                iterBegin = (this->pOrgsAft_ioffsets[level][from]).iterT;
                iterEnd   = (to != cnt) ? (this->pOrgsAft_ioffsets[level][to]).iterT : this->pOrgsAftTimestamp[level].end();

                next_from = (this->pOrgsAft_ioffsets[level][from]).pid;
                next_to   = (to != cnt) ? (this->pOrgsAft_ioffsets[level][to]).pid : -1;

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


inline bool HINT_M_ALL::getBounds_RepsIn(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd)
{
    OffsetEntry_ALL_RepsIn qdummy;
    Offsets_ALL_RepsIn_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pRepsIn_ioffsets[level].begin();
            iterIOEnd = this->pRepsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareStartOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterIStart = iterIO->iterI;
                iterIEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterI : this->pRepsInIds[level].end());

                next_from = iterIO->pid;

                return true;
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
                iterIStart = (this->pRepsIn_ioffsets[level][from]).iterI;
                iterIEnd = ((from+1 != cnt) ? (this->pRepsIn_ioffsets[level][from+1]).iterI : this->pRepsInIds[level].end());

                next_from = (this->pRepsIn_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;
                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBounds_RepsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<Timestamp>::iterator &iterBegin, vector<Timestamp>::iterator  &iterEnd, RelationIdIterator &iterI)
{
    OffsetEntry_ALL_RepsIn qdummy;
    Offsets_ALL_RepsIn_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pRepsIn_ioffsets[level].begin();
            iterIOEnd = this->pRepsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareEndOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterI = iterIO->iterI;
                iterBegin = iterIO->iterT;
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : this->pRepsInTimestamp[level].end());

                next_from = iterIO->pid;

                return true;
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
                iterI = (this->pRepsIn_ioffsets[level][from]).iterI;
                iterBegin = (this->pRepsIn_ioffsets[level][from]).iterT;
                iterEnd = ((from+1 != cnt) ? (this->pRepsIn_ioffsets[level][from+1]).iterT : this->pRepsInTimestamp[level].end());

                next_from = (this->pRepsIn_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBoundsE_RepsIn(unsigned int level, Timestamp t, PartitionId &next_from, vector<Timestamp>::iterator &iterEnd, RelationIdIterator &iterI)
{
    OffsetEntry_ALL_RepsIn qdummy;
    Offsets_ALL_RepsIn_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pRepsIn_ioffsets[level].begin();
            iterIOEnd = this->pRepsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareEndOffesetsByTimestamp_WithCM);
            iterIO--;
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= t))
            {
                iterI = this->pRepsInIds[level].begin();
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : this->pRepsInTimestamp[level].end());

                next_from = iterIO->pid;

                return true;
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
                from--;
            }
            else if (tmp >= t)
            {
                while (((this->pRepsIn_ioffsets[level][from]).tstamp >= t) && (from > -1))
                    from--;
//                if ((get<0>(this->pRepsIn_ioffsets[level][from]) != t) || (from == -1))
//                    from++;
            }

            if ((from != -1) && (from != cnt))
            {
                iterI = this->pRepsInIds[level].begin();
                iterEnd = ((from+1 != cnt) ? (this->pRepsIn_ioffsets[level][from+1]).iterT : this->pRepsInTimestamp[level].end());

                next_from = (this->pRepsIn_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


inline bool HINT_M_ALL::getBounds_RepsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd)
{
    OffsetEntry_ALL_RepsIn qdummyA, qdummyB;
    Offsets_ALL_RepsIn_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;


    if (cnt > 0)
    {
        // Adjusting pointers.
        if ((from == -1) || (to == -1))
        {
            qdummyA.tstamp = a;
            iterIOBegin = this->pRepsIn_ioffsets[level].begin();
            iterIOEnd = this->pRepsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummyA);//, CompareStartOffesetsByTimestamp_WithCM);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= b))
            {
                next_from = iterIO->pid;

                qdummyB.tstamp = b;
                iterIStart = iterIO->iterI;

                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB);//, CompareStartOffesetsByTimestamp_WithCM);
//                iterIO2 = iterIO;
//                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
//                    iterIO2++;

                iterIEnd = ((iterIO2 != iterIOEnd) ? iterIEnd = iterIO2->iterI : this->pRepsInIds[level].end());

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
            Timestamp tmp = (this->pRepsIn_ioffsets[level][from]).tstamp;
            if (tmp < a)
            {
                while (((this->pRepsIn_ioffsets[level][from]).tstamp < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while (((this->pRepsIn_ioffsets[level][from]).tstamp > a) && (from > -1))
                    from--;
                if (((this->pRepsIn_ioffsets[level][from]).tstamp != a) || (from == -1))
                    from++;
            }

            tmp = (this->pRepsIn_ioffsets[level][to]).tstamp;
            if (tmp > b)
            {
                while (((this->pRepsIn_ioffsets[level][to]).tstamp > b) && (to > -1))
                    to--;
                to++;
            }
//                else if (tmp <= b)
            else if (tmp == b)
            {
                while (((this->pRepsIn_ioffsets[level][to]).tstamp <= b) && (to < cnt))
                    to++;
            }

            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterIStart = (this->pRepsIn_ioffsets[level][from]).iterI;
                iterIEnd   = (to != cnt) ? (this->pRepsIn_ioffsets[level][to]).iterI : this->pRepsInIds[level].end();

                next_from = (this->pRepsIn_ioffsets[level][from]).pid;
                next_to   = (to != cnt) ? (this->pRepsIn_ioffsets[level][to]).pid : -1;

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


inline bool HINT_M_ALL::getBounds_RepsAft(unsigned int level, Timestamp t, PartitionId &next_from, RelationIdIterator &iterIStart, RelationIdIterator &iterIEnd)
{
    OffsetEntry_ALL_RepsAft qdummy;
    Offsets_ALL_RepsAft_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pRepsAft_ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = this->pRepsAft_ioffsets[level].begin();
            iterIOEnd = this->pRepsAft_ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareIdOffesetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterIStart = iterIO->iterI;
                iterIEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterI : this->pRepsAft[level].end());

                next_from = iterIO->pid;

                return true;
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
                iterIStart = (this->pRepsAft_ioffsets[level][from]).iterI;
                iterIEnd = ((from+1 != cnt) ? (this->pRepsAft_ioffsets[level][from+1]).iterI : this->pRepsAft[level].end());

                next_from = (this->pRepsAft_ioffsets[level][from]).pid;

                return true;
            }
            else
            {
                next_from = -1;

                return false;
            }
        }
    }
    else
    {
        next_from = -1;

        return false;
    }
}


// Auxiliary functions to scan partitions.
inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Equals(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qdummyS.first == iter->first))
        {
            if (qend == iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Equals(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qstart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qstart == (*iter)))
        {
            vcand[(*iterI)] = 1;
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_RepsIn_Equals(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qend);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qend == (*iter)))
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Starts(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qdummyS.first == iter->first))
        {
            if (qend < iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Starts(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qstart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qstart == (*iter)))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Starts(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qstart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qstart == (*iter)))
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_RepsIn_Starts(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qend+1);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_RepsAft_Starts(unsigned int level, Timestamp a, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;


    if (this->getBounds_RepsAft(level, a, next_from, iterIStart, iterIEnd))
    {
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Started(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qdummyS.first == iter->first))
        {
            if (qend > iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Started(unsigned int level, Timestamp a, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qdummyS.first == iter->first))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
            iter++;
        }
    }
}

inline void HINT_M_ALL::scanLastPartition_RepsIn_Started(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        vector<Timestamp>::iterator pivot = lower_bound(iterBegin, iterEnd, qend);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanPartitions_RepsIn_Started(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, boost::dynamic_bitset<> &vcand, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;


    if (this->getBounds_RepsIn(level, a, b, next_from, next_to, iterIStart, iterIEnd))
    {
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsIn_Finishes(unsigned int level, Timestamp b, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (qend == iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Finishes(unsigned int level, Timestamp a, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Finishes(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        vector<Timestamp>::iterator pivot = lower_bound(iterBegin, iterEnd, qstart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_RepsIn_Finishes(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qend);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qend == (*iter)))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_RepsIn_Finishes(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qend);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qend == (*iter)))
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsIn_Finished(unsigned int level, Timestamp b, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
            if (qend == iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsIn_Finished(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qend == iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Finished(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qstart+1);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanPartitions_OrgsAft_Finished(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, boost::dynamic_bitset<> &vcand, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    PartitionId from = next_from, to = next_to;


    if (this->getBounds_OrgsAft(level, a, b, next_from, next_to, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Meets(unsigned int level, Timestamp a, Timestamp qstart, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qstart == iter->first))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Meets(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qstart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qstart == (*iter)))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsIn_Met(unsigned int level, Timestamp a, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qend == iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_RepsIn_Met(unsigned int level, Timestamp a, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qend);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qend == (*iter)))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Overlaps(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qend > iter->first))
        {
            if (qend < iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Overlaps(unsigned int level, Timestamp a, Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qstart+1);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qend > (*iter)))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsIn_Overlaps(unsigned int level, Timestamp b, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyE, CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (qdummyE.first < iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= *iterI;
#endif
            }
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsAft_Overlaps(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        vector<Timestamp>::iterator pivot = lower_bound(iterBegin, iterEnd, qend);
        for (iter = iterBegin; iter != pivot; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= *iterI;
#endif

            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Overlapped(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
             if ((qdummyS.first < iter->second) && (qend > iter->second))
             {
 #ifdef WORKLOAD_COUNT
                 result++;
 #else
                 result ^= (*iterI);
 #endif
             }
             iterI++;
         }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Overlapped(unsigned int level, Timestamp a, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
             if (qdummyS.first < iter->second)
             {
 #ifdef WORKLOAD_COUNT
                 result++;
 #else
                 result ^= (*iterI);
 #endif
             }
             iterI++;
         }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Overlapped(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
             if (qstart < iter->second)
             {
 #ifdef WORKLOAD_COUNT
                 result++;
 #else
                 result ^= (*iterI);
 #endif
             }
             iterI++;
         }
    }
}


inline void HINT_M_ALL::scanFirstPartition_RepsIn_Overlapped(unsigned int level, Timestamp a, Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qstart+1);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && ((*iter) < qend))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_RepsIn_Overlapped(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qstart+1);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_RepsIn_Overlapped(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        vector<Timestamp>::iterator pivot = lower_bound(iterBegin, iterEnd, qend);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Contains(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
            if (qend > iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Contains(unsigned int level, Timestamp a, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsIn_Contains(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qend > iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Contains(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qstart+1);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Contained(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
//        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyS);
        for (iter = iterBegin; iter != pivot; iter++)
        {
//            if (iter->first > qdummyS.first)
//                break;

            if (qend < iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Contained(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        vector<Timestamp>::iterator pivot = lower_bound(iterBegin, iterEnd, qstart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_Contained(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        vector<Timestamp>::iterator pivot = lower_bound(iterBegin, iterEnd, qstart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_RepsAft_Contained(unsigned int level, Timestamp a, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;


    if (this->getBounds_RepsAft(level, a, next_from, iterIStart, iterIEnd))
    {
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
            if (vcand[(*iterI)] == 1)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            else
                vcand[(*iterI)] = 1;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsIn_Precedes(unsigned int level, Timestamp b, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyE);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsAft_Precedes(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qend);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_ALL::scanPartitions_OrgsIn_Precedes(unsigned int level, Timestamp b, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBoundsS_OrgsIn(level, b, next_from, iterBegin, iterI))
    {
        iterEnd = this->pOrgsInTimestamps[level].end();
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanPartitions_OrgsAft_Precedes(unsigned int level, Timestamp b, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBoundsS_OrgsAft(level, b, next_from, iterBegin, iterI))
    {
        iterEnd = this->pOrgsAftTimestamp[level].end();
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= *iterI;
#endif

            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_Preceded(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qstart > iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_RepsIn_Preceded(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >::iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (this->getBounds_RepsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        vector<Timestamp>::iterator pivot = lower_bound(iterBegin, iterEnd, qstart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= *iterI;
#endif

            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanPartitions_OrgsIn_Preceded(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >::iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (this->getBoundsE_OrgsIn(level, a, next_from, iterEnd, iterI))
    {
        iterBegin = this->pOrgsInTimestamps[level].begin();
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanPartitions_RepsIn_Preceded(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >::iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from;


    if (this->getBoundsE_RepsIn(level, a, next_from, iterEnd, iterI))
    {
        iterBegin = this->pRepsInTimestamp[level].begin();
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;
    tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> qdummy;


    if (this->getBounds_OrgsIn(level, a, next_from, iterIStart, iterIEnd))
    {
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp qstart, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyE);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (qstart <= iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qstart <= iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_OrgsAft_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> qdummy;


    if (this->getBounds_OrgsAft(level, a, next_from, iterIStart, iterIEnd))
    {
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_RepsIn_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> qdummy;


    if (this->getBounds_RepsIn(level, a, next_from, iterIStart, iterIEnd))
    {
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_RepsIn_gOverlaps(unsigned int level, Timestamp a, RecordEnd qdummyS, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_RepsIn(level, a, next_from, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, qdummyS.end);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanFirstPartition_RepsAft_gOverlaps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;


    if (this->getBounds_RepsAft(level, a, next_from, iterIStart, iterIEnd))
    {
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsIn_gOverlaps(unsigned int level, Timestamp b, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsIn(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyE);
        for (iter = iterBegin; iter != pivot; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= *iterI;
#endif

            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanLastPartition_OrgsAft_gOverlaps(unsigned int level, Timestamp b, RecordStart qdummySE, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds_OrgsAft(level, b, next_from, iterBegin, iterEnd, iterI))
    {
        vector<Timestamp>::iterator pivot = lower_bound(iterBegin, iterEnd, qdummySE.start);
        for (iter = iterBegin; iter != pivot; iter++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= *iterI;
#endif

            iterI++;
        }
    }
}


inline void HINT_M_ALL::scanPartitions_OrgsIn_gOverlaps(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;


    if (this->getBounds_OrgsIn(level, a, b, next_from, next_to, iterIStart, iterIEnd))
    {
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}


inline void HINT_M_ALL::scanPartitions_OrgsAft_gOverlaps(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIdIterator iterI, iterIStart, iterIEnd;


    if (this->getBounds_OrgsAft(level, a, b, next_from, next_to, iterIStart, iterIEnd))
    {
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}
//
//
//
//
//inline void HINT_M_ALL::scanFirstPartition_OrgsIn_gContains(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
//{
//    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
//    RelationIdIterator iterI;
//
//
//    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
//    {
//        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
//        iterI += iter-iterBegin;
//        while (iter != iterEnd)
//        {
//            if (qend >= iter->second)
//            {
//#ifdef WORKLOAD_COUNT
//                result++;
//#else
//                result ^= (*iterI);
//#endif
//            }
//            iter++;
//            iterI++;
//        }
//    }
//}
//
//
//inline void HINT_M_ALL::scanFirstPartition_OrgsIn_gContains(unsigned int level, Timestamp a, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
//{
//    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
//    RelationIdIterator iterI;
//
//
//    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
//    {
//        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
//        iterI += iter-iterBegin;
//        while (iter != iterEnd)
//        {
//#ifdef WORKLOAD_COUNT
//            result++;
//#else
//            result ^= (*iterI);
//#endif
//
//            iterI++;
//            iter++;
//        }
//    }
//}
//
//
//inline void HINT_M_ALL::scanFirstPartition_OrgsAft_gContains(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
//{
//    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
//    RelationIdIterator iterI;
//
//
//    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
//    {
//        iter = lower_bound(iterBegin, iterEnd, qstart);
//        iterI += iter-iterBegin;
//        while (iter != iterEnd)
//        {
//            if (vcand[(*iterI)] == 1)
//            {
//#ifdef WORKLOAD_COUNT
//                result++;
//#else
//                result ^= (*iterI);
//#endif
//            }
//            else
//                vcand[(*iterI)] = 1;
//            iterI++;
//            iter++;
//        }
//    }
//}
//
//
//inline void HINT_M_ALL::scanLastPartition_OrgsIn_gContains(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, size_t &result)
//{
//    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
//    RelationIdIterator iterI;
//
//
//    if (this->getBounds_OrgsIn(level, b, next_from, iterBegin, iterEnd, iterI))
//    {
//        for (iter = iterBegin; iter != iterEnd; iter++)
//        {
//            if (qend >= iter->second)
//            {
//#ifdef WORKLOAD_COUNT
//                result++;
//#else
//                result ^= (*iterI);
//#endif
//            }
//            iterI++;
//        }
//    }
//}
//
//
//inline void HINT_M_ALL::scanLastPartition_RepsIn_gContains(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
//{
//    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
//    RelationIdIterator iterI;
//
//
//    if (this->getBounds_RepsIn(level, b, next_from, iterBegin, iterEnd, iterI))
//    {
//        vector<Timestamp>::iterator pivot = upper_bound(iterBegin, iterEnd, qend);
//        for (iter = iterBegin; iter != pivot; iter++)
//        {
//            if (vcand[(*iterI)] == 1)
//            {
//#ifdef WORKLOAD_COUNT
//                result++;
//#else
//                result ^= (*iterI);
//#endif
//            }
//            else
//                vcand[(*iterI)] = 1;
//            iterI++;
//        }
//    }
//}
//
//
//
//
//inline void HINT_M_ALL::scanFirstPartition_OrgsIn_gContained(unsigned int level, Timestamp a, Timestamp qend, pair<Timestamp, Timestamp> qdummyS, PartitionId &next_from, size_t &result)
//{
//    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
//    RelationIdIterator iterI;
//
//
//    if (this->getBounds_OrgsIn(level, a, next_from, iterBegin, iterEnd, iterI))
//    {
//        vector<pair<Timestamp, Timestamp> >::iterator pivot = upper_bound(iterBegin, iterEnd, qdummyS);
////        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyS);
//        for (iter = iterBegin; iter != pivot; iter++)
//        {
////            if (iter->first > qdummyS.first)
////                break;
//
//            if (qend <= iter->second)
//            {
//#ifdef WORKLOAD_COUNT
//                result++;
//#else
//                result ^= (*iterI);
//#endif
//            }
//            iterI++;
//        }
//    }
//}
//
//
//inline void HINT_M_ALL::scanFirstPartition_OrgsAft_gContained(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
//{
//    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
//    RelationIdIterator iterI;
//
//
//    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
//    {
//        vector<Timestamp>::iterator pivot = upper_bound(iterBegin, iterEnd, qstart);
//        for (iter = iterBegin; iter != pivot; iter++)
//        {
//#ifdef WORKLOAD_COUNT
//            result++;
//#else
//            result ^= (*iterI);
//#endif
//
//            iterI++;
//        }
//    }
//}
//
//
//inline void HINT_M_ALL::scanFirstPartition_OrgsAft_gContained(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
//{
//    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
//    RelationIdIterator iterI;
//
//
//    if (this->getBounds_OrgsAft(level, a, next_from, iterBegin, iterEnd, iterI))
//    {
//        vector<Timestamp>::iterator pivot = upper_bound(iterBegin, iterEnd, qstart);
//        for (iter = iterBegin; iter != pivot; iter++)
//        {
//            if (vcand[(*iterI)] == 1)
//            {
//#ifdef WORKLOAD_COUNT
//                result++;
//#else
//                result ^= (*iterI);
//#endif
//            }
//            else
//                vcand[(*iterI)] = 1;
//            iterI++;
//        }
//    }
//}
//
//

//inline void HINT_M_ALL::scanLastPartition_RepsIn_gContained(unsigned int level, Timestamp b, Timestamp qend, PartitionId &next_from, boost::dynamic_bitset<> &vcand, size_t &result)
//{
//    vector<Timestamp>::iterator iter, iterBegin, iterEnd;
//    RelationIdIterator iterI;
//
//    if (this->getBounds_RepsIn(level, b, next_from, iterBegin, iterEnd, iterI))
//    {
//        iter = lower_bound(iterBegin, iterEnd, qend);
//        iterI += iter-iterBegin;
//        while (iter != iterEnd)
//        {
//            if (vcand[(*iterI)] == 1)
//            {
//#ifdef WORKLOAD_COUNT
//                result++;
//#else
//                result ^= (*iterI);
//#endif
//            }
//            else
//                vcand[(*iterI)] = 1;
//            iter++;
//            iterI++;
//        }
//    }
//}
//
//


// Basic predicates of Allen's algebra
size_t HINT_M_ALL::executeBottomUp_Equals(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    Timestamp prevb;
    pair<Timestamp, Timestamp> qdummyS(Q.start, Q.start);
    bool foundzero = false;
    bool firstfound = false, lastfound = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinB = -1;
    boost::dynamic_bitset<> vcand(this->numIndexedRecords);
    Timestamp a_partition = -1, b_partition = -1;
    short int level = 0, a_level = -1, b_level = -1;


    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
                // Last partition found
                if ((a == b) && (!lastfound))
                {
                    b_partition = b;
                    b_level = level;
                    lastfound = 1;
                }
            }
            else
            {
                // First partition found
                this->scanFirstPartition_OrgsAft_Equals(level, a, Q.start, next_fromOaftA, vcand);
                a_partition = a;
                a_level = level;
                firstfound = 1;
            }
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            //b-=(int)(pow(2,level));
            b--;
            if ((!firstfound) && b<a)
            {
                // First partition found
                this->scanFirstPartition_OrgsAft_Equals(level, a, Q.start, next_fromOaftA, vcand);
                a_partition = a;
                a_level = level;

            }
            else
            {
                // Last partition found
                if (!lastfound)
                {
                    b_partition = prevb;
                    b_level = level;
                    lastfound = 1;
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }

    if (b_level == -1)
    {
        // TODO: root covered?
        this->scanFirstPartition_OrgsIn_Equals(a_level, a_partition, Q.end, qdummyS, next_fromOinA, result);
    }
    else
    {
        this->scanLastPartition_RepsIn_Equals(b_level, b_partition, Q.end, next_fromRinB, vcand, result);
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Starts(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyS(Q.start, Q.start);
    bool foundone = false;
//    bool foundoneB = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinB = -1, next_fromRaftB = -1;
    boost::dynamic_bitset<> vcand(this->numIndexedRecords);


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundoneA && foundoneB)
//            break;

        if (a == b)
        {
            this->scanFirstPartition_OrgsIn_Starts(l, a, Q.end, qdummyS, next_fromOinA, result);
            this->scanFirstPartition_OrgsAft_Starts(l, a, Q.start, next_fromOaftA, result);
        }
        else
        {
//            if (!foundone)
                this->scanFirstPartition_OrgsAft_Starts(l, a, Q.start, next_fromOaftA, vcand, result);

            this->scanLastPartition_RepsIn_Starts(l, b, Q.end, next_fromRinB, vcand, result);
            this->scanLastPartition_RepsAft_Starts(l, b, next_fromRaftB, vcand, result);    // Re-using function from gContained
        }

        if (a%2) //last bit of a is 1
            foundone = 1;
//        if (!(b%2)) //last bit of b is 0
//            foundzero = 1;
        a >>= 1; // a = a div 2
        b >>= 1;
    }

    // Handle root.
    if (!foundone)
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (Q.start == iter->first))
        {
            if (Q.end < iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iter++;
            iterI++;
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Started(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyS(Q.start, Q.start);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinB = -1, next_fromRinAB = -1, next_toRinAB = -1;
    boost::dynamic_bitset<> vcand(this->numIndexedRecords);


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
//        if (foundone)
//            break;

        if (a == b)
            this->scanFirstPartition_OrgsIn_Started(l, a, Q.end, qdummyS, next_fromOinA, result);
        else
        {
            if (!foundone)
            {
                this->scanFirstPartition_OrgsIn_Started(l, a, qdummyS, next_fromOinA, result);
                this->scanFirstPartition_OrgsAft_Starts(l, a, Q.start, next_fromOaftA, vcand, result);
            }

            this->scanPartitions_RepsIn_Started(l, a+1, b-1, next_fromRinAB, next_toRinAB, vcand, result);

            if (!foundzero)
                this->scanLastPartition_RepsIn_Started(l, b, Q.end, next_fromRinB, vcand, result);
        }

        if (a%2) //last bit of b is 1
            foundone = 1;
        if (!(b%2)) //last bit of a is 0
            foundzero = 1;
        a >>= 1; // a = a div 2
        b >>= 1;
    }

    // Handle root.
//    if (!(foundone && foundzero))
    if (!foundone)
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (Q.start == iter->first))
        {
            if (Q.end > iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iter++;
            iterI++;
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Finishes(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyS(Q.start, Q.start);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOaftA = -1, next_fromOinB = -1, next_fromRinB = -1, next_fromRaftA = -1;
    boost::dynamic_bitset<> vcand(this->numIndexedRecords);


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
//        if (foundzero)
//            break;

        if (a == b)
        {
            this->scanLastPartition_OrgsIn_Finishes(l, b, Q.end, qdummyS, next_fromOinB, result);
            this->scanLastPartition_RepsIn_Finishes(l, b, Q.end, next_fromRinB, result);
        }
        else
        {
            if (!foundzero)
                this->scanLastPartition_RepsIn_Finishes(l, b, Q.end, next_fromRinB, vcand, result);

            if (!foundone)
                this->scanFirstPartition_OrgsAft_Finishes(l, a, Q.start, next_fromOaftA, vcand, result); // Re-using function overlapped
            else
                this->scanFirstPartition_OrgsAft_Finishes(l, a, next_fromOaftA, vcand, result); // Re-using function overlapped

            this->scanLastPartition_RepsAft_Starts(l, a, next_fromRaftA, vcand, result);    // Re-using function from gContained, a and next_fromRaftA instead of b and next_fromRaftB
        }

        if (a%2) //last bit of a is 1
            foundone = 1;
        if (!(b%2)) //last bit of a is 0
            foundzero = 1;
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }

    // Handle root.
//    if (!(foundone && foundzero))
    if (!foundzero)
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (Q.end == iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= *iterI;
#endif
            }
            iterI++;
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Finished(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyS(Q.start+1, Q.start+1);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOaftA = -1, next_fromOinB = -1, next_fromRinB = -1, next_fromOaftAB = -1, next_toOaftAB = -1;
    boost::dynamic_bitset<> vcand(this->numIndexedRecords);


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
//        if (foundzero)
//            break;

        if (a == b)
            this->scanLastPartition_OrgsIn_Finished(l, b, Q.end, qdummyS, next_fromOinB, result);

        else
        {
            if (!foundzero)
            {
                this->scanLastPartition_OrgsIn_Finished(l, b, Q.end, next_fromOinB, result);
                this->scanLastPartition_RepsIn_Finishes(l, b, Q.end, next_fromRinB, vcand, result);
            }

            if (!foundone)
                this->scanFirstPartition_OrgsAft_Finished(l, a, Q.start, next_fromOaftA, vcand, result);        // Re-using function overlaps

            this->scanPartitions_OrgsAft_Finished(l, a+1, b-1, next_fromOaftAB, next_toOaftAB, vcand, result);
        }

        if (a%2) //last bit of a is 1
            foundone = 1;
        if (!(b%2)) //last bit of b is 0
            foundzero = 1;
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }

    // Handle root.
//    if (!(foundone && foundzero))
    if (!foundzero)
    {
        // Comparisons needed
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI = this->pOrgsInIds[this->numBits].begin() + (iter-iterBegin);
        while (iter != iterEnd)
        {
            if (Q.end == iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= *iterI;
#endif
            }
            iter++;
            iterI++;
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Meets(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyE(Q.end, Q.end);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinB = -1, next_fromOaftB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
        if (foundone)
            break;

        // Re-using start functions
        this->scanFirstPartition_OrgsIn_Meets(l, b, Q.end, qdummyE, next_fromOinB, result);
        this->scanFirstPartition_OrgsAft_Meets(l, b, Q.end, next_fromOaftB, result);

        if (b%2) //last bit of b is 1
            foundone = 1;
//        if (!(b%2)) //last bit of a is 0
//            foundzero = 1;
        b >>= 1; // b = b div 2
    }

    // Handle root.
//    if (!(foundone && foundzero))
    if (!foundone)
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iter = lower_bound(iterBegin, iterEnd, qdummyE, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (Q.end == iter->first))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iter++;
            iterI++;
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Met(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyE(Q.start, Q.start);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromRinA = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
        if (foundzero)
            break;

        // Re-use end functions
        this->scanLastPartition_OrgsIn_Met(l, a, Q.start, next_fromOinA, result);
        this->scanLastPartition_RepsIn_Met(l, a, Q.start, next_fromRinA, result);

//        if (a%2) //last bit of b is 1
//            foundone = 1;
        if (!(a%2)) //last bit of a is 0
            foundzero = 1;
        a >>= 1; // a = a div 2
    }

    // Handle root.
//    if (!(foundone && foundzero))
    if (!foundzero)
    {
        // Comparisons needed
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iterI = this->pOrgsInIds[this->numBits].begin();
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (Q.start == iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Overlaps(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyS(Q.start+1, Q.start+1);
    pair<Timestamp, Timestamp> qdummyE(Q.end, Q.end);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromOaftAB = -1, next_toOaftAB = -1, next_fromOinB = -1, next_fromOaftB = -1, next_fromRinB = -1, next_fromRaftB = -1;
    boost::dynamic_bitset<> vcand(this->numIndexedRecords);


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            if (a == b)
                return result;

            this->scanPartitions_OrgsAft_Finished(l, a+1, b-1, next_fromOaftAB, next_toOaftAB, vcand, result);

            this->scanLastPartition_RepsIn_Starts(l, b, Q.end, next_fromRinB, vcand, result);   // Re-using function from Starts
            this->scanLastPartition_RepsAft_Starts(l, b, next_fromRaftB, vcand, result);    // Re-using function from gContained

            this->scanLastPartition_OrgsIn_Overlaps(l, b, qdummyE, next_fromOinB, result);
            this->scanLastPartition_OrgsAft_Overlaps(l, b, Q.end, next_fromOaftB, result);

        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                this->scanFirstPartition_OrgsIn_Overlaps(l, a, Q.end, qdummyS, next_fromOinA, result);
                this->scanFirstPartition_OrgsAft_Overlaps(l, a, Q.start, Q.end, next_fromOaftA, result);
            }
            else
            {
                this->scanFirstPartition_OrgsAft_Finished(l, a, Q.start, next_fromOaftA, vcand, result);
            }

            if (a < b)
            {
                this->scanLastPartition_OrgsIn_Overlaps(l, b, qdummyE, next_fromOinB, result);
                this->scanLastPartition_OrgsAft_Overlaps(l, b, Q.end, next_fromOaftB, result);

                this->scanPartitions_OrgsAft_Finished(l, a+1, b-1, next_fromOaftAB, next_toOaftAB, vcand, result);

                this->scanLastPartition_RepsIn_Starts(l, b, Q.end, next_fromRinB, vcand, result);   // Re-using function from Starts
                this->scanLastPartition_RepsAft_Starts(l, b, next_fromRaftB, vcand, result);    // Re-using function from gContained
            }

            if (a%2) //last bit of a is 1
                foundone = 1;
            if (!(b%2)) //last bit of b is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }

    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterIStart = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
    else
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (Q.end > iter->first))
        {
            if (Q.end < iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iter++;
            iterI++;
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Overlapped(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyS(Q.start, Q.start);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1, next_fromOinB = -1, next_fromRinB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromRinAB = -1, next_toRinAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1;
    boost::dynamic_bitset<> vcand(this->numIndexedRecords);


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            if (a == b)
                return result;

            this->scanFirstPartition_OrgsIn_Overlapped(l, a, Q.start, next_fromOinA, result);
            this->scanFirstPartition_RepsIn_Overlapped(l, a, Q.start, next_fromRinA, result);

            this->scanPartitions_RepsIn_Started(l, a+1, b-1, next_fromRinAB, next_toRinAB, vcand, result);
//            this->scanLastPartition_RepsIn_Overlapped(l, b, Q.end, next_fromRinB, vcand, result);

            this->scanFirstPartition_OrgsAft_Finishes(l, a, Q.start, next_fromOaftA, vcand, result);
            this->scanLastPartition_RepsAft_Starts(l, a, next_fromRaftA, vcand, result);    // Re-using function from gContained, a and next_fromRaftA instead of b and next_fromRaftB
        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition
                this->scanFirstPartition_OrgsIn_Overlapped(l, a, Q.end, qdummyS, next_fromOinA, result);
                this->scanFirstPartition_RepsIn_Overlapped(l, a, Q.start, Q.end, next_fromRinA, result);
            }
            else
            {
                this->scanFirstPartition_OrgsIn_Overlapped(l, a, qdummyS, next_fromOinA, result);
                this->scanFirstPartition_RepsIn_Overlapped(l, a, Q.start, next_fromRinA, result);
            }

            if (a < b)
            {
                this->scanPartitions_RepsIn_Started(l, a+1, b-1, next_fromRinAB, next_toRinAB, vcand, result);
                this->scanLastPartition_RepsIn_Overlapped(l, b, Q.end, next_fromRinB, vcand, result);

                this->scanFirstPartition_OrgsAft_Finishes(l, a, Q.start, next_fromOaftA, vcand, result);
                this->scanLastPartition_RepsAft_Starts(l, a, next_fromRaftA, vcand, result);    // Re-using function from gContained, a and next_fromRaftA instead of b and next_fromRaftB
            }

            if (a%2) //last bit of a is 1
                foundone = 1;
            if (!(b%2)) //last bit of b is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }


    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterIStart = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
    else
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if ((Q.start < iter->second) && (Q.end > iter->second))
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Contains(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyS(Q.start+1, Q.start+1);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromOinB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOaftAB = -1, next_toOaftAB = -1, next_fromRinB = -1, next_fromRinAB = -1, next_toRinAB = -1;
    boost::dynamic_bitset<> vcand(this->numIndexedRecords);


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            if (a+1 >= b)
                break;

            this->scanPartitions_OrgsIn_gOverlaps(l, a+1, b-1, next_fromOinAB, next_toOinAB, result);           // Re-using function from gOverlaps
            this->scanPartitions_RepsIn_Started(l, a+1, b-1, next_fromRinAB, next_toRinAB, vcand, result);      // Re-using function from Started
            this->scanPartitions_OrgsAft_Finished(l, a+1, b-1, next_fromOaftAB, next_toOaftAB, vcand, result);  // Re-using function from Finished
        }
        else
        {
            if (a == b)
            {
                this->scanFirstPartition_OrgsIn_Contains(l, a, Q.end, qdummyS, next_fromOinA, result);
                // TODO ?
//                return result;
            }
            else
            {
                if (!foundone)
                    this->scanFirstPartition_OrgsIn_Contains(l, a, qdummyS, next_fromOinA, result);
                this->scanPartitions_OrgsIn_gOverlaps(l, a+1, b-1, next_fromOinAB, next_toOinAB, result);           // Re-using function from gOverlaps

                if (!foundzero)
                    this->scanLastPartition_OrgsIn_Contains(l, b, Q.end, next_fromOinB, result);

                if (!foundone)
                    this->scanFirstPartition_OrgsAft_Contains(l, a, Q.start, next_fromOaftA, vcand, result);

                this->scanPartitions_OrgsAft_Finished(l, a+1, b-1, next_fromOaftAB, next_toOaftAB, vcand, result);  // Re-using function from Finished
                this->scanPartitions_RepsIn_Started(l, a+1, b-1, next_fromRinAB, next_toRinAB, vcand, result);      // Re-using function from Started

                if (!foundzero)
                    this->scanLastPartition_RepsIn_Started(l, b, Q.end, next_fromRinB, vcand, result);              // Re-using function from Started
            }
        }

        if (a%2) //last bit of a is 1
            foundone = 1;
        if (!(b%2)) //last bit of b is 0
            foundzero = 1;
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }


    // Handle root.
    if (!(foundone && foundzero))
    {
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iter = lower_bound(iterBegin, iterEnd, qdummyS, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
            if (Q.end > iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iter++;
            iterI++;
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Contained(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyS(Q.start, Q.start);
    RecordEnd qdummyE(Q.end+1, Q.end+1);
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromOinB = -1, next_fromRinA = -1, next_fromRinB = -1, next_fromRaftA = -1, next_fromRaftB = -1;
    boost::dynamic_bitset<> vcand(this->numIndexedRecords);


    for (auto l = 0; l < this->numBits; l++)
    {
        if (a == b)
        {
            this->scanFirstPartition_OrgsIn_Contained(l, a, Q.end, qdummyS, next_fromOinA, result);
            this->scanFirstPartition_OrgsAft_Contained(l, a, Q.start, next_fromOaftA, result);
            this->scanFirstPartition_RepsIn_gOverlaps(l, a, qdummyE, next_fromRinA, result);    // Re-using function from overlaps
            this->scanFirstPartition_RepsAft_gOverlaps(l, a, next_fromRaftA, result);           // Re-using function from gOverlaps
//            return result;    // TODO?
        }
        else
        {
            this->scanFirstPartition_OrgsAft_Contained(l, a, Q.start, next_fromOaftA, vcand, result);
            this->scanFirstPartition_RepsAft_Contained(l, a, next_fromRaftA, vcand, result);   // Re-using function from gContained

            this->scanLastPartition_RepsIn_Starts(l, b, Q.end, next_fromRinB, vcand, result);   // Re-using function from Starts
            this->scanLastPartition_RepsAft_Starts(l, b, next_fromRaftB, vcand, result);    // Re-using function from gContained
        }

//        if (a%2) //last bit of b is 1
//            foundone = 1;
//        if (!(b%2)) //last bit of a is 0
//            foundzero = 1;
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }


    // Handle root.
    iterI = this->pOrgsInIds[this->numBits].begin();
    iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
    vector<pair< Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits].end(), qdummyS, CompareTimestampPairsByStart);
    for (iter = iterBegin; iter != pivot; iter++)
    {
        if (Q.end < iter->second)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
        iterI++;
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Precedes(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIStart, iterIEnd;
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyE(Q.end+1, Q.end+1);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinB = -1, next_fromOaftB = -1, next_fromOinAB = -1, next_fromOaftAB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (!(foundone && foundzero))
        if (!foundone)
        {
            this->scanLastPartition_OrgsIn_Precedes(l, b, qdummyE, next_fromOinB, result);
            this->scanLastPartition_OrgsAft_Precedes(l, b, Q.end+1, next_fromOaftB, result);
        }

        if (b+1 >= (int)(pow(2, this->numBits-l)))
            break;

        this->scanPartitions_OrgsIn_Precedes(l, b+1, next_fromOinAB, result);
        this->scanPartitions_OrgsAft_Precedes(l, b+1, next_fromOaftAB, result);

        if (b%2) //last bit of b is 1
            foundone = 1;
//        if (!(b%2)) //last bit of a is 0
//            foundzero = 1;
        b >>= 1; // b = b div 2
    }

    // Handle root.
//    if (!(foundone && foundzero))
    if (!foundone)
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iter = lower_bound(iterBegin, iterEnd, qdummyE, CompareTimestampPairsByStart);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif

            iter++;
            iterI++;
        }
    }
    else
    {
        iterIStart = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_Preceded(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyS(Q.start, Q.start);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromRinA = -1, next_fromOinAB = -1, next_fromRinAB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (!(foundone && foundzero))
        if (!foundzero)
        {
            this->scanFirstPartition_OrgsIn_Preceded(l, a, Q.start, next_fromOinA, result);
            this->scanFirstPartition_RepsIn_Preceded(l, a, Q.start, next_fromRinA, result);
        }

        if (a == 0)
            break;
//        if (a > 0)
//        {
            this->scanPartitions_OrgsIn_Preceded(l, a, next_fromOinAB, result);
            this->scanPartitions_RepsIn_Preceded(l, a, next_fromRinAB, result);
//        }


//        if (a%2) //last bit of b is 1
//            foundone = 1;
        if (!(a%2)) //last bit of a is 0
            foundzero = 1;
        a >>= 1; // a = a div 2
    }

    // Handle root.
//    if (!(foundone && foundzero))
    if (!foundzero)
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (Q.start > iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }
    else
    {
        iterIStart = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }


    return result;
}


// Generalized predicates, ACM SIGMOD'22 gOverlaps
size_t HINT_M_ALL::executeBottomUp_gOverlaps(StabbingQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIStart, iterIEnd;
    Timestamp a = Q.point >> (this->maxBits-this->numBits); // prefix
    RecordStart qdummySE(0, Q.point+1);
    RecordEnd qdummyS(0, Q.point);
    pair<Timestamp, Timestamp> qdummyE(Q.point+1, Q.point+1);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1, next_fromOinB = -1, next_fromOaftB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOaftAB = -1, next_toOaftAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results

            // Handle the partition that contains a: consider both originals and replicas
            this->scanFirstPartition_OrgsIn_gOverlaps(l, a, next_fromOinA, result);
            this->scanFirstPartition_OrgsAft_gOverlaps(l, a, next_fromOaftA, result);

            this->scanFirstPartition_RepsIn_gOverlaps(l, a, next_fromRinA, result);
            this->scanFirstPartition_RepsAft_gOverlaps(l, a, next_fromRaftA, result);
        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            this->scanFirstPartition_OrgsIn_gOverlaps(l, a, Q.point, qdummyE, next_fromOinA, result);
            this->scanLastPartition_OrgsAft_gOverlaps(l, a, qdummySE, next_fromOaftA, result);

            this->scanFirstPartition_RepsIn_gOverlaps(l, a, qdummyS, next_fromRinA, result);
            this->scanFirstPartition_RepsAft_gOverlaps(l, a, next_fromRaftA, result);

            if (a%2) //last bit of b is 1
                foundone = 1;
            if (!(a%2)) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
    }

    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterIStart = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
    else
    {
        // Comparisons needed
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits].end(), qdummyE, CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (Q.point <= iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= this->pOrgsInIds[this->numBits][iter-iterBegin];
#endif
            }
        }
    }


    return result;
}


size_t HINT_M_ALL::executeBottomUp_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    RecordStart qdummySE(0, Q.end+1);
    RecordEnd qdummyS(0, Q.start);
    pair<Timestamp, Timestamp> qdummyE(Q.end+1, Q.end+1);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1, next_fromOinB = -1, next_fromOaftB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOaftAB = -1, next_toOaftAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1;


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
        iterIStart = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIStart; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
    else
    {
        // Comparisons needed
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits].end(), qdummyE, CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (Q.start <= iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterI++;
        }
    }


    return result;
}
