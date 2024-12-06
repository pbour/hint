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

#include "hint_m.h"



inline void HINT_M_SubsSort_SS_CM::updateCounters(const Record &r)
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


inline void HINT_M_SubsSort_SS_CM::updatePartitions(const Record &r)
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
                    this->pRepsInTmp[level][this->pRepsIn_offsets[level][a]++] = Record(r.id, r.start, r.end);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftTmp[level][this->pRepsAft_offsets[level][a]++] = Record(r.id, r.start, r.end);
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
                    this->pOrgsAftTmp[level][this->pOrgsAft_offsets[level][a]++] = Record(r.id, r.start, r.end);
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
                    this->pOrgsAftTmp[level][this->pOrgsAft_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsInTmp[level][this->pRepsIn_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftTmp[level][this->pRepsAft_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSort_SS_CM::HINT_M_SubsSort_SS_CM(const Relation &R, const unsigned int numBits, const unsigned int maxBits)  : HierarchicalIndex(R, numBits, maxBits)
{
    OffsetEntry_SS_CM dummySE;
    Offsets_SS_CM_Iterator iterSEO, iterSEOBegin, iterSEOEnd;
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
    this->pOrgsAftTmp = new Relation[this->height];
    this->pRepsInTmp  = new Relation[this->height];
    this->pRepsAftTmp = new Relation[this->height];
    
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
        this->pRepsAftTmp[l].resize(sumRaft);
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
            sort(this->pRepsInTmp[l].begin()+this->pRepsIn_offsets[l][pId], this->pRepsInTmp[l].begin()+this->pRepsIn_offsets[l][pId+1], CompareByEnd);
            sort(this->pRepsAftTmp[l].begin()+this->pRepsAft_offsets[l][pId], this->pRepsAftTmp[l].begin()+this->pRepsAft_offsets[l][pId+1], CompareByEnd);
        }
    }
    
    
    // Step 5: break-down data to create id- and timestamp-dedicated arrays; free auxiliary memory.
    this->pOrgsInIds  = new RelationId[this->height];
    this->pOrgsAftIds = new RelationId[this->height];
    this->pRepsInIds  = new RelationId[this->height];
    this->pRepsAftIds = new RelationId[this->height];
    this->pOrgsInTimestamps  = new vector<pair<Timestamp, Timestamp> >[this->height];
    this->pOrgsAftTimestamps = new vector<pair<Timestamp, Timestamp> >[this->height];
    this->pRepsInTimestamps  = new vector<pair<Timestamp, Timestamp> >[this->height];
    this->pRepsAftTimestamps = new vector<pair<Timestamp, Timestamp> >[this->height];
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
        this->pOrgsAftTimestamps[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsAftIds[l][j] = this->pOrgsAftTmp[l][j].id;
            this->pOrgsAftTimestamps[l][j].first = this->pOrgsAftTmp[l][j].start;
            this->pOrgsAftTimestamps[l][j].second = this->pOrgsAftTmp[l][j].end;
        }
        
        cnt = pRepsInTmp[l].size();
        this->pRepsInIds[l].resize(cnt);
        this->pRepsInTimestamps[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pRepsInIds[l][j] = this->pRepsInTmp[l][j].id;
            this->pRepsInTimestamps[l][j].first = this->pRepsInTmp[l][j].start;
            this->pRepsInTimestamps[l][j].second = this->pRepsInTmp[l][j].end;
        }

        cnt = pRepsAftTmp[l].size();
        this->pRepsAftIds[l].resize(cnt);
        this->pRepsAftTimestamps[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pRepsAftIds[l][j] = this->pRepsAftTmp[l][j].id;
            this->pRepsAftTimestamps[l][j].first = this->pRepsAftTmp[l][j].start;
            this->pRepsAftTimestamps[l][j].second = this->pRepsAftTmp[l][j].end;
        }
    }
    
    
    // Free auxiliary memory
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
    delete[] this->pRepsAftTmp;
    

    // Step 4: create offset pointers
    this->pOrgsIn_ioffsets  = new Offsets_SS_CM[this->height];
    this->pOrgsAft_ioffsets = new Offsets_SS_CM[this->height];
    this->pRepsIn_ioffsets  = new Offsets_SS_CM[this->height];
    this->pRepsAft_ioffsets = new Offsets_SS_CM[this->height];
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
                    iterSEOBegin = this->pOrgsIn_ioffsets[l+1].begin();
                    iterSEOEnd = this->pOrgsIn_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOBegin, iterSEOEnd, dummySE);//, CompareOffsetsByTimestamp);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOBegin): -1;
                }
                this->pOrgsIn_ioffsets[l].push_back(OffsetEntry_SS_CM(pId, this->pOrgsInIds[l].begin()+sumOin, this->pOrgsInTimestamps[l].begin()+sumOin, tmp));
            }
            
            dummySE.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsAft_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOBegin = this->pOrgsAft_ioffsets[l+1].begin();
                    iterSEOEnd = this->pOrgsAft_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOBegin, iterSEOEnd, dummySE);//, CompareOffsetsByTimestamp);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOBegin): -1;
                }
                this->pOrgsAft_ioffsets[l].push_back(OffsetEntry_SS_CM(pId, this->pOrgsAftIds[l].begin()+sumOaft, this->pOrgsAftTimestamps[l].begin()+sumOaft, tmp));
            }
            
            dummySE.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsIn_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOBegin = this->pRepsIn_ioffsets[l+1].begin();
                    iterSEOEnd = this->pRepsIn_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOBegin, iterSEOEnd, dummySE);//, CompareOffsetsByTimestamp);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOBegin): -1;
                }
                this->pRepsIn_ioffsets[l].push_back(OffsetEntry_SS_CM(pId, this->pRepsInIds[l].begin()+sumRin, this->pRepsInTimestamps[l].begin()+sumRin, tmp));
            }
            
            dummySE.tstamp = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsAft_sizes[l][pId] > 0)
            {
                isEmpty = false;
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOBegin = this->pRepsAft_ioffsets[l+1].begin();
                    iterSEOEnd = this->pRepsAft_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOBegin, iterSEOEnd, dummySE);//, CompareOffsetsByTimestamp);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOBegin): -1;
                }
                this->pRepsAft_ioffsets[l].push_back(OffsetEntry_SS_CM(pId, this->pRepsAftIds[l].begin()+sumRaft, this->pRepsAftTimestamps[l].begin()+sumRaft, tmp));
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


void HINT_M_SubsSort_SS_CM::getStats()
{
    size_t sum = 0;
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        this->numPartitions += cnt;

        this->numOriginalsIn  += this->pOrgsInIds[l].size();
        this->numOriginalsAft += this->pOrgsAftIds[l].size();
        this->numReplicasIn   += this->pRepsInIds[l].size();
        this->numReplicasAft  += this->pRepsAftIds[l].size();
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasAft)/(this->numPartitions-numEmptyPartitions);
}


HINT_M_SubsSort_SS_CM::~HINT_M_SubsSort_SS_CM()
{
    delete[] this->pOrgsIn_ioffsets;
    delete[] this->pOrgsAft_ioffsets;
    delete[] this->pRepsIn_ioffsets;
    delete[] this->pRepsAft_ioffsets;
    
    delete[] this->pOrgsInIds;
    delete[] this->pOrgsInTimestamps;
    delete[] this->pOrgsAftIds;
    delete[] this->pOrgsAftTimestamps;
    delete[] this->pRepsInIds;
    delete[] this->pRepsInTimestamps;
    delete[] this->pRepsAftIds;
    delete[] this->pRepsAftTimestamps;
}


// Auxiliary functions to determine exactly how to scan a partition.
inline bool HINT_M_SubsSort_SS_CM::getBounds(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, vector<pair<Timestamp, Timestamp> >::iterator &iterBegin, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI)
{
    OffsetEntry_SS_CM qdummy;
    Offsets_SS_CM_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = ioffsets[level].size();
    PartitionId from = next_from;

    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = ioffsets[level].begin();
            iterIOEnd = ioffsets[level].end();

            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareOffsetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterI = iterIO->iterI;
                iterBegin = iterIO->iterT;
                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : timestamps[level].end());

                next_from =iterIO->pid;
                
                return true;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while (((ioffsets[level][from]).tstamp < t) && (from < cnt))
                    from++;
            }
            else if (tmp > t)
            {
                while (((ioffsets[level][from]).tstamp > t) && (from > -1))
                    from--;
                if (((ioffsets[level][from]).tstamp != t) || (from == -1))
                    from++;
            }

            if ((from != cnt) && ((ioffsets[level][from]).tstamp == t))
            {
                iterI = (ioffsets[level][from]).iterI;
                iterBegin = (ioffsets[level][from]).iterT;
                iterEnd = ((from+1 != cnt) ? (ioffsets[level][from+1]).iterT : timestamps[level].end());

                next_from = (ioffsets[level][from]).pid;
                
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


inline bool HINT_M_SubsSort_SS_CM::getBounds(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, RelationId *ids, RelationIdIterator &iterIBegin, RelationIdIterator &iterIEnd)
{
    OffsetEntry_SS_CM qdummy;
    Offsets_SS_CM_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
    size_t cnt = ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = ioffsets[level].begin();
            iterIOEnd = ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareOffsetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp == t))
            {
                iterIBegin = iterIO->iterI;
                iterIEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterI : ids[level].end());

                next_from =iterIO->pid;
                
                return true;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while (((ioffsets[level][from]).tstamp < t) && (from < cnt))
                    from++;
            }
            else if (tmp > t)
            {
                while (((ioffsets[level][from]).tstamp > t) && (from > -1))
                    from--;
                if (((ioffsets[level][from]).tstamp != t) || (from == -1))
                    from++;
            }

            if ((from != cnt) && ((ioffsets[level][from]).tstamp == t))
            {
                iterIBegin = (ioffsets[level][from]).iterI;
                iterIEnd = ((from+1 != cnt) ? (ioffsets[level][from+1]).iterI : ids[level].end());

                next_from = (ioffsets[level][from]).pid;
                
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


inline bool HINT_M_SubsSort_SS_CM::getBoundsS(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, RelationIdIterator &iterIBegin, RelationIdIterator &iterIEnd)
{
    OffsetEntry_SS_CM qdummy;
    Offsets_SS_CM_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = ioffsets[level].begin();
            iterIOEnd = ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareOffsetsByTimestamp);
//            if ((iterIO != iterIOEnd) && (get<0>(this->pOrgsIn_ioffsets[level][from]) >= t))
            if (iterIO != iterIOEnd)
            {
                iterIBegin = iterIO->iterI;
//                iterBegin = iterIO->iterT;
////                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : this->pOrgsInTimestamps[level].end());
//                iterEnd = timestamps[level].end();
                iterIEnd = iterIBegin + (timestamps[level].end()-iterIO->iterT);

                next_from =iterIO->pid;
                
                return true;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while (((ioffsets[level][from]).tstamp < t) && (from < cnt))
                    from++;
            }
            else if (tmp > t)
            {
                while (((ioffsets[level][from]).tstamp > t) && (from > -1))
                    from--;
                if (((ioffsets[level][from]).tstamp != t) || (from == -1))
                    from++;
            }

//            if ((from != cnt) && (get<0>(this->pOrgsIn_ioffsets[level][from]) >= b))
            if ((from != cnt) && (from != -1))
            {
                iterIBegin = (ioffsets[level][from]).iterI;
//                iterBegin = (ioffsets[level][from]).iterT;
////                iterEnd = ((from+1 != cnt) ? get<2>(this->pOrgsIn_ioffsets[level][from+1]) : this->pOrgsInTimestamps[level].end());
//                iterEnd = timestamps[level].end();
                iterIEnd = iterIBegin + (timestamps[level].end()-(ioffsets[level][from]).iterT);

                next_from = (ioffsets[level][from]).pid;
                
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

inline bool HINT_M_SubsSort_SS_CM::getBoundsE(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, RelationId *ids, vector<pair<Timestamp, Timestamp> > *timestamps, RelationIdIterator &iterIBegin, RelationIdIterator &iterIEnd)

{
    OffsetEntry_SS_CM qdummy;
    Offsets_SS_CM_Iterator iterIO, iterIOBegin, iterIOEnd;
    size_t cnt = ioffsets[level].size();
    PartitionId from = next_from;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (from > cnt))
        {
            qdummy.tstamp = t;
            iterIOBegin = ioffsets[level].begin();
            iterIOEnd = ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummy);//, CompareOffsetsByTimestamp);
            iterIO--;
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= t))
            {
//                iterI = iterIO->iterI;
                iterIBegin = ids[level].begin();
//                iterBegin = timestamps[level].begin();
//                iterEnd = ((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : timestamps[level].end());
                iterIEnd = iterIBegin + (((iterIO+1 != iterIOEnd) ? (iterIO+1)->iterT : timestamps[level].end()) - timestamps[level].begin());

                next_from =iterIO->pid;

                return true;
            }
            else
                return false;
        }
        else
        {
            Timestamp tmp = (ioffsets[level][from]).tstamp;
            if (tmp < t)
            {
                while (((ioffsets[level][from]).tstamp < t) && (from < cnt))
                    from++;
                from--;
            }
            else if (tmp >= t)
            {
                while (((ioffsets[level][from]).tstamp >= t) && (from > -1))
                    from--;
//                if (((ioffsets[level][from]).tstamp != t) || (from == -1))
//                    from++;
            }

            if ((from != -1) && (from != cnt))
            {
//                iterI = (ioffsets[level][from]).iterI;
                iterIBegin = ids[level].begin();
//                iterBegin = timestamps[level].begin();
//                iterEnd = ((from+1 != cnt) ? (ioffsets[level][from+1]).iterT : timestamps[level].end());
                iterIEnd = iterIBegin + (((from+1 != cnt) ? (ioffsets[level][from+1]).iterT : timestamps[level].end()) - timestamps[level].begin());

                next_from = (ioffsets[level][from]).pid;

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


inline bool HINT_M_SubsSort_SS_CM::getBounds(unsigned int level, Timestamp ts, Timestamp te, PartitionId &next_from, PartitionId &next_to, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, vector<pair<Timestamp, Timestamp> >::iterator &iterBegin, vector<pair<Timestamp, Timestamp> >::iterator &iterEnd, RelationIdIterator &iterI)
{
    OffsetEntry_SS_CM qdummyA, qdummyB;
    Offsets_SS_CM_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
    size_t cnt = ioffsets[level].size();
    PartitionId from = next_from, to = next_to;


    if (cnt > 0)
    {
        // Do binary search or follow vertical pointers.
        if ((from == -1) || (to == -1))
        {
            qdummyA.tstamp = ts;
            iterIOBegin = ioffsets[level].begin();
            iterIOEnd = ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummyA);//, CompareOffsetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= te))
            {
                next_from =iterIO->pid;

                qdummyB.tstamp = te;
                iterI = iterIO->iterI;
                iterBegin = iterIO->iterT;

                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB);//, CompareOffsetsByTimestamp);
//                iterIO2 = iterIO;
//                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= te))
//                    iterIO2++;

                iterEnd = ((iterIO2 != iterIOEnd) ? iterEnd = iterIO2->iterT: timestamps[level].end());

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
            Timestamp tmp = (ioffsets[level][from]).tstamp;
            if (tmp < ts)
            {
                while (((ioffsets[level][from]).tstamp < ts) && (from < cnt))
                    from++;
            }
            else if (tmp > ts)
            {
                while (((ioffsets[level][from]).tstamp > ts) && (from > -1))
                    from--;
                if (((ioffsets[level][from]).tstamp != ts) || (from == -1))
                    from++;
            }

            tmp = (ioffsets[level][to]).tstamp;
            if (tmp > te)
            {
                while (((ioffsets[level][to]).tstamp > te) && (to > -1))
                    to--;
                to++;
            }
//                else if (tmp <= te)
            else if (tmp == te)
            {
                while (((ioffsets[level][to]).tstamp <= te) && (to < cnt))
                    to++;
            }

            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterI = (ioffsets[level][from]).iterI;
                iterBegin = (ioffsets[level][from]).iterT;
                iterEnd   = (to != cnt)? (ioffsets[level][to]).iterT: timestamps[level].end();

                next_from = (ioffsets[level][from]).pid;
                next_to   = (to != cnt) ? (ioffsets[level][to]).pid  : -1;
                
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


inline bool HINT_M_SubsSort_SS_CM::getBounds(unsigned int level, Timestamp ts, Timestamp te, PartitionId &next_from, PartitionId &next_to, Offsets_SS_CM *ioffsets, RelationId *ids, RelationIdIterator &iterIBegin, RelationIdIterator &iterIEnd)
{
    OffsetEntry_SS_CM qdummyTS, qdummyTE;
    Offsets_SS_CM_Iterator iterIO, iterIO2, iterIOBegin, iterIOEnd;
    size_t cnt = ioffsets[level].size();
    PartitionId from = next_from, to = next_to;


    if (cnt > 0)
    {
        from = next_from;
        to = next_to;

        // Do binary search or follow vertical pointers.
        if ((from == -1) || (to == -1))
        {
            qdummyTS.tstamp = ts;
            iterIOBegin = ioffsets[level].begin();
            iterIOEnd = ioffsets[level].end();
            iterIO = lower_bound(iterIOBegin, iterIOEnd, qdummyTS);//, CompareOffsetsByTimestamp);
            if ((iterIO != iterIOEnd) && (iterIO->tstamp <= te))
            {
                next_from =iterIO->pid;

                qdummyTE.tstamp = te;
                iterIBegin = iterIO->iterI;

                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyTE);//, CompareOffsetsByTimestamp);
//                iterIO2 = iterIO;
//                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
//                    iterIO2++;

                iterIEnd = ((iterIO2 != iterIOEnd) ? iterIEnd = iterIO2->iterI: ids[level].end());

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
            Timestamp tmp = (ioffsets[level][from]).tstamp;
            if (tmp < ts)
            {
                while (((ioffsets[level][from]).tstamp < ts) && (from < cnt))
                    from++;
            }
            else if (tmp > ts)
            {
                while (((ioffsets[level][from]).tstamp > ts) && (from > -1))
                    from--;
                if (((ioffsets[level][from]).tstamp != ts) || (from == -1))
                    from++;
            }

            tmp = (ioffsets[level][to]).tstamp;
            if (tmp > te)
            {
                while (((ioffsets[level][to]).tstamp > te) && (to > -1))
                    to--;
                to++;
            }
//                else if (tmp <= b)
            else if (tmp == te)
            {
                while (((ioffsets[level][to]).tstamp <= te) && (to < cnt))
                    to++;
            }

            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterIBegin = (ioffsets[level][from]).iterI;
                iterIEnd   = (to != cnt)? (ioffsets[level][to]).iterI: ids[level].end();

                next_from = (ioffsets[level][from]).pid;
                next_to   = (to != cnt) ? (ioffsets[level][to]).pid  : -1;
                
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps_Equals(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart, qstart), compare);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qstart == iter->first))
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps_Starts(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart, qstart), compare);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qstart == iter->first))
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckStart_Starts(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qt == iter->first))
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps_Started(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart, qstart), compare);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qstart == iter->first))
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps_Finishes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qstart, qstart), compare);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (qend == iter->second)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps2_Finishes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qend, qend), compare);
        iterI += (iter-iterBegin);
        while ((iter != iterEnd) && (qend == iter->second))
        {
            if (qstart > iter->first)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_Finishes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
        iterI += (iter-iterBegin);
        while ((iter != iterEnd) && (qt == iter->second))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= *iterI;
#endif

            iter++;
            iterI++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps_Finished(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart+1, qstart+1), compare);
        iterI += (iter-iterBegin);
        while (iter != iterEnd)
        {
            if (qend == iter->second)
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
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps2_Finished(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qend, qend), compare);
        iterI += (iter-iterBegin);
        while ((iter != iterEnd) && (qend == iter->second))
        {
            if (qstart < iter->first)
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
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_Finished(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qt == iter->second)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_Met(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qt == iter->second))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= *iterI;
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckAllConditions_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart+1, qstart+1), compare);
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckTwoConditions1_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart+1, qstart+1), compare);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qend > iter->first))
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckTwoConditions2_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart+1, qstart+1), compare);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckOneCondition_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair< Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckOneCondition2_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qt < iter->first)
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

inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckTwoConditions_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (qt < iter->second)
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


inline void HINT_M_SubsSort_SS_CM::scanPartitions_CheckOneCondition_Overlaps(unsigned int level, Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;


    if (this->getBounds(level, ts, te, next_from, next_to, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        {
            if (qt < iter->second)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckTwoConditions3_Overlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qend+1, qend+1), compare);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
            if (qstart < iter->first)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= *iterI;
#endif
            }
            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckAllConditions_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair< Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qstart, qstart), compare);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if ((qstart < iter->second) && (qend > iter->second))
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckTwoConditions1_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart+1, qstart+1), compare);
        iterI += iter-iterBegin;
        while ((iter != iterEnd) && (qend > iter->second))
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckTwoConditions2_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair< Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (qt < iter->second)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckTwoConditions3_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair< Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qend, qend), compare);
        for (iter = iterBegin; iter != pivot; iter++)
        {
            if (qstart > iter->first)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckTwoConditions4_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair< Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qstart, qstart), compare);
        for (iter = iterBegin; iter != pivot; iter++)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckOneCondition_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qt < iter->second)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckOneCondition_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart+1, qstart+1), compare);
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

inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_Overlapped(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
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



inline void HINT_M_SubsSort_SS_CM::scanPartitions_CheckOneCondition_Overlapped(unsigned int level, Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;


    if (this->getBounds(level, ts, te, next_from, next_to, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qt > iter->first)
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





inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps_Contains(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart+1, qstart+1), compare);
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckStart_Contains(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qstart+1, qstart+1), compare);
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



inline void HINT_M_SubsSort_SS_CM::scanPartitions_CheckEnd_Contains(unsigned int level, Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;


    if (this->getBounds(level, ts, te, next_from, next_to, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qt > iter->second)
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

inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps_Contained(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair< Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qstart, qstart), compare);
        for (iter = iterBegin; iter != pivot; iter++)
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
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckStart_Contained(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_Contained(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qt+1, qt+1), compare);
        iterI += iter-iterBegin;
        while (iter != iterEnd)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= *iterI;
#endif

            iterI++;
            iter++;
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_Precedes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
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


inline void HINT_M_SubsSort_SS_CM::scanPartitions_Precedes(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;


    if (this->getBoundsS(level, t, next_from, ioffsets, timestamps, iterIBegin, iterIEnd))
    {
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= *iterI;
#endif
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_Preceded(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qt > iter->second)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_Preceded(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
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


inline void HINT_M_SubsSort_SS_CM::scanPartitions_Preceded(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, RelationId *ids, vector<pair<Timestamp, Timestamp> > *timestamps, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;


    if (this->getBoundsE(level, t, next_from, ioffsets, ids, timestamps, iterIBegin, iterIEnd))
    {
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckBothTimestamps_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qstart, Timestamp qend, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qend+1, qend+1), compare);
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (qt <= iter->second)
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckEnd_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        iter = lower_bound(iterBegin, iterEnd, make_pair(qt, qt), compare);
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


inline void HINT_M_SubsSort_SS_CM::scanPartition_CheckStart_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), Timestamp qt, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;


    if (this->getBounds(level, t, next_from, ioffsets, timestamps, iterBegin, iterEnd, iterI))
    {
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(qt+1, qt+1), compare);
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


inline bool getBounds(unsigned int level, Timestamp t, PartitionId &next_from, Offsets_SS_CM *ioffsets, vector<pair<Timestamp, Timestamp> > *timestamps, RelationIdIterator &iterIBegin, RelationIdIterator &iterIEnd);
inline void HINT_M_SubsSort_SS_CM::scanPartition_NoChecks_gOverlaps(unsigned int level, Timestamp t, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;


    if (this->getBounds(level, t, next_from, ioffsets, ids, iterIBegin, iterIEnd))
    {
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}


inline void HINT_M_SubsSort_SS_CM::scanPartitions_NoChecks_gOverlaps(unsigned int level, Timestamp ts, Timestamp te, Offsets_SS_CM *ioffsets, RelationId *ids, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    OffsetEntry_SS_CM qdummyTS, qdummyTE;


    if (this->getBounds(level, ts, te, next_from, next_to, ioffsets, ids, iterIBegin, iterIEnd))
    {
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}


// Basic predicates of Allen's algebra
size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Equals(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    Timestamp prevb;
    pair<Timestamp, Timestamp> qdummyS(Q.start, Q.start);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1;
    bool firstfound = false, lastfound = false;
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
//                this->scanFirstPartition_OrgsAft_Equals(level, a, Q.start, next_fromOaftA, vcand);
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
//                this->scanFirstPartition_OrgsAft_Equals(level, a, Q.start, next_fromOaftA, vcand);
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
        this->scanPartition_CheckBothTimestamps_Equals(a_level, a_partition, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinA, result);
    else
    {
        this->scanPartition_CheckBothTimestamps_Equals(a_level, a_partition, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOaftA, result);
    }

    return result;
//
//    for (auto l = 0; l < this->numBits; l++)
//    {
//        if (foundone)// && foundzero)
//            break;
//
//        if (a == b)
//            this->scanPartition_CheckBothTimestamps_Equals(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinA, result);
//        else
//        {
//            this->scanPartition_CheckBothTimestamps_Equals(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOaftA, result);
//        }
//
//        if (a%2) //last bit of b is 1
//            foundone = 1;
////        if (!(a%2)) //last bit of a is 0
////            foundzero = 1;
//        a >>= 1; // a = a div 2
//        b >>= 1; // b = b div 2
//    }
//
//    // Handle root.
////    if (!(foundone && foundzero))
//    if (!foundone)
//    {
//        // Comparisons needed
//        iterI = this->pOrgsInIds[this->numBits].begin();
//        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
//        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
//        iter = lower_bound(iterBegin, iterEnd, make_pair(Q.start, Q.start), CompareTimestampPairsByStart);
//        iterI += iter-iterBegin;
//        while ((iter != iterEnd) && (Q.start == iter->first))
//        {
//            if (Q.end == iter->second)
//                result ^= (*iterI);
//            iter++;
//            iterI++;
//        }
//    }


    return result;
}


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Starts(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
        if (foundone)
            break;

        if (a == b)
        {
            this->scanPartition_CheckBothTimestamps_Starts(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinA, result);
            this->scanPartition_CheckStart_Starts(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, next_fromOaftA, result);
        }
        else
            this->scanPartition_CheckBothTimestamps_Starts(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOaftA, result);

        if (a%2) //last bit of b is 1
            foundone = 1;
//        if (!(b%2)) //last bit of a is 0
//            foundzero = 1;
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
        iter = lower_bound(iterBegin, iterEnd, make_pair(Q.start, Q.start), CompareTimestampPairsByStart);
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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Started(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
        if (foundone)
            break;

        if (a == b)
            this->scanPartition_CheckBothTimestamps_Started(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinA, result);
        else
        {
            this->scanPartition_CheckStart_Starts(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, next_fromOinA, result);
            this->scanPartition_CheckBothTimestamps_Started(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOaftA, result);
        }

        if (a%2) //last bit of b is 1
            foundone = 1;
//        if (!(a%2)) //last bit of a is 0
//            foundzero = 1;
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
        iter = lower_bound(iterBegin, iterEnd, make_pair(Q.start, Q.start), CompareTimestampPairsByStart);
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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Finishes(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinB = -1, next_fromRinB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
        if (foundzero)
            break;

        if (a == b)
        {
            this->scanPartition_CheckBothTimestamps_Finishes(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinB, result);
            this->scanPartition_CheckEnd_Finishes(l, b, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.end, next_fromRinB, result);
        }
        else
            this->scanPartition_CheckBothTimestamps2_Finishes(l, b, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, Q.end, next_fromRinB, result);

//        if (b%2) //last bit of b is 1
//            foundone = 1;
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
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(Q.start, Q.start), CompareTimestampPairsByStart);
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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Finished(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinB = -1, next_fromRinB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
        if (foundzero)
            break;

        if (a == b)
            this->scanPartition_CheckBothTimestamps_Finished(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinB, result);

        else
        {
            this->scanPartition_CheckEnd_Finished(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, Q.end, next_fromOinB, result);
            this->scanPartition_CheckBothTimestamps2_Finished(l, b, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, Q.end, next_fromRinB, result);
        }

//        if (b%2) //last bit of b is 1
//            foundone = 1;
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
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iter = lower_bound(iterBegin, iterEnd, make_pair(Q.start+1, Q.start+1), CompareTimestampPairsByStart);
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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Meets(RangeQuery Q)
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
        this->scanPartition_CheckStart_Starts(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.end, next_fromOinB, result);
        this->scanPartition_CheckStart_Starts(l, b, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.end, next_fromOaftB, result);

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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Met(RangeQuery Q)
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
        this->scanPartition_CheckEnd_Finished(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, Q.start, next_fromOinA, result);
        this->scanPartition_CheckEnd_Met(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, next_fromRinA, result);

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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Overlaps(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1, next_fromOinB = -1, next_fromOaftB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOaftAB = -1, next_toOaftAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1, next_fromRinB = -1, next_fromRaftB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            if (a == b)
                return result;

            this->scanPartition_CheckOneCondition_Overlaps(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.end, next_fromOinB, result);
            this->scanPartition_CheckOneCondition_Overlaps(l, b, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.end, next_fromOaftB, result);
//
//            // Why doesn't work????
//            this->scanPartition_NoChecks_gOverlaps(l, b, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinB, result);
//            this->scanPartition_NoChecks_gOverlaps(l, b, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftB, result);

            this->scanPartition_CheckOneCondition2_Overlaps(l, b, this->pRepsIn_ioffsets, this->pRepsInTimestamps, Q.start, next_fromRinB, result);
            this->scanPartition_CheckOneCondition2_Overlaps(l, b, this->pRepsAft_ioffsets, this->pRepsAftTimestamps, Q.start, next_fromRaftB, result);
        }
        else
        {
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                this->scanPartition_CheckAllConditions_Overlaps(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinA, result);
                this->scanPartition_CheckTwoConditions1_Overlaps(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOaftA, result);
            }
            else
            {
                this->scanPartition_CheckTwoConditions_Overlaps(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.end, next_fromOinB, result);
                this->scanPartition_CheckOneCondition_Overlaps(l, b, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.end, next_fromOaftB, result);

                if (!foundzero)
                {
                    this->scanPartition_CheckTwoConditions3_Overlaps(l, b, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, Q.end, next_fromRinB, result);
                    this->scanPartition_CheckOneCondition2_Overlaps(l, b, this->pRepsAft_ioffsets, this->pRepsAftTimestamps, Q.start, next_fromRaftB, result);
                }
                else
                {
                    this->scanPartition_CheckOneCondition2_Overlaps(l, b, this->pRepsIn_ioffsets, this->pRepsInTimestamps, Q.start, next_fromRinB, result);
                    this->scanPartition_CheckOneCondition2_Overlaps(l, b, this->pRepsAft_ioffsets, this->pRepsAftTimestamps, Q.start, next_fromRaftB, result);
                }
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
        iterIBegin = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
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
        iter = lower_bound(iterBegin, iterEnd, make_pair(Q.start+1, Q.start+1), CompareTimestampPairsByStart);
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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Overlapped(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1, next_fromOinB = -1, next_fromRinB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromRinAB = -1, next_toRinAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            if (a == b)
                return result;

            this->scanPartition_CheckOneCondition_Overlapped(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, Q.start, next_fromOinA, result);
            this->scanPartition_CheckOneCondition_Overlapped(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, next_fromRinA, result);

            this->scanPartition_CheckEnd_Overlapped(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, Q.end, next_fromOaftA, result);
            this->scanPartition_CheckOneCondition_Overlaps(l, a, this->pRepsAft_ioffsets, this->pRepsAftTimestamps, CompareTimestampPairsByEnd, Q.end, next_fromRaftA, result);
        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition
                this->scanPartition_CheckAllConditions_Overlapped(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinA, result);
                this->scanPartition_CheckTwoConditions1_Overlapped(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, Q.end, next_fromRinA, result);
            }
            else
            {
                if (!foundone)
                {
                    this->scanPartition_CheckTwoConditions2_Overlapped(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, next_fromOinA, result);
                    this->scanPartition_CheckOneCondition_Overlapped(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, next_fromRinA, result);
                }
                else
                {
                    this->scanPartition_CheckOneCondition_Overlapped(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, Q.start, next_fromOinA, result);
                    this->scanPartition_CheckOneCondition_Overlapped(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, next_fromRinA, result);
                }
            }

            if (a < b)
            {
                if (!foundone)
                {
                    this->scanPartition_CheckTwoConditions4_Overlapped(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOaftA, result);
                    this->scanPartition_CheckOneCondition_Overlaps(l, a, this->pRepsAft_ioffsets, this->pRepsAftTimestamps, CompareTimestampPairsByEnd, Q.end, next_fromRaftA, result);
                }
                else
                {
                    this->scanPartition_CheckEnd_Overlapped(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, Q.end, next_fromOaftA, result);
                    this->scanPartition_CheckOneCondition_Overlaps(l, a, this->pRepsAft_ioffsets, this->pRepsAftTimestamps, CompareTimestampPairsByEnd, Q.end, next_fromRaftA, result);
                }
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
        iterIBegin = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
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
        vector<pair< Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, iterEnd, make_pair(Q.start, Q.start), CompareTimestampPairsByStart);
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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Contains(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromOinB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOaftAB = -1, next_toOaftAB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            if (a+1 >= b)
                break;

            this->scanPartitions_NoChecks_gOverlaps(l, a+1, b-1, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, result);   // Re-using function from overlap
            this->scanPartitions_CheckEnd_Contains(l, a+1, b-1, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, Q.end, next_fromOaftAB, next_toOaftAB, result);
        }
        else
        {
            if (a == b)
            {
                this->scanPartition_CheckBothTimestamps_Contains(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinA, result);
            }
            else
            {
                if (!foundone)
                {
                    this->scanPartition_CheckStart_Contains(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, next_fromOinA, result);
                    this->scanPartition_CheckBothTimestamps_Contains(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOaftA, result);
                }

                this->scanPartitions_NoChecks_gOverlaps(l, a+1, b-1, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, result);   // Re-using function from overlap
                this->scanPartitions_CheckEnd_Contains(l, a+1, b-1, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, Q.end, next_fromOaftAB, next_toOaftAB, result);

                if (!foundzero)
                {
                    this->scanPartition_CheckEnd_Overlapped(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, Q.end, next_fromOinB, result);
                }
            }
        }

        if (a%2) //last bit of a is 1
            foundone = 1;
        if (!(b%2)) //last bit of b is 0
            foundzero = 1;
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }

    if (!(foundone && foundzero))
    {
        iterI = this->pOrgsInIds[this->numBits].begin();
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iter = lower_bound(iterBegin, iterEnd, make_pair(Q.start+1, Q.start+1), CompareTimestampPairsByStart);
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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Contained(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOaftA = -1, next_fromRinA = -1, next_fromRaftA = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (foundone && foundzero)
//            break;

        if (a == b)
        {
            this->scanPartition_CheckBothTimestamps_Contained(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinA, result);
            this->scanPartition_CheckStart_Contained(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, next_fromOaftA, result);
            this->scanPartition_CheckEnd_Contained(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.end, next_fromRinA, result);
            this->scanPartition_NoChecks_gOverlaps(l, a, this->pRepsAft_ioffsets, this->pRepsAftIds, next_fromRaftA, result);
        }
        else
        {
            this->scanPartition_CheckBothTimestamps_Contained(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOaftA, result);
            this->scanPartition_CheckEnd_Contained(l, a, this->pRepsAft_ioffsets, this->pRepsAftTimestamps, CompareTimestampPairsByEnd, Q.end, next_fromRaftA, result);
        }

//        if (a%2) //last bit of b is 1
//            foundone = 1;
//        if (!(a%2)) //last bit of a is 0
//            foundzero = 1;
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }

//    if (!(foundone && foundzero))
//    {
        // Comparisons needed
    iterI = this->pOrgsInIds[this->numBits].begin();
    iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
    vector<pair< Timestamp, Timestamp> >::iterator pivot = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits].end(), make_pair(Q.start, Q.start), CompareTimestampPairsByStart);
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
//    }


    return result;
}


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Precedes(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp b = Q.end >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinB = -1, next_fromOaftB = -1, next_fromOinAB = -1, next_fromOaftAB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (!(foundone && foundzero))
        if (!foundone)
        {
            this->scanPartition_Precedes(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.end+1, next_fromOinB, result);
            this->scanPartition_Precedes(l, b, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.end+1, next_fromOaftB, result);
        }

        // TODO: test effectiveness on Mogon
        if (b+1 >= (int)(pow(2, this->numBits-l)))
            break;

        this->scanPartitions_Precedes(l, b+1, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, next_fromOinAB, result);
        this->scanPartitions_Precedes(l, b+1, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, next_fromOaftAB, result);

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
        iter = lower_bound(iterBegin, iterEnd, make_pair(Q.end+1, Q.end+1), CompareTimestampPairsByStart);
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
        iterIBegin = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
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


size_t HINT_M_SubsSort_SS_CM::executeBottomUp_Preceded(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromRinA = -1, next_fromOinAB = -1, next_fromRinAB = -1;


    for (auto l = 0; l < this->numBits; l++)
    {
//        if (!(foundone && foundzero))
        if (!foundzero)
        {
            this->scanPartition_Preceded(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, Q.start, next_fromOinA, result);
            this->scanPartition_Preceded(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, next_fromRinA, result);
        }

        // TODO: test effectiveness on Mogon
        if (a == 0)
            break;
//        if (a > 0)
//        {
            this->scanPartitions_Preceded(l, a, this->pOrgsIn_ioffsets, this->pOrgsInIds, this->pOrgsInTimestamps, next_fromOinAB, result);
            this->scanPartitions_Preceded(l, a, this->pRepsIn_ioffsets, this->pRepsInIds, this->pRepsInTimestamps, next_fromRinAB, result);
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
        iterBegin = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = this->pOrgsInTimestamps[this->numBits].end();
        iterI = this->pOrgsInIds[this->numBits].begin();
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
        iterIBegin = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
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
size_t HINT_M_SubsSort_SS_CM::executeBottomUp_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
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
            this->scanPartition_NoChecks_gOverlaps(l, a, this->pRepsIn_ioffsets, this->pRepsInIds, next_fromRinA, result);
            this->scanPartition_NoChecks_gOverlaps(l, a, this->pRepsAft_ioffsets, this->pRepsAftIds, next_fromRaftA, result);

            this->scanPartitions_NoChecks_gOverlaps(l, a, b, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, result);
            this->scanPartitions_NoChecks_gOverlaps(l, a, b, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftAB, next_toOaftAB, result);
        }
        else
        {
            // Comparisons needed

            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                this->scanPartition_CheckBothTimestamps_gOverlaps(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.start, Q.end, next_fromOinA, result);
                this->scanPartition_CheckStart_gOverlaps(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.end, next_fromOaftA, result);
            }
            else
            {
                // Lemma 1
                this->scanPartition_CheckEnd_gOverlaps(l, a, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, Q.start, next_fromOinA, result);
                this->scanPartition_NoChecks_gOverlaps(l, a, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftA, result);
            }

            // Lemma 1, 3
            this->scanPartition_CheckEnd_gOverlaps(l, a, this->pRepsIn_ioffsets, this->pRepsInTimestamps, CompareTimestampPairsByEnd, Q.start, next_fromRinA, result);
            this->scanPartition_NoChecks_gOverlaps(l, a, this->pRepsAft_ioffsets, this->pRepsAftIds, next_fromRaftA, result);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_NoChecks_gOverlaps(l, a+1, b-1, this->pOrgsIn_ioffsets, this->pOrgsInIds, next_fromOinAB, next_toOinAB, result);
                this->scanPartitions_NoChecks_gOverlaps(l, a+1, b-1, this->pOrgsAft_ioffsets, this->pOrgsAftIds, next_fromOaftAB, next_toOaftAB, result);

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanPartition_CheckStart_gOverlaps(l, b, this->pOrgsIn_ioffsets, this->pOrgsInTimestamps, CompareTimestampPairsByStart, Q.end, next_fromOinB, result);
                this->scanPartition_CheckStart_gOverlaps(l, b, this->pOrgsAft_ioffsets, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, Q.end, next_fromOaftB, result);
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
        iterIBegin = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
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
        iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits].end(), make_pair<Timestamp, Timestamp>(Q.end+1, Q.end+1), CompareTimestampPairsByStart);
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
