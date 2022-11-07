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



inline void HINT_M_SubsSortSopt_CM::updateCounters(const Record &r)
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


inline void HINT_M_SubsSortSopt_CM::updatePartitions(const Record &r)
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
                    this->pRepsInTmp[level][a][this->pRepsIn_sizes[level][a]] = RecordEnd(r.id, r.end);
                    this->pRepsIn_sizes[level][a]++;
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAft[level][a][this->pRepsAft_sizes[level][a]] = r.id;
                    this->pRepsAft_sizes[level][a]++;
                }
            }
            else
            {
                if ((a == b) && (!lastfound))
                {
                    this->pOrgsInTmp[level][a][this->pOrgsIn_sizes[level][a]] = Record(r.id, r.start, r.end);
                    this->pOrgsIn_sizes[level][a]++;
                }
                else
                {
                    this->pOrgsAftTmp[level][a][this->pOrgsAft_sizes[level][a]] = RecordStart(r.id, r.start);
                    this->pOrgsAft_sizes[level][a]++;
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
                    this->pOrgsInTmp[level][prevb][this->pOrgsIn_sizes[level][prevb]] = Record(r.id, r.start, r.end);
                    this->pOrgsIn_sizes[level][prevb]++;
                }
                else
                {
                    this->pOrgsAftTmp[level][prevb][this->pOrgsAft_sizes[level][prevb]] = RecordStart(r.id, r.start);
                    this->pOrgsAft_sizes[level][prevb]++;
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsInTmp[level][prevb][this->pRepsIn_sizes[level][prevb]] = RecordEnd(r.id, r.end);
                    this->pRepsIn_sizes[level][prevb]++;
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAft[level][prevb][this->pRepsAft_sizes[level][prevb]] = r.id;
                    this->pRepsAft_sizes[level][prevb]++;
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSortSopt_CM::HINT_M_SubsSortSopt_CM(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsAft_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsAft_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsAft_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsAft_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    // Step 2: allocate necessary memory.
    this->pOrgsInTmp  = new Relation*[this->height];
    this->pOrgsAftTmp = new RelationStart*[this->height];
    this->pRepsInTmp  = new RelationEnd*[this->height];
    this->pRepsAft = new RelationId*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsInTmp[l]  = new Relation[cnt];
        this->pOrgsAftTmp[l] = new RelationStart[cnt];
        this->pRepsInTmp[l]  = new RelationEnd[cnt];
        this->pRepsAft[l] = new RelationId[cnt];
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsInTmp[l][pId].resize(this->pOrgsIn_sizes[l][pId]);
            this->pOrgsAftTmp[l][pId].resize(this->pOrgsAft_sizes[l][pId]);
            this->pRepsInTmp[l][pId].resize(this->pRepsIn_sizes[l][pId]);
            this->pRepsAft[l][pId].resize(this->pRepsAft_sizes[l][pId]);
        }
    }
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        memset(this->pOrgsIn_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pOrgsAft_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pRepsIn_sizes[l], 0, cnt*sizeof(size_t));
        memset(this->pRepsAft_sizes[l], 0, cnt*sizeof(size_t));
    }
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    // Step 4: sort partition contents.
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsInTmp[l][pId].sortByStart();
            this->pOrgsAftTmp[l][pId].sortByStart();
            this->pRepsInTmp[l][pId].sortByEnd();
        }
    }
    
    // Free auxiliary memory.
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
    
    // Copy and free auxiliary memory.
    this->pOrgsInIds  = new RelationId*[this->height];
    this->pOrgsAftIds = new RelationId*[this->height];
    this->pRepsInIds  = new RelationId*[this->height];
    this->pOrgsInTimestamps = new vector<pair<Timestamp, Timestamp> >*[this->height];
    this->pOrgsAftTimestamp = new vector<Timestamp>*[this->height];
    this->pRepsInTimestamp  = new vector<Timestamp>*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsInIds[l]  = new RelationId[cnt];
        this->pOrgsAftIds[l] = new RelationId[cnt];
        this->pRepsInIds[l]  = new RelationId[cnt];
        this->pOrgsInTimestamps[l] = new vector<pair<Timestamp, Timestamp> >[cnt];
        this->pOrgsAftTimestamp[l] = new vector<Timestamp>[cnt];
        this->pRepsInTimestamp[l]  = new vector<Timestamp>[cnt];
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            auto cnt = this->pOrgsInTmp[l][pId].size();
            this->pOrgsInIds[l][pId].resize(cnt);
            this->pOrgsInTimestamps[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pOrgsInIds[l][pId][j] = this->pOrgsInTmp[l][pId][j].id;
                this->pOrgsInTimestamps[l][pId][j].first = this->pOrgsInTmp[l][pId][j].start;
                this->pOrgsInTimestamps[l][pId][j].second = this->pOrgsInTmp[l][pId][j].end;
            }
            
            cnt = this->pOrgsAftTmp[l][pId].size();
            this->pOrgsAftIds[l][pId].resize(cnt);
            this->pOrgsAftTimestamp[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pOrgsAftIds[l][pId][j] = this->pOrgsAftTmp[l][pId][j].id;
                this->pOrgsAftTimestamp[l][pId][j] = this->pOrgsAftTmp[l][pId][j].start;
            }
            
            cnt = this->pRepsInTmp[l][pId].size();
            this->pRepsInIds[l][pId].resize(cnt);
            this->pRepsInTimestamp[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pRepsInIds[l][pId][j] = this->pRepsInTmp[l][pId][j].id;
                this->pRepsInTimestamp[l][pId][j] = this->pRepsInTmp[l][pId][j].end;
            }
        }
        
        delete[] this->pOrgsInTmp[l];
        delete[] this->pOrgsAftTmp[l];
        delete[] this->pRepsInTmp[l];
    }
    delete[] this->pOrgsInTmp;
    delete[] this->pOrgsAftTmp;
    delete[] this->pRepsInTmp;
}


void HINT_M_SubsSortSopt_CM::getStats()
{
    size_t sum = 0;
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        this->numPartitions += cnt;
        for (int pid = 0; pid < cnt; pid++)
        {
            this->numOriginalsIn  += this->pOrgsInIds[l][pid].size();
            this->numOriginalsAft += this->pOrgsAftIds[l][pid].size();
            this->numReplicasIn   += this->pRepsInIds[l][pid].size();
            this->numReplicasAft  += this->pRepsAft[l][pid].size();
            if ((this->pOrgsInIds[l][pid].empty()) && (this->pOrgsAftIds[l][pid].empty()) && (this->pRepsInIds[l][pid].empty()) && (this->pRepsAft[l][pid].empty()))
                this->numEmptyPartitions++;
        }
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasAft)/(this->numPartitions-numEmptyPartitions);
}


HINT_M_SubsSortSopt_CM::~HINT_M_SubsSortSopt_CM()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsInIds[l];
        delete[] this->pOrgsInTimestamps[l];
        delete[] this->pOrgsAftIds[l];
        delete[] this->pOrgsAftTimestamp[l];
        delete[] this->pRepsInIds[l];
        delete[] this->pRepsInTimestamp[l];
        delete[] this->pRepsAft[l];
    }
    
    delete[] this->pOrgsInIds;
    delete[] this->pOrgsInTimestamps;
    delete[] this->pOrgsAftIds;
    delete[] this->pOrgsAftTimestamp;
    delete[] this->pRepsInIds;
    delete[] this->pRepsInTimestamp;
    delete[] this->pRepsAft;
}


// Generalized predicates, ACM SIGMOD'22 gOverlaps
size_t HINT_M_SubsSortSopt_CM::executeBottomUp_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterBegin, iterEnd;
    vector<Timestamp>::iterator iterS, iterSBegin, iterSEnd;
    vector<Timestamp>::iterator iterE, iterEBegin, iterEEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyE(Q.end+1, Q.end+1);
    Timestamp qdummySE = Q.end+1;
    Timestamp qdummyS = Q.start;
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            iterIBegin = this->pRepsInIds[l][a].begin();
            iterIEnd = this->pRepsInIds[l][a].end();
            for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            iterIBegin =this->pRepsAft[l][a].begin();
            iterIEnd = this->pRepsAft[l][a].end();
            for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            
            // Handle rest: consider only originals
            for (auto j = a; j <= b; j++)
            {
                iterIBegin = this->pOrgsInIds[l][j].begin();
                iterIEnd = this->pOrgsInIds[l][j].end();
                for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= (*iterI);
#endif
                }
                iterIBegin = this->pOrgsAftIds[l][j].begin();
                iterIEnd = this->pOrgsAftIds[l][j].end();
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
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                iterBegin = this->pOrgsInTimestamps[l][a].begin();
                iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[l][a].end(), qdummyE, CompareTimestampPairsByStart);
                for (iter = iterBegin; iter != iterEnd; iter++)
                {
                    if (Q.start <= iter->second)
                    {
#ifdef WORKLOAD_COUNT
                        result++;
#else
                        result ^= this->pOrgsInIds[l][a][iter-iterBegin];
#endif
                    }
                }
                iterSBegin = this->pOrgsAftTimestamp[l][a].begin();
                iterSEnd = lower_bound(iterSBegin, this->pOrgsAftTimestamp[l][a].end(), qdummySE);
                for (iterS = iterSBegin; iterS != iterSEnd; iterS++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= this->pOrgsAftIds[l][a][iterS-iterSBegin];
#endif
                }
            }
            else
            {
                // Lemma 1
                iterBegin = this->pOrgsInTimestamps[l][a].begin();
                iterEnd = this->pOrgsInTimestamps[l][a].end();
                for (iter = iterBegin; iter != iterEnd; iter++)
                {
                    if (Q.start <= iter->second)
                    {
#ifdef WORKLOAD_COUNT
                        result++;
#else
                        result ^= this->pOrgsInIds[l][a][iter-iterBegin];
#endif
                    }
                }
                iterSBegin = this->pOrgsAftTimestamp[l][a].begin();
                iterSEnd = this->pOrgsAftTimestamp[l][a].end();
                for (vector<Timestamp>::iterator iter = iterSBegin; iter != iterSEnd; iter++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= this->pOrgsAftIds[l][a][iter-iterSBegin];
#endif
                }
            }
            
            // Lemma 1, 3
            iterEBegin = this->pRepsInTimestamp[l][a].begin();
            iterEEnd = this->pRepsInTimestamp[l][a].end();
            vector<Timestamp>::iterator pivot = lower_bound(iterEBegin, iterEEnd, qdummyS);
            for (iterE = pivot; iterE != iterEEnd; iterE++)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= this->pRepsInIds[l][a][iterE-iterEBegin];
#endif
            }
            iterIBegin = this->pRepsAft[l][a].begin();
            iterIEnd = this->pRepsAft[l][a].end();
            for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= (*iterI);
#endif
            }
            
            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                for (auto j = a+1; j < b; j++)
                {
                    iterIBegin = this->pOrgsInIds[l][j].begin();
                    iterIEnd = this->pOrgsInIds[l][j].end();
                    for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
                    {
#ifdef WORKLOAD_COUNT
                        result++;
#else
                        result ^= (*iterI);
#endif
                    }
                    iterIBegin = this->pOrgsAftIds[l][j].begin();
                    iterIEnd = this->pOrgsAftIds[l][j].end();
                    for (iterI = iterIBegin; iterI != iterIEnd; iterI++)
                    {
#ifdef WORKLOAD_COUNT
                        result++;
#else
                        result ^= (*iterI);
#endif
                    }
                }
                
                // Handle the partition that contains b: consider only originals, comparisons needed
                iterBegin = this->pOrgsInTimestamps[l][b].begin();
                iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[l][b].end(), qdummyE, CompareTimestampPairsByStart);
                for (iter = iterBegin; iter != iterEnd; iter++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= this->pOrgsInIds[l][b][iter-iterBegin];
#endif
                }
                iterSBegin = this->pOrgsAftTimestamp[l][b].begin();
                iterSEnd = lower_bound(iterSBegin, this->pOrgsAftTimestamp[l][b].end(), qdummySE);
                for (iterS = iterSBegin; iterS != iterSEnd; iterS++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= this->pOrgsAftIds[l][b][iterS-iterSBegin];
#endif
                }
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
        iterIBegin = this->pOrgsInIds[this->numBits][0].begin();
        iterIEnd = this->pOrgsInIds[this->numBits][0].end();
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
        iterBegin = this->pOrgsInTimestamps[this->numBits][0].begin();
        iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits][0].end(), qdummyE, CompareTimestampPairsByStart);
        for (iter = iterBegin; iter != iterEnd; iter++)
        {
            if (Q.start <= iter->second)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= this->pOrgsInIds[this->numBits][0][iter-iterBegin];
#endif
            }
        }
    }
    
    
    return result;
}
