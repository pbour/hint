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



inline void HINT_M_SubsSortSopt::updateCounters(const Record &r)
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


inline void HINT_M_SubsSortSopt::updatePartitions(const Record &r)
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
                    this->pRepsIn[level][a][this->pRepsIn_sizes[level][a]] = RecordEnd(r.id, r.end);
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
                    this->pOrgsIn[level][a][this->pOrgsIn_sizes[level][a]] = Record(r.id, r.start, r.end);
                    this->pOrgsIn_sizes[level][a]++;
                }
                else
                {
                    this->pOrgsAft[level][a][this->pOrgsAft_sizes[level][a]] = RecordStart(r.id, r.start);
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
                    this->pOrgsIn[level][prevb][this->pOrgsIn_sizes[level][prevb]] = Record(r.id, r.start, r.end);
                    this->pOrgsIn_sizes[level][prevb]++;
                }
                else
                {
                    this->pOrgsAft[level][prevb][this->pOrgsAft_sizes[level][prevb]] = RecordStart(r.id, r.start);
                    this->pOrgsAft_sizes[level][prevb]++;
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsIn[level][prevb][this->pRepsIn_sizes[level][prevb]] = RecordEnd(r.id, r.end);
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


HINT_M_SubsSortSopt::HINT_M_SubsSortSopt(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Initialize statistics
    this->numOriginalsIn = this->numOriginalsAft = this->numReplicasIn = this->numReplicasAft = 0;
    
    
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
    this->pOrgsIn  = new Relation*[this->height];
    this->pOrgsAft = new RelationStart*[this->height];
    this->pRepsIn  = new RelationEnd*[this->height];
    this->pRepsAft = new RelationId*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsIn[l]  = new Relation[cnt];
        this->pOrgsAft[l] = new RelationStart[cnt];
        this->pRepsIn[l]  = new RelationEnd[cnt];
        this->pRepsAft[l] = new RelationId[cnt];
        
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsIn[l][j].resize(this->pOrgsIn_sizes[l][j]);
            this->pOrgsAft[l][j].resize(this->pOrgsAft_sizes[l][j]);
            this->pRepsIn[l][j].resize(this->pRepsIn_sizes[l][j]);
            this->pRepsAft[l][j].resize(this->pRepsAft_sizes[l][j]);
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
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsIn[l][j].sortByStart();
            this->pOrgsAft[l][j].sortByStart();
            this->pRepsIn[l][j].sortByEnd();
        }
    }

    
    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(pOrgsIn_sizes[l]);
        free(pOrgsAft_sizes[l]);
        free(pRepsIn_sizes[l]);
        free(pRepsAft_sizes[l]);
    }
    free(pOrgsIn_sizes);
    free(pOrgsAft_sizes);
    free(pRepsIn_sizes);
    free(pRepsAft_sizes);
}


void HINT_M_SubsSortSopt::print(char c)
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        printf("Level %d: %d partitions\n", l, cnt);
        for (auto j = 0; j < cnt; j++)
        {
            printf("OrgsIn %d (%d): ", j, this->pOrgsIn[l][j].size());
            //            for (auto k = 0; k < this->bucketcountersA[i][j]; k++)
            //                printf("%d ", this->pOrgs[i][j][k].id);
            printf("\n");
            printf("OrgsAft %d (%d): ", j, this->pOrgsAft[l][j].size());
            printf("\n");
            printf("RepsIn %d (%d): ", j, this->pRepsIn[l][j].size());
            //            for (auto k = 0; k < this->bucketcountersB[i][j]; k++)
            //                printf("%d ", this->pReps[i][j][k].id);
            printf("\n");
            printf("RepsAft %d (%d): ", j, this->pRepsAft[l][j].size());
            printf("\n\n");
        }
    }
}


void HINT_M_SubsSortSopt::getStats()
{
    size_t sum = 0;
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        this->numPartitions += cnt;
        for (int j = 0; j < cnt; j++)
        {
            this->numOriginalsIn  += this->pOrgsIn[l][j].size();
            this->numOriginalsAft += this->pOrgsAft[l][j].size();
            this->numReplicasIn   += this->pRepsIn[l][j].size();
            this->numReplicasAft  += this->pRepsAft[l][j].size();
            if ((this->pOrgsIn[l][j].empty()) && (this->pOrgsAft[l][j].empty()) && (this->pRepsIn[l][j].empty()) && (this->pRepsAft[l][j].empty()))
                this->numEmptyPartitions++;
        }
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasAft)/(this->numPartitions-numEmptyPartitions);
}


HINT_M_SubsSortSopt::~HINT_M_SubsSortSopt()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsIn[l];
        delete[] this->pOrgsAft[l];
        delete[] this->pRepsIn[l];
        delete[] this->pRepsAft[l];
    }
    
    delete[] this->pOrgsIn;
    delete[] this->pOrgsAft;
    delete[] this->pRepsIn;
    delete[] this->pRepsAft;
}


// Generalized predicates, ACM SIGMOD'22 gOverlaps
size_t HINT_M_SubsSortSopt::executeBottomUp_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterBegin;
    RelationIterator iter, iterEnd;
    RelationStart::iterator iterS, iterSBegin, iterSEnd;
    RelationEnd::iterator iterE, iterEBegin, iterEEnd;
    RelationIdIterator iterI, iterIBegin, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    Record qdummyE(0, Q.end+1, Q.end+1);
    RecordStart qdummySE(0, Q.end+1);
    RecordEnd qdummyS(0, Q.start);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            iterEBegin = this->pRepsIn[l][a].begin();
            iterEEnd = this->pRepsIn[l][a].end();
            for (iterE = iterEBegin; iterE != iterEEnd; iterE++)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iterE->id;
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
                iterBegin = this->pOrgsIn[l][j].begin();
                iterEnd = this->pOrgsIn[l][j].end();
                for (iter = iterBegin; iter != iterEnd; iter++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iter->id;
#endif
                }
                iterSBegin = this->pOrgsAft[l][j].begin();
                iterSEnd = this->pOrgsAft[l][j].end();
                for (iterS = iterSBegin; iterS != iterSEnd; iterS++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iterS->id;
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
                iterBegin = this->pOrgsIn[l][a].begin();
                iterEnd = lower_bound(iterBegin, this->pOrgsIn[l][a].end(), qdummyE);
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
                iterSBegin = this->pOrgsAft[l][a].begin();
                iterSEnd = lower_bound(iterSBegin, this->pOrgsAft[l][a].end(), qdummySE);
                for (iterS = iterSBegin; iterS != iterSEnd; iterS++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iterS->id;
#endif
                }
            }
            else
            {
                // Lemma 1
                iterBegin = this->pOrgsIn[l][a].begin();
                iterEnd = this->pOrgsIn[l][a].end();
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
                iterSBegin = this->pOrgsAft[l][a].begin();
                iterSEnd = this->pOrgsAft[l][a].end();
                for (iterS = iterSBegin; iterS != iterSEnd; iterS++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iterS->id;
#endif
                }
            }
            
            // Lemma 1, 3
            iterEEnd = this->pRepsIn[l][a].end();
            iterEBegin = lower_bound(this->pRepsIn[l][a].begin(), iterEEnd, qdummyS);
            for (iterE = iterEBegin; iterE != iterEEnd; iterE++)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iterE->id;
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
                    iterBegin = this->pOrgsIn[l][j].begin();
                    iterEnd = this->pOrgsIn[l][j].end();
                    for (iter = iterBegin; iter != iterEnd; iter++)
                    {
#ifdef WORKLOAD_COUNT
                        result++;
#else
                        result ^= iter->id;
#endif
                    }
                    iterSBegin = this->pOrgsAft[l][j].begin();
                    iterSEnd = this->pOrgsAft[l][j].end();
                    for (iterS = iterSBegin; iterS != iterSEnd; iterS++)
                    {
#ifdef WORKLOAD_COUNT
                        result++;
#else
                        result ^= iterS->id;
#endif
                    }
                }
                
                // Handle the partition that contains b: consider only originals, comparisons needed
                iterBegin = this->pOrgsIn[l][b].begin();
                iterEnd = lower_bound(iterBegin, this->pOrgsIn[l][b].end(), qdummyE);
                for (iter = iterBegin; iter != iterEnd; iter++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iter->id;
#endif
                }
                iterSBegin = this->pOrgsAft[l][b].begin();
                iterSEnd = lower_bound(iterSBegin, this->pOrgsAft[l][b].end(), qdummySE);
                for (iterS = iterSBegin; iterS != iterSEnd; iterS++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iterS->id;
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
        iterBegin = this->pOrgsIn[this->numBits][0].begin();
        iterEnd = this->pOrgsIn[this->numBits][0].end();
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
        iterBegin = this->pOrgsIn[this->numBits][0].begin();
        iterEnd = lower_bound(iterBegin, this->pOrgsIn[this->numBits][0].end(), qdummyE);
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
