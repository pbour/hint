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

#include "hint_m.h"



inline void HINT_M::updateCounters(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0;
    
    
    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
                //printf("added to level %d, bucket %d, class B\n",level,a);
                this->pReps_sizes[level][a]++;
            }
            else
            {
                //printf("added to level %d, bucket %d, class A\n",level,a);
                this->pOrgs_sizes[level][a]++;
                firstfound = 1;
            }
            //a+=(int)(pow(2,level));
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            //b-=(int)(pow(2,level));
            b--;
            if ((!firstfound) && b<a)
            {
                //printf("added to level %d, bucket %d, class A\n",level,prevb);
                this->pOrgs_sizes[level][prevb]++;
            }
            else
            {
                //printf("added to level %d, bucket %d, class B\n",level,prevb);
                this->pReps_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


inline void HINT_M::updatePartitions(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0;
    
    
    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
                this->pReps[level][a][this->pReps_sizes[level][a]] = r;
                this->pReps_sizes[level][a]++;
            }
            else
            {
                this->pOrgs[level][a][this->pOrgs_sizes[level][a]] = r;
                this->pOrgs_sizes[level][a]++;
                firstfound = 1;
            }
            //a+=(int)(pow(2,level));
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            b--;
            //b-=(int)(pow(2,level));
            if ((!firstfound) && b < a)
            {
                this->pOrgs[level][prevb][this->pOrgs_sizes[level][prevb]] = r;
                this->pOrgs_sizes[level][prevb]++;
            }
            else
            {
                this->pReps[level][prevb][this->pReps_sizes[level][prevb]] = r;
                this->pReps_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M::HINT_M(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgs_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pReps_sizes = (size_t **)malloc(this->height*sizeof(size_t *));

    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgs_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pReps_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);

    
    // Step 2: allocate necessary memory.
    this->pOrgs = new Relation*[this->height];
    this->pReps = new Relation*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));

        this->pOrgs[l] = new Relation[cnt];
        this->pReps[l] = new Relation[cnt];

        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgs[l][j].resize(this->pOrgs_sizes[l][j]);
            this->pReps[l][j].resize(this->pReps_sizes[l][j]);
        }
    }
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        memset(this->pOrgs_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pReps_sizes[l], 0, cnt*sizeof(size_t));
    }


    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);


    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(pOrgs_sizes[l]);
        free(pReps_sizes[l]);
    }
    free(pOrgs_sizes);
    free(pReps_sizes);
}


void HINT_M::print(char c)
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        printf("Level %d: %d partitions\n", l, cnt);
        for (auto j = 0; j < cnt; j++)
        {
            printf("Orgs %d (%d): ", j, this->pOrgs[l][j].size());
//            for (auto k = 0; k < this->bucketcountersA[i][j]; k++)
//                printf("%d ", this->pOrgs[i][j][k].id);
            printf("\n");
            printf("Reps %d (%d): ", j, this->pReps[l][j].size());
//            for (auto k = 0; k < this->bucketcountersB[i][j]; k++)
//                printf("%d ", this->pReps[i][j][k].id);
            printf("\n\n");
        }
    }
}


void HINT_M::getStats()
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);

        this->numPartitions += cnt;
        for (int j = 0; j < cnt; j++)
        {
            this->numReplicas += this->pReps[l][j].size();
            if ((this->pOrgs[l][j].empty()) && (this->pReps[l][j].empty()))
                this->numEmptyPartitions++;
        }
    }

    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicas)/(this->numPartitions-numEmptyPartitions);
}


HINT_M::~HINT_M()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgs[l];
        delete[] this->pReps[l];
    }
    delete[] this->pOrgs;
    delete[] this->pReps;
}


// Generalized predicates, ACM SIGMOD'22 gOverlaps
size_t HINT_M::executeTopDown_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterStart, iterEnd;
    RelationIterator iter;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        // Handle the partition that contains a: consider both originals and replicas, comparisons needed
        iterStart = this->pOrgs[l][a].begin();
        iterEnd = this->pOrgs[l][a].end();
        for (iter = iterStart; iter != iterEnd; iter++)
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
        
        iterStart = this->pReps[l][a].begin();
        iterEnd = this->pReps[l][a].end();
        for (iter = iterStart; iter != iterEnd; iter++)
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
        
        if (a < b)
        {
            // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
            for (auto j = a+1; j < b; j++)
            {
                iterStart = this->pOrgs[l][j].begin();
                iterEnd = this->pOrgs[l][j].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iter->id;
#endif
                }
            }
            
            // Handle the partition that contains b: consider only originals, comparisons needed
            iterStart = this->pOrgs[l][b].begin();
            iterEnd = this->pOrgs[l][b].end();
            for (iter = iterStart; iter != iterEnd; iter++)
            {
                if (iter->start <= Q.end)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iter->id;
#endif
                }
            }
        }
        
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root: consider only originals, comparisons needed
    iterStart = this->pOrgs[this->numBits][0].begin();
    iterEnd = this->pOrgs[this->numBits][0].end();
    for (iter = iterStart; iter != iterEnd; iter++)
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
    
    
    return result;
}


size_t HINT_M::executeBottomUp_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterStart;
    RelationIterator iter, iterEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            iterStart = this->pReps[l][a].begin();
            iterEnd = this->pReps[l][a].end();
            for (iter = iterStart; iter != iterEnd; iter++)
            {
#ifdef WORKLOAD_COUNT
                result++;
#else
                result ^= iter->id;
#endif
            }
            
            // Handle rest: consider only originals
            for (auto j = a; j <= b; j++)
            {
                iterStart = this->pOrgs[l][j].begin();
                iterEnd = this->pOrgs[l][j].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iter->id;
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
                if (!foundzero && !foundone)
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
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
                }
                else if (foundzero)
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (iter->start <= Q.end)
                        {
#ifdef WORKLOAD_COUNT
                            result++;
#else
                            result ^= iter->id;
#endif
                        }
                    }
                }
                else if (foundone)
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
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
            }
            else
            {
                // Lemma 1
                if (!foundzero)
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
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
                else
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
#ifdef WORKLOAD_COUNT
                        result++;
#else
                        result ^= iter->id;
#endif
                    }
                }
            }
            
            // Lemma 1, 3
            if (!foundzero)
            {
                //TODO with
                iterStart = this->pReps[l][a].begin();
                iterEnd = this->pReps[l][a].end();
                for (iter = iterStart; iter != iterEnd; iter++)
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
            else
            {
                iterStart = this->pReps[l][a].begin();
                iterEnd = this->pReps[l][a].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                {
#ifdef WORKLOAD_COUNT
                    result++;
#else
                    result ^= iter->id;
#endif
                }
            }
            
            if (a < b)
            {
                if (!foundone)
                {
                    // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                    for (auto j = a+1; j < b; j++)
                    {
                        iterStart = this->pOrgs[l][j].begin();
                        iterEnd = this->pOrgs[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
                        {
#ifdef WORKLOAD_COUNT
                            result++;
#else
                            result ^= iter->id;
#endif
                        }
                    }
                    
                    // Handle the partition that contains b: consider only originals, comparisons needed
                    iterStart = this->pOrgs[l][b].begin();
                    iterEnd = this->pOrgs[l][b].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (iter->start <= Q.end)
                        {
#ifdef WORKLOAD_COUNT
                            result++;
#else
                            result ^= iter->id;
#endif
                        }
                    }
                }
                else
                {
                    for (auto j = a+1; j <= b; j++)
                    {
                        iterStart = this->pOrgs[l][j].begin();
                        iterEnd = this->pOrgs[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
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
            
            if ((!foundone) && (b%2)) //last bit of b is 1
                foundone = 1;
            if ((!foundzero) && (!(a%2))) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterStart = this->pOrgs[this->numBits][0].begin();
        iterEnd = this->pOrgs[this->numBits][0].end();
        for (iter = iterStart; iter != iterEnd; iter++)
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
        iterStart = this->pOrgs[this->numBits][0].begin();
        iterEnd = this->pOrgs[this->numBits][0].end();
        for (iter = iterStart; iter != iterEnd; iter++)
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
    }
    
    
    return result;
}
