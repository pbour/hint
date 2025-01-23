/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   George Christodoulou
 * Author:   Nikos Mamoulis
 ******************************************************************************
 * Copyright (c) 2020 - 2024
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



inline void HINT_M_SubsSort_CM::updateCounters(const Record &r)
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


inline void HINT_M_SubsSort_CM::updatePartitions(const Record &r)
//{
//    int level = 0;
//    Timestamp a = r.start >> (this->maxBits-this->numBits);
//    Timestamp b = r.end   >> (this->maxBits-this->numBits);
//    Timestamp prevb;
//    int firstfound = 0, lastfound = 0;
//
//
//    while (level < this->height && a <= b)
//    {
//        if (a%2)
//        { //last bit of a is 1
//            if (firstfound)
//            {
//                if ((a == b) && (!lastfound))
//                {
//                    this->pRepsInTmp[level][a][this->pRepsIn_sizes[level][a]] = Record(r.id, r.start, r.end);
//                    this->pRepsIn_sizes[level][a]++;
//                    lastfound = 1;
//                }
//                else
//                {
//                    this->pRepsAftTmp[level][a][this->pRepsAft_sizes[level][a]] = Record(r.id, r.start, r.end);
//                    this->pRepsAft_sizes[level][a]++;
//                }
//            }
//            else
//            {
//                if ((a == b) && (!lastfound))
//                {
//                    this->pOrgsInTmp[level][a][this->pOrgsIn_sizes[level][a]] = Record(r.id, r.start, r.end);
//                    this->pOrgsIn_sizes[level][a]++;
//                }
//                else
//                {
//                    this->pOrgsAftTmp[level][a][this->pOrgsAft_sizes[level][a]] = Record(r.id, r.start, r.end);
//                    this->pOrgsAft_sizes[level][a]++;
//                }
//                firstfound = 1;
//            }
//            a++;
//        }
//        if (!(b%2))
//        { //last bit of b is 0
//            prevb = b;
//            b--;
//            if ((!firstfound) && b < a)
//            {
//                if (!lastfound)
//                {
//                    this->pOrgsInTmp[level][prevb][this->pOrgsIn_sizes[level][prevb]] = Record(r.id, r.start, r.end);
//                    this->pOrgsIn_sizes[level][prevb]++;
//                }
//                else
//                {
//                    this->pOrgsAftTmp[level][prevb][this->pOrgsAft_sizes[level][prevb]] = Record(r.id, r.start, r.end);
//                    this->pOrgsAft_sizes[level][prevb]++;
//                }
//            }
//            else
//            {
//                if (!lastfound)
//                {
//                    this->pRepsInTmp[level][prevb][this->pRepsIn_sizes[level][prevb]] = Record(r.id, r.start, r.end);
//                    this->pRepsIn_sizes[level][prevb]++;
//                    lastfound = 1;
//                }
//                else
//                {
//                    this->pRepsAftTmp[level][prevb][this->pRepsAft_sizes[level][prevb]] = Record(r.id, r.start, r.end);
//                    this->pRepsAft_sizes[level][prevb]++;
//                }
//            }
//        }
//        a >>= 1; // a = a div 2
//        b >>= 1; // b = b div 2
//        level++;
//    }
//}
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
                    this->pRepsInTmp[level][a][this->pRepsIn_sizes[level][a]] = Record(r.id, r.start, r.end);;
                    this->pRepsIn_sizes[level][a]++;
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftTmp[level][a][this->pRepsAft_sizes[level][a]] = Record(r.id, r.start, r.end);;
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
                    this->pOrgsAftTmp[level][a][this->pOrgsAft_sizes[level][a]] = Record(r.id, r.start, r.end);
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
                    this->pOrgsAftTmp[level][prevb][this->pOrgsAft_sizes[level][prevb]] = Record(r.id, r.start, r.end);
                    this->pOrgsAft_sizes[level][prevb]++;
                }
            }
            else
            {
                if (!lastfound)
                {
                    this->pRepsInTmp[level][prevb][this->pRepsIn_sizes[level][prevb]] = Record(r.id, r.start, r.end);
                    this->pRepsIn_sizes[level][prevb]++;
                    lastfound = 1;
                }
                else
                {
                    this->pRepsAftTmp[level][prevb][this->pRepsAft_sizes[level][prevb]] = Record(r.id, r.start, r.end);
                    this->pRepsAft_sizes[level][prevb]++;
                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSort_CM::HINT_M_SubsSort_CM(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
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
    this->pOrgsAftTmp = new Relation*[this->height];
    this->pRepsInTmp  = new Relation*[this->height];
    this->pRepsAftTmp = new Relation*[this->height];
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsInTmp[l]  = new Relation[cnt];
        this->pOrgsAftTmp[l] = new Relation[cnt];
        this->pRepsInTmp[l]  = new Relation[cnt];
        this->pRepsAftTmp[l] = new Relation[cnt];
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsInTmp[l][pId].resize(this->pOrgsIn_sizes[l][pId]);
            this->pOrgsAftTmp[l][pId].resize(this->pOrgsAft_sizes[l][pId]);
            this->pRepsInTmp[l][pId].resize(this->pRepsIn_sizes[l][pId]);
            this->pRepsAftTmp[l][pId].resize(this->pRepsAft_sizes[l][pId]);
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
    this->pRepsAftIds = new RelationId*[this->height];
    this->pOrgsInTimestamps  = new vector<pair<Timestamp, Timestamp> >*[this->height];
    this->pOrgsAftTimestamps = new vector<pair<Timestamp, Timestamp> >*[this->height];
    this->pRepsInTimestamps  = new vector<pair<Timestamp, Timestamp> >*[this->height];
    this->pRepsAftTimestamps = new vector<pair<Timestamp, Timestamp> >*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsInIds[l]  = new RelationId[cnt];
        this->pOrgsAftIds[l] = new RelationId[cnt];
        this->pRepsInIds[l]  = new RelationId[cnt];
        this->pRepsAftIds[l] = new RelationId[cnt];
        this->pOrgsInTimestamps[l]  = new vector<pair<Timestamp, Timestamp> >[cnt];
        this->pOrgsAftTimestamps[l] = new vector<pair<Timestamp, Timestamp> >[cnt];
        this->pRepsInTimestamps[l]  = new vector<pair<Timestamp, Timestamp> >[cnt];
        this->pRepsAftTimestamps[l] = new vector<pair<Timestamp, Timestamp> >[cnt];
        
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
            this->pOrgsAftTimestamps[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pOrgsAftIds[l][pId][j] = this->pOrgsAftTmp[l][pId][j].id;
                this->pOrgsAftTimestamps[l][pId][j].first = this->pOrgsAftTmp[l][pId][j].start;
                this->pOrgsAftTimestamps[l][pId][j].second = this->pOrgsAftTmp[l][pId][j].end;
            }
            
            cnt = this->pRepsInTmp[l][pId].size();
            this->pRepsInIds[l][pId].resize(cnt);
            this->pRepsInTimestamps[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pRepsInIds[l][pId][j] = this->pRepsInTmp[l][pId][j].id;
                this->pRepsInTimestamps[l][pId][j].first = this->pRepsInTmp[l][pId][j].start;
                this->pRepsInTimestamps[l][pId][j].second = this->pRepsInTmp[l][pId][j].end;
            }

            cnt = this->pRepsAftTmp[l][pId].size();
            this->pRepsAftIds[l][pId].resize(cnt);
            this->pRepsAftTimestamps[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pRepsAftIds[l][pId][j] = this->pRepsAftTmp[l][pId][j].id;
                this->pRepsAftTimestamps[l][pId][j].first = this->pRepsAftTmp[l][pId][j].start;
                this->pRepsAftTimestamps[l][pId][j].second = this->pRepsAftTmp[l][pId][j].end;
            }
        }
        
        delete[] this->pOrgsInTmp[l];
        delete[] this->pOrgsAftTmp[l];
        delete[] this->pRepsInTmp[l];
        delete[] this->pRepsAftTmp[l];
    }
    delete[] this->pOrgsInTmp;
    delete[] this->pOrgsAftTmp;
    delete[] this->pRepsInTmp;
    delete[] this->pRepsAftTmp;
    
    
//    for (auto l = 0; l < this->numBits; l++)
//    {
//        auto cnt = (int)(pow(2, this->numBits-l));
//        for (auto i = 0; i < cnt; i++)
//        {
//            cout << "pOrgsInMetadata[" << l << "][" << i << "]: <" << this->pOrgsInMetadata[l][i].minStart << ", " << this->pOrgsInMetadata[l][i].maxStart << ", " << this->pOrgsInMetadata[l][i].minEnd << ", " << this->pOrgsInMetadata[l][i].maxEnd << ">\t\t";
//
//            cout << "pOrgsAftMetadata[" << l << "][" << i << "]: <";
//            if ((l == 0) && ((i == 0) || !(i%2)))
//                cout << "N/A, N/A, N/A, N/A>\t\t";
//            else
//                 cout << this->pOrgsAftMetadata[l][i].minStart << ", " << this->pOrgsAftMetadata[l][i].maxStart << ", " << this->pOrgsAftMetadata[l][i].minEnd << ", " << this->pOrgsAftMetadata[l][i].maxEnd << ">\t\t";
//
//            cout << "\t\tpRepsInMetadata[" << l << "][" << i << "]: <";
//            if ((l == 0) && ((i == 0) || (i%2)))
//                cout << "N/A, N/A, N/A, N/A>\t\t";
//            else
//                cout << this->pRepsInMetadata[l][i].minStart << ", " << this->pRepsInMetadata[l][i].maxStart << ", " << this->pRepsInMetadata[l][i].minEnd << ", " << this->pRepsInMetadata[l][i].maxEnd << ">\t\t";
//
//            cout << "\t\tpRepsAftMetadata[" << l << "][" << i << "]: <";
//            if (l == 0)
//                cout << "N/A, N/A, N/A, N/A>" << endl;
//            else
//                cout << this->pRepsAftMetadata[l][i].minStart << ", " << this->pRepsAftMetadata[l][i].maxStart << ", " << this->pRepsAftMetadata[l][i].minEnd << ", " << this->pRepsAftMetadata[l][i].maxEnd << ">" << endl;
//        }
//    }

//    for (auto l = 0; l < this->numBits; l++)
//    {
//        auto cnt = (int)(pow(2, this->numBits-l));
//        for (auto i = 0; i < cnt; i++)
//        {
//            cout << "pOrgsInMetadata[" << l << "][" << i << "]: ";
//            if (!this->pOrgsInIds[l][i].empty())
//                cout << "<" << this->pOrgsInMetadata[l][i].minStart << ", " << this->pOrgsInMetadata[l][i].maxStart << ", " << this->pOrgsInMetadata[l][i].minEnd << ", " << this->pOrgsInMetadata[l][i].maxEnd << ">\t\t";
//            else
//                cout << "<N/A, N/A, N/A, N/A>\t\t";
//
//            cout << "pOrgsAftMetadata[" << l << "][" << i << "]: ";
//            if (!this->pOrgsAftIds[l][i].empty())
//                 cout << "<" << this->pOrgsAftMetadata[l][i].minStart << ", " << this->pOrgsAftMetadata[l][i].maxStart << ", " << this->pOrgsAftMetadata[l][i].minEnd << ", " << this->pOrgsAftMetadata[l][i].maxEnd << ">\t\t";
//            else
//                cout << "<N/A, N/A, N/A, N/A>\t\t";
//
//            cout << "\t\tpRepsInMetadata[" << l << "][" << i << "]: ";
//            if (!this->pRepsInIds[l][i].empty())
//                cout << "<" << this->pRepsInMetadata[l][i].minStart << ", " << this->pRepsInMetadata[l][i].maxStart << ", " << this->pRepsInMetadata[l][i].minEnd << ", " << this->pRepsInMetadata[l][i].maxEnd << ">\t\t";
//            else
//                cout << "<N/A, N/A, N/A, N/A>\t\t";
//
//            cout << "\t\tpRepsAftMetadata[" << l << "][" << i << "]: ";
//            if (!this->pRepsAftIds[l][i].empty())
//                cout << "<" << this->pRepsAftMetadata[l][i].minStart << ", " << this->pRepsAftMetadata[l][i].maxStart << ", " << this->pRepsAftMetadata[l][i].minEnd << ", " << this->pRepsAftMetadata[l][i].maxEnd << ">" << endl;
//            else
//                cout << "<N/A, N/A, N/A, N/A>" << endl;
//        }
//    }
}


void HINT_M_SubsSort_CM::getStats()
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
            this->numReplicasAft  += this->pRepsAftIds[l][pid].size();
            if ((this->pOrgsInIds[l][pid].empty()) && (this->pOrgsAftIds[l][pid].empty()) && (this->pRepsInIds[l][pid].empty()) && (this->pRepsAftIds[l][pid].empty()))
                this->numEmptyPartitions++;
        }
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasAft)/(this->numPartitions-numEmptyPartitions);
}


HINT_M_SubsSort_CM::~HINT_M_SubsSort_CM()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsInIds[l];
        delete[] this->pOrgsInTimestamps[l];
        delete[] this->pOrgsAftIds[l];
        delete[] this->pOrgsAftTimestamps[l];
        delete[] this->pRepsInIds[l];
        delete[] this->pRepsInTimestamps[l];
        delete[] this->pRepsAftIds[l];
    }
    
    delete[] this->pOrgsInIds;
    delete[] this->pOrgsInTimestamps;
    delete[] this->pOrgsAftIds;
    delete[] this->pOrgsAftTimestamps;
    delete[] this->pRepsInIds;
    delete[] this->pRepsInTimestamps;
    delete[] this->pRepsAftIds;
}


// Querying
inline void HINT_M_SubsSort_CM::scanPartition_CheckBothTimestamps_gOverlaps(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), RangeQuery &q, size_t &result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = lower_bound(iterBegin, timestamps[level][t].end(), pair<Timestamp, Timestamp>(q.end+1, q.end+1));
    
    for (auto iter = iterBegin; iter != iterEnd; iter++)
    {
        if (q.start <= iter->second)
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


inline void HINT_M_SubsSort_CM::scanPartition_CheckEnd_gOverlaps(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, RangeQuery &q, size_t &result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = timestamps[level][t].end();

    for (auto iter = iterBegin; iter != iterEnd; iter++)
    {
        if (q.start <= iter->second)
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


inline void HINT_M_SubsSort_CM::scanPartition_CheckEnd_gOverlaps(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), RangeQuery &q, size_t &result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = timestamps[level][t].end();
    auto iter = lower_bound(iterBegin, iterEnd, make_pair(q.start, q.start), compare);
    
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


inline void HINT_M_SubsSort_CM::scanPartition_CheckStart_gOverlaps(unsigned int level, Timestamp t, RelationId **ids, vector<pair<Timestamp, Timestamp>> **timestamps, bool (*compare)(const pair<Timestamp, Timestamp>&, const pair<Timestamp, Timestamp>&), RangeQuery &q, size_t &result)
{
    auto iterI = ids[level][t].begin();
    auto iterBegin = timestamps[level][t].begin();
    auto iterEnd = lower_bound(iterBegin, timestamps[level][t].end(), pair<Timestamp, Timestamp>(q.end+1, q.end+1));

    for (auto iter = iterBegin; iter != iterEnd; iter++)
    {
#ifdef WORKLOAD_COUNT
        result++;
#else
        result ^= (*iterI);
#endif

        iterI++;
    }
}


inline void HINT_M_SubsSort_CM::scanPartition_NoChecks_gOverlaps(unsigned int level, Timestamp t, RelationId **ids, size_t &result)
{
    auto iterIBegin = ids[level][t].begin();
    auto iterIEnd = ids[level][t].end();

    for (auto iterI = iterIBegin; iterI != iterIEnd; iterI++)
    {
#ifdef WORKLOAD_COUNT
        result++;
#else
        result ^= (*iterI);
#endif
    }
}


inline void HINT_M_SubsSort_CM::scanPartitions_NoChecks_gOverlaps(unsigned int level, Timestamp ts, Timestamp te, RelationId **ids, size_t &result)
{
    for (auto j = ts; j <= te; j++)
    {
        auto iterIBegin = ids[level][j].begin();
        auto iterIEnd = ids[level][j].end();
        for (auto iterI = iterIBegin; iterI != iterIEnd; iterI++)
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result ^= (*iterI);
#endif
        }
    }
}


size_t HINT_M_SubsSort_CM::executeBottomUp_gOverlaps(RangeQuery q)
{
    size_t result = 0;
    Timestamp a = q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = q.end   >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            this->scanPartition_NoChecks_gOverlaps(l, a, this->pRepsInIds, result);
            this->scanPartition_NoChecks_gOverlaps(l, a, this->pRepsAftIds, result);
            
            // Handle rest: consider only originals
            this->scanPartitions_NoChecks_gOverlaps(l, a, b, this->pOrgsInIds, result);
            this->scanPartitions_NoChecks_gOverlaps(l, a, b, this->pOrgsAftIds, result);
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                this->scanPartition_CheckBothTimestamps_gOverlaps(l, a, this->pOrgsInIds, this->pOrgsInTimestamps, CompareTimestampPairsByStart, q, result);
                this->scanPartition_CheckStart_gOverlaps(l, a, this->pOrgsAftIds, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, q, result);
            }

            else
            {
                // Lemma 1
                this->scanPartition_CheckEnd_gOverlaps(l, a, this->pOrgsInIds, this->pOrgsInTimestamps, q, result);
                this->scanPartition_NoChecks_gOverlaps(l, a, this->pOrgsAftIds, result);
            }

            // Lemma 1, 3
            this->scanPartition_CheckEnd_gOverlaps(l, a, this->pRepsInIds, this->pRepsInTimestamps, CompareTimestampPairsByEnd, q, result);
            this->scanPartition_NoChecks_gOverlaps(l, a, this->pRepsAftIds, result);

            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_NoChecks_gOverlaps(l, a+1, b-1, this->pOrgsInIds, result);
                this->scanPartitions_NoChecks_gOverlaps(l, a+1, b-1, this->pOrgsAftIds, result);

                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanPartition_CheckStart_gOverlaps(l, b, this->pOrgsInIds, this->pOrgsInTimestamps, CompareTimestampPairsByStart, q, result);
                this->scanPartition_CheckStart_gOverlaps(l, b, this->pOrgsAftIds, this->pOrgsAftTimestamps, CompareTimestampPairsByStart, q, result);
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
        auto iterIBegin = this->pOrgsInIds[this->numBits][0].begin();
        auto iterIEnd = this->pOrgsInIds[this->numBits][0].end();
        for (auto iterI = iterIBegin; iterI != iterIEnd; iterI++)
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
        auto iterI = this->pOrgsInIds[this->numBits][0].begin();
        auto iterBegin = this->pOrgsInTimestamps[this->numBits][0].begin();
        auto iterEnd = lower_bound(iterBegin, this->pOrgsInTimestamps[this->numBits][0].end(), make_pair<Timestamp, Timestamp>(q.end+1, q.end+1), CompareTimestampPairsByStart);
        for (auto iter = iterBegin; iter != iterEnd; iter++)
        {
            if ((iter->first <= q.end) && (q.start <= iter->second))
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
