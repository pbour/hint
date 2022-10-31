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

#include "relation.h"



Record::Record()
{
}


Record::Record(RecordId id, Timestamp start, Timestamp end)
{
	this->id = id;
	this->start = start;
	this->end = end;
}


bool Record::operator < (const Record& rhs) const
{
    if (this->start == rhs.start)
        return this->end < rhs.end;
    else
        return this->start < rhs.start;
}

bool Record::operator >= (const Record& rhs) const
{
    return !((*this) < rhs);
}


void Record::print(const char c) const
{
	cout << c << this->id << ": [" << this->start << ".." << this->end << "]" << endl;
}

 
Record::~Record()
{
}



RecordStart::RecordStart()
{
}


RecordStart::RecordStart(RecordId id, Timestamp start)
{
    this->id = id;
    this->start = start;
}


bool RecordStart::operator < (const RecordStart& rhs) const
{
    if (this->start == rhs.start)
        return this->id < rhs.id;
    else
        return this->start < rhs.start;
}

bool RecordStart::operator >= (const RecordStart& rhs) const
{
    return !((*this) < rhs);
}


void RecordStart::print(const char c) const
{
    cout << c << this->id << ": [" << this->start << "..*]" << endl;
}


RecordStart::~RecordStart()
{
}



RecordEnd::RecordEnd()
{
}


RecordEnd::RecordEnd(RecordId id, Timestamp end)
{
    this->id = id;
    this->end = end;
}


bool RecordEnd::operator < (const RecordEnd& rhs) const
{
    if (this->end == rhs.end)
        return this->id < rhs.id;
    else
//        return this->end < rhs.end;
        return this->end < rhs.end;
}

bool RecordEnd::operator >= (const RecordEnd& rhs) const
{
    return !((*this) < rhs);
}


void RecordEnd::print(const char c) const
{
    cout << c << this->id << ": [*.." << this->end << "]" << endl;
}


RecordEnd::~RecordEnd()
{
}



TimestampPair::TimestampPair()
{
    
}


TimestampPair::TimestampPair(Timestamp start, Timestamp end)
{
    this->start = start;
    this->end   = end;
}


bool TimestampPair::operator < (const TimestampPair &rhs) const
{
    return this->start < rhs.start;
}


bool TimestampPair::operator >= (const TimestampPair &rhs) const
{
    return this->start >= rhs.start;
}


void TimestampPair::print(const char c) const
{
    cout << c << this->start << ": [*.." << this->end << "]" << endl;
}


TimestampPair::~TimestampPair()
{
}



Relation::Relation()
{
    this->gstart          = std::numeric_limits<Timestamp>::max();
    this->gend            = std::numeric_limits<Timestamp>::min();
	this->longestRecord   = std::numeric_limits<Timestamp>::min();
    this->avgRecordExtent = 0;
}


Relation::Relation(Relation &R) : vector<Record>(R)
{
}


void Relation::load(const char *filename)
{
	Timestamp rstart, rend;
	ifstream inp(filename);
    size_t sum = 0;
    RecordId numRecords = 0;

	
	if (!inp)
	{
		cerr << endl << "Error - cannot open data file \"" << filename << "\"" << endl << endl;
		exit(1);
	}

	while (inp >> rstart >> rend)
	{
        if (rstart > rend)
        {
            cerr << endl << "Error - start is after end for interval [" << rstart << ".." << rend << "]" << endl << endl;
            exit(1);
        }
        
        this->emplace_back(numRecords, rstart, rend);
        numRecords++;

        this->gstart = std::min(this->gstart, rstart);
        this->gend   = std::max(this->gend  , rend);
		this->longestRecord = std::max(this->longestRecord, rend-rstart+1);
        sum += rend-rstart;
	}
	inp.close();
	
    this->avgRecordExtent = (float)sum/this->size();
}


void Relation::sortByStart()
{
    sort(this->begin(), this->end());
}


void Relation::sortByEnd()
{
    sort(this->begin(), this->end(), CompareByEnd);
}


void Relation::print(char c)
{
	for (const Record& rec : (*this))
		rec.print(c);
}


Relation::~Relation()
{
}


// Querying
size_t Relation::execute_Equals(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if (iter->start == Q.start && iter->end == Q.end)
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


size_t Relation::execute_Starts(RangeQuery Q) //Q.start == interval.start, Q.end < interval.end
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if ((iter->start == Q.start) && (iter->end > Q.end))
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


size_t Relation::execute_Started(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if ((iter->start == Q.start) && (iter->end < Q.end))
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


size_t Relation::execute_Finishes(RangeQuery Q) // same end, Q.start > interval.start
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if ((iter->end == Q.end) && (iter->start < Q.start))
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


size_t Relation::execute_Finished(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if ((iter->end == Q.end) && (iter->start > Q.start))
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


size_t Relation::execute_Meets(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if (iter->start == Q.end)
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


size_t Relation::execute_Met(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if (iter->end == Q.start)
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


size_t Relation::execute_Overlaps(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if ((Q.start < iter->start) && (iter->start < Q.end) && (Q.end < iter->end))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result^= iter->id;
#endif
        }
    }
    
    return result;
}

size_t Relation::execute_Overlapped(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if ((Q.start > iter->start) && (Q.start < iter->end) && (Q.end > iter->end))
        {
#ifdef WORKLOAD_COUNT
            result++;
#else
            result^= iter->id;
#endif
        }
    }
    
    return result;
}


size_t Relation::execute_Contains(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if ((iter->start > Q.start) && (iter->end < Q.end))
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


size_t Relation::execute_Contained(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if ((iter->start < Q.start) && (iter->end > Q.end))
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


size_t Relation::execute_Precedes(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
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
    
    return result;
}


size_t Relation::execute_Preceded(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
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
    
    return result;
}


size_t Relation::execute_gOverlaps(StabbingQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
    {
        if ((iter->start <= Q.point) && (Q.point <= iter->end))
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


size_t Relation::execute_gOverlaps(RangeQuery Q)
{
    size_t result = 0;
    
    RelationIterator iterEnd = this->end();
    for (RelationIterator iter = this->begin(); iter != iterEnd; iter++)
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



RelationStart::RelationStart()
{
}


void RelationStart::sortByStart()
{
    sort(this->begin(), this->end());
}


void RelationStart::print(char c)
{
    for (const RecordStart& rec : (*this))
        rec.print(c);
}


RelationStart::~RelationStart()
{
}



RelationEnd::RelationEnd()
{
}


void RelationEnd::sortByEnd()
{
    sort(this->begin(), this->end());
}


void RelationEnd::print(char c)
{
    for (const RecordEnd& rec : (*this))
        rec.print(c);
}


RelationEnd::~RelationEnd()
{
}


RelationId::RelationId()
{
}


void RelationId::print(char c)
{
    for (const RecordId& rec : (*this))
        cout << c << rec << endl;
}


RelationId::~RelationId()
{
}
