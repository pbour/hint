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

#ifndef _RELATION_H_
#define _RELATION_H_

#include "../def_global.h"



class Record
{
public:
    RecordId id;
    Timestamp start;
    Timestamp end;
    
    Record();
    Record(RecordId id, Timestamp start, Timestamp end);
    
    bool operator < (const Record &rhs) const;
    bool operator >= (const Record &rhs) const;
    void print(const char c) const;
    ~Record();
};

inline bool CompareRecordsByEnd(const Record &lhs, const Record &rhs)
{
    if (lhs.end == rhs.end)
        return (lhs.id < rhs.id);
    else
        return (lhs.end < rhs.end);
}




class RecordStart
{
public:
    RecordId id;
    Timestamp start;
    
    RecordStart();
    RecordStart(RecordId id, Timestamp start);
    
    bool operator < (const RecordStart &rhs) const;
    bool operator >= (const RecordStart &rhs) const;
    void print(const char c) const;
    ~RecordStart();
};



// copy of RecordStart
class RecordEnd
{
public:
    RecordId id;
    Timestamp end;
    
    RecordEnd();
    RecordEnd(RecordId id, Timestamp end);
    
    bool operator < (const RecordEnd &rhs) const;
    bool operator >= (const RecordEnd &rhs) const;
    void print(const char c) const;
    ~RecordEnd();
};



class TimestampPair
{
public:
    Timestamp start;
    Timestamp end;
    
    TimestampPair();
    TimestampPair(Timestamp start, Timestamp end);
    
    bool operator < (const TimestampPair &rhs) const;
    bool operator >= (const TimestampPair &rhs) const;
    void print(const char c) const;
    ~TimestampPair();
};



// Descending order
inline bool CompareByEnd(const Record &lhs, const Record &rhs)
{
    if (lhs.end == rhs.end)
        return (lhs.id < rhs.id);
    else
        return (lhs.end < rhs.end);
};



class Relation : public vector<Record>
{
public:
    Timestamp gstart;
    Timestamp gend;
    Timestamp longestRecord;
    float avgRecordExtent;
    
    Relation();
    Relation(Relation &R);
    void load(const char *filename);
    void sortByStart();
    void sortByEnd();
    void print(char c);
    ~Relation();
    
    // Querying
    // Basic predicates of Allen's algebra
    size_t execute_Equals(RangeQuery Q);
    size_t execute_Starts(RangeQuery Q);
    size_t execute_Started(RangeQuery Q);
    size_t execute_Finishes(RangeQuery Q);
    size_t execute_Finished(RangeQuery Q);
    size_t execute_Meets(RangeQuery Q);
    size_t execute_Met(RangeQuery Q);
    size_t execute_Overlaps(RangeQuery Q);
    size_t execute_Overlapped(RangeQuery Q);
    size_t execute_Contains(RangeQuery Q);
    size_t execute_Contained(RangeQuery Q);
    size_t execute_Precedes(RangeQuery Q);
    size_t execute_Preceded(RangeQuery Q);

    // Generalized predicate, ACM SIGMOD'22 gOverlaps
    size_t execute_gOverlaps(StabbingQuery Q);
    size_t execute_gOverlaps(RangeQuery Q);
};
typedef Relation::const_iterator RelationIterator;



class RelationStart : public vector<RecordStart>
{
public:
    RelationStart();
    void sortByStart();
    void print(char c);
    ~RelationStart();
};
typedef RelationStart::const_iterator RelationStartIterator;



// Copy of RelationStart
class RelationEnd : public vector<RecordEnd>
{
public:
    RelationEnd();
    void sortByEnd();
    void print(char c);
    ~RelationEnd();
};
typedef RelationEnd::const_iterator RelationEndIterator;



class RelationId : public vector<RecordId>
{
public:
    RelationId();
    void print(char c);
    ~RelationId();
};
typedef RelationId::iterator RelationIdIterator;
#endif //_RELATION_H_
