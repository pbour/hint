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

#ifndef _OFFSETS_H_
#define _OFFSETS_H_

#include "relation.h"



class OffsetEntry_SS_CM
{
public:
    Timestamp tstamp;
    RelationIdIterator iterI;
    vector<pair<Timestamp, Timestamp> >::iterator iterT;
    PartitionId pid;
    
    OffsetEntry_SS_CM();
    OffsetEntry_SS_CM(Timestamp tstamp, RelationIdIterator iterI, vector<pair<Timestamp, Timestamp> >::iterator iterT, PartitionId pid);
    bool operator < (const OffsetEntry_SS_CM &rhs) const;
    bool operator >= (const OffsetEntry_SS_CM &rhs) const;
    ~OffsetEntry_SS_CM();
};

typedef vector<OffsetEntry_SS_CM> Offsets_SS_CM;
typedef Offsets_SS_CM::const_iterator Offsets_SS_CM_Iterator;



class OffsetEntry_ALL
{
public:
    Timestamp tstamp;
    RelationIdIterator iterI;
    PartitionId pid;
    
    OffsetEntry_ALL();
    OffsetEntry_ALL(Timestamp tstamp, RelationIdIterator iterI, PartitionId pid);
    bool operator < (const OffsetEntry_ALL &rhs) const;
    bool operator >= (const OffsetEntry_ALL &rhs) const;
    ~OffsetEntry_ALL();
};


class OffsetEntry_ALL_Timestamp : public OffsetEntry_ALL
{
public:
    vector<Timestamp>::iterator iterT;
    
    OffsetEntry_ALL_Timestamp();
    OffsetEntry_ALL_Timestamp(Timestamp tstamp, RelationIdIterator iterI, vector<Timestamp>::iterator iterT, PartitionId pid);
    bool operator < (const OffsetEntry_ALL_Timestamp &rhs) const;
    bool operator >= (const OffsetEntry_ALL_Timestamp &rhs) const;
    ~OffsetEntry_ALL_Timestamp();
};


class OffsetEntry_ALL_Timestamps : public OffsetEntry_ALL
{
public:
    vector<pair<Timestamp, Timestamp> >::iterator iterT;
    
    OffsetEntry_ALL_Timestamps();
    OffsetEntry_ALL_Timestamps(Timestamp tstamp, RelationIdIterator iterI, vector<pair<Timestamp, Timestamp> >::iterator iterT, PartitionId pid);
    bool operator < (const OffsetEntry_ALL_Timestamps &rhs) const;
    bool operator >= (const OffsetEntry_ALL_Timestamps &rhs) const;
    ~OffsetEntry_ALL_Timestamps();
};

typedef OffsetEntry_ALL_Timestamps OffsetEntry_ALL_OrgsIn;
typedef OffsetEntry_ALL_Timestamp  OffsetEntry_ALL_OrgsAft;
typedef OffsetEntry_ALL_Timestamp  OffsetEntry_ALL_RepsIn;
typedef OffsetEntry_ALL            OffsetEntry_ALL_RepsAft;

typedef vector<OffsetEntry_ALL_OrgsIn>  Offsets_ALL_OrgsIn;
typedef vector<OffsetEntry_ALL_OrgsAft> Offsets_ALL_OrgsAft;
typedef vector<OffsetEntry_ALL_RepsIn>  Offsets_ALL_RepsIn;
typedef vector<OffsetEntry_ALL_RepsAft> Offsets_ALL_RepsAft;

typedef Offsets_ALL_OrgsIn::const_iterator  Offsets_ALL_OrgsIn_Iterator;
typedef Offsets_ALL_OrgsAft::const_iterator Offsets_ALL_OrgsAft_Iterator;
typedef Offsets_ALL_RepsIn::const_iterator  Offsets_ALL_RepsIn_Iterator;
typedef Offsets_ALL_RepsAft::const_iterator Offsets_ALL_RepsAft_Iterator;
#endif //_OFFSETS_H_
