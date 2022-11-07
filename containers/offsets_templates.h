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

#ifndef _OFFSETS_TEMPLATES_H_
#define _OFFSETS_TEMPLATES_H_

#include "relation.h"



template <class T>
class OffsetEntry_SS
{
public:
    Timestamp tstamp;
    typename T::iterator iter;
    PartitionId pid;
    
    OffsetEntry_SS();
    OffsetEntry_SS(Timestamp tstamp, typename T::iterator iter, PartitionId pid);
    bool operator < (const OffsetEntry_SS<T> &rhs) const;
    bool operator >= (const OffsetEntry_SS<T> &rhs) const;
    ~OffsetEntry_SS();
};

// For HINT
typedef OffsetEntry_SS<RelationId>    OffsetEntry_SS_HINT;

// For HINT^m
typedef OffsetEntry_SS<Relation>      OffsetEntry_SS_OrgsIn;
typedef OffsetEntry_SS<RelationStart> OffsetEntry_SS_OrgsAft;
typedef OffsetEntry_SS<RelationEnd>   OffsetEntry_SS_RepsIn;
typedef OffsetEntry_SS<RelationId>    OffsetEntry_SS_RepsAft;



template <class T>
class Offsets_SS : public vector<OffsetEntry_SS<T> >
{
public:
    Offsets_SS();
    ~Offsets_SS();
};

// For HINT
typedef Offsets_SS<RelationId> Offsets_SS_HINT;
typedef Offsets_SS_HINT::const_iterator Offsets_SS_HINT_Iterator;

// For HINT^m
typedef Offsets_SS<Relation>      Offsets_SS_OrgsIn;
typedef Offsets_SS<RelationStart> Offsets_SS_OrgsAft;
typedef Offsets_SS<RelationEnd>   Offsets_SS_RepsIn;
typedef Offsets_SS<RelationId>    Offsets_SS_RepsAft;
typedef Offsets_SS_OrgsIn::const_iterator  Offsets_SS_OrgsIn_Iterator;
typedef Offsets_SS_OrgsAft::const_iterator Offsets_SS_OrgsAft_Iterator;
typedef Offsets_SS_RepsIn::const_iterator  Offsets_SS_RepsIn_Iterator;
typedef Offsets_SS_RepsAft::const_iterator Offsets_SS_RepsAft_Iterator;
#endif //_OFFSETS_TEMPLATES_H_
