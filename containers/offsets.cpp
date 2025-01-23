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

#include "offsets.h"



OffsetEntry_SS_CM::OffsetEntry_SS_CM()
{
}
    

OffsetEntry_SS_CM::OffsetEntry_SS_CM(Timestamp tstamp, RelationIdIterator iterI, vector<pair<Timestamp, Timestamp> >::iterator iterT, PartitionId pid)
{
    this->tstamp = tstamp;
    this->iterI  = iterI;
    this->iterT  = iterT;
    this->pid    = pid;
}


bool OffsetEntry_SS_CM::operator < (const OffsetEntry_SS_CM &rhs) const
{
    return this->tstamp < rhs.tstamp;
}


bool OffsetEntry_SS_CM::operator >= (const OffsetEntry_SS_CM &rhs) const
{
    return this->tstamp >= rhs.tstamp;
}


OffsetEntry_SS_CM::~OffsetEntry_SS_CM()
{
}



OffsetEntry_ALL::OffsetEntry_ALL()
{
    }


OffsetEntry_ALL::OffsetEntry_ALL(Timestamp tstamp, RelationIdIterator iterI, PartitionId pid)
{
    this->tstamp = tstamp;
    this->iterI  = iterI;
    this->pid    = pid;
}


bool OffsetEntry_ALL::operator < (const OffsetEntry_ALL &rhs) const
{
    return this->tstamp < rhs.tstamp;
}


bool OffsetEntry_ALL::operator >= (const OffsetEntry_ALL &rhs) const
{
    return this->tstamp >= rhs.tstamp;
}


OffsetEntry_ALL::~OffsetEntry_ALL()
{
}



OffsetEntry_ALL_Timestamp::OffsetEntry_ALL_Timestamp()
{
}


OffsetEntry_ALL_Timestamp::OffsetEntry_ALL_Timestamp(Timestamp tstamp, RelationIdIterator iterI, vector<Timestamp>::iterator iterT, PartitionId pid) : OffsetEntry_ALL(tstamp, iterI, pid)
{
    this->iterT = iterT;
}



bool OffsetEntry_ALL_Timestamp::operator < (const OffsetEntry_ALL_Timestamp &rhs) const
{
    return this->tstamp < rhs.tstamp;
}


bool OffsetEntry_ALL_Timestamp::operator >= (const OffsetEntry_ALL_Timestamp &rhs) const
{
    return this->tstamp >= rhs.tstamp;
}


OffsetEntry_ALL_Timestamp::~OffsetEntry_ALL_Timestamp()
{
}


    
OffsetEntry_ALL_Timestamps::OffsetEntry_ALL_Timestamps()
{
}


OffsetEntry_ALL_Timestamps::OffsetEntry_ALL_Timestamps(Timestamp tstamp, RelationIdIterator iterI, vector<pair<Timestamp, Timestamp> >::iterator iterT, PartitionId pid) : OffsetEntry_ALL(tstamp, iterI, pid)
{
    this->iterT = iterT;
}


bool OffsetEntry_ALL_Timestamps::operator < (const OffsetEntry_ALL_Timestamps &rhs) const
{
    return this->tstamp < rhs.tstamp;
}


bool OffsetEntry_ALL_Timestamps::operator >= (const OffsetEntry_ALL_Timestamps &rhs) const
{
    return this->tstamp >= rhs.tstamp;
}


OffsetEntry_ALL_Timestamps::~OffsetEntry_ALL_Timestamps()
{
}
