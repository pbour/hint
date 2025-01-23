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

#include "offsets_templates.h"



template <class T>
OffsetEntry_SS<T>::OffsetEntry_SS()
{
}


template <class T>
OffsetEntry_SS<T>::OffsetEntry_SS(Timestamp tstamp, typename T::iterator iter, PartitionId pid)
{
    this->tstamp = tstamp;
    this->iter   = iter;
    this->pid    = pid;
}


template <class T>
bool OffsetEntry_SS<T>::operator < (const OffsetEntry_SS &rhs) const
{
    return this->tstamp < rhs.tstamp;
}


template <class T>
bool OffsetEntry_SS<T>::operator >= (const OffsetEntry_SS &rhs) const
{
    return this->tstamp >= rhs.tstamp;
}


template <class T>
OffsetEntry_SS<T>::~OffsetEntry_SS()
{
}



template <class T>
Offsets_SS<T>::Offsets_SS()
{
}


template <class T>
Offsets_SS<T>::~Offsets_SS()
{
}
