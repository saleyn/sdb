// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file  secdb_fmt.hxx
//------------------------------------------------------------------------------
/// \brief SecDB file format reader/writer
///
/// \see https://github.com/saleyn/secdb/wiki/Data-Format
//------------------------------------------------------------------------------
// Copyright (c) 2015 Omnibius, LLC
// Author:  Serge Aleynikov <saleyn@gmail.com>
// Created: 2015-10-15
//------------------------------------------------------------------------------
#pragma once

#include <secdb/secdb_fmt.hpp>

#include <utxx/time.hpp>
#include <utxx/print.hpp>
#include <utxx/timestamp.hpp>
#include <utxx/leb128.hpp>
#include <utxx/endian.hpp>
#include <utxx/scope_exit.hpp>

namespace secdb {

//==============================================================================
// CandleHeader
//==============================================================================
inline Candle* CandleHeader::
TimeToCandle(int a_ts)
{
  assert(a_ts  < 86400);
  int  n       = (a_ts - m_start_time)/m_resolution;
  bool invalid = n < 0 || n >= int(m_candles.size());
  return utxx::unlikely(invalid) ? nullptr : &m_candles[n];
}

//------------------------------------------------------------------------------
inline int CandleHeader::
CandleToTime(uint a_idx) const
{
  assert(a_idx < m_candles.size());
  return m_start_time + m_resolution*a_idx;
}

//==============================================================================
// QuoteSample
//==============================================================================
template <uint MaxDepth, typename PxT>
int QuoteSample<MaxDepth, PxT>::
Write(FILE* a_file)
{
  if (utxx::unlikely(m_count == 0))
    return 0;

  assert(m_count <= MaxDepth);

  char  buf[1024];
  char* p = buf;
  StreamBase::Write(p);                       // Write stream header
  p   += utxx::encode_uleb128<0>(m_time, p);  // Encode time since last second
  *p++ = m_count;                             // Count of PxLevel's

  // Encode PxLevels
  for (auto it = m_levels.begin(), e = m_levels.end(); it != e; ++it) {
    // Encode the Px,Qty
    p += utxx::encode_sleb128(it->m_px,  p);
    p += utxx::encode_sleb128(it->m_qty, p);
  }

  size_t sz = p - buf;
  assert(sz <= sizeof(buf));

  return (fwrite(buf, 1, sz, a_file) == sz) ? sz : -1;
}

} // namespace secdb