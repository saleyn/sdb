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
  if (utxx::unlikely((m_bid_cnt || m_ask_cnt) == 0))
    return 0;

  char  buf[1024];
  char* p   = buf;
  char* end = p + sizeof(buf);
  StreamBase::Write(p);                       // Write stream header
  p   += utxx::encode_uleb128<0>(m_time, p);  // Encode time since last second
  *p++ = uint8_t(m_ask_cnt << 4 | m_bid_cnt); // Count of PxLevel's bids & asks

  // Encode PxLevels
  for (auto it = m_levels.begin(), e = it + BidCount() + AskCount(); it != e; ++it)
    it->Encode(p, end); // Encode the Px,Qty

  assert(p <= end);

  size_t sz = p - buf;
  return (fwrite(buf, 1, sz, a_file) == sz) ? sz : -1;
}

//------------------------------------------------------------------------------
template <uint MaxDepth, typename PxT>
int QuoteSample<MaxDepth, PxT>::
Read(const char* a_buf, size_t a_sz, bool a_is_delta, PxT& a_last_px)
{
  static constexpr uint8_t s_stream_id = uint8_t(StreamType::Quotes);

  auto begin = a_buf;
  auto end   = a_buf + a_sz;

  // Must be this stream
  assert((*a_buf & 0x7F) == s_stream_id);

  a_buf++;
  m_time = uleb128_decode(a_buf);

  if (a_buf >= end)
    return 0;     // Not enough data

  m_bid_cnt = *a_buf & 0x0F;
  m_ask_cnt = (*a_buf++ >> 4) & 0x0F;

  if (m_bid_cnt > int(MaxDepth) || m_ask_cnt > int(MaxDepth))
    throw utxx::runtime_error("Too many price levels: ",m_bid_cnt,' ',m_ask_cnt);

  auto p = &m_levels[0];
  auto e = p + m_bid_cnt + m_ask_cnt;

  // Decode the Px/Qty of the first price level
  p->Decode(a_buf, end);

  // If this is a delta quote, the price value is the diff between last known
  // price and current price, so:
  auto first_px = a_is_delta ? (p->m_px += a_last_px) : p->m_px;
  auto last_px  = first_px;

  for (++p; p != e; ++p) {
    p->Decode(a_buf, end);
    p->m_px += last_px;
    last_px  = p->m_px;
    if (a_buf > end)
      return 0; // Need more data
  }

  a_last_px = first_px;

  return a_buf - begin;
}

} // namespace secdb