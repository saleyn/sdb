// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file  secdb_fmt.hpp
//------------------------------------------------------------------------------
/// \brief SecDB file format reader/writer
///
/// \see https://github.com/saleyn/secdb/wiki/Data-Format
//------------------------------------------------------------------------------
// Copyright (c) 2015 Omnibius, LLC
// Author:  Serge Aleynikov <saleyn@gmail.com>
// Created: 2015-10-15
//------------------------------------------------------------------------------

#include <sys/mman.h>
#include <utxx/scope_exit.hpp>
#include <utxx/time_val.hpp>
#include <utxx/math.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>
#include <stddef.h>
#include <secdb/secdb_fmt.hxx>

using namespace std;

namespace secdb {

/// Convert uuid to string
std::string ToString(uuid const& a) { return boost::uuids::to_string(a); }

//==============================================================================
// Header
//==============================================================================
int Header::Read(FILE* a_file, size_t a_file_size)
{
  assert(a_file);

  int  y, m, d;
  char xchg[32], symb[64], instr[64], uuid[64];

  if (fseek(a_file, 0, SEEK_SET) < 0)
    throw std::runtime_error
      (std::string("Cannot rewind to beginning of file header: ") +
       strerror(errno));

  int n = fscanf(a_file,
    "#!/usr/bin/env secdb\n"
    "version:  %u\n"
    "date:     %d-%d-%d\n"
    "exchange: %s\n"
    "symbol:   %s\n"
    "instr:    %s\n"
    "secid:    %ld\n"
    "depth:    %d\n"
    "px-step:  %lf\n"
    "uuid:     %s\n",
    &m_version, &y, &m, &d, xchg, symb, instr,
    &m_secid,   &m_depth,   &m_px_step, uuid);

  if (n != 11)
    throw std::runtime_error("Invalid SecDB header!");

  m_date         = utxx::time_val::universal_time(y, m, d, 0, 0, 0).sec();
  m_exchange     = xchg;
  m_symbol       = symb;
  m_instrument   = instr;
  m_uuid         = boost::uuids::string_generator()(uuid);

  m_px_scale     = m_px_step  != 0.0 ? (int)(1.0 / m_px_step + 0.5) : 0;
  m_px_precision = m_px_scale ? utxx::math::log(m_px_scale,     10) : 0;

  char buf[128];

  while (true) {
    if (!fgets(buf, sizeof(buf), a_file))
      throw std::runtime_error
        (std::string("Error reading SecDB header: ")+ strerror(errno));
    if (strcmp("", buf) == 0)
      break;
  }

  return ftell(a_file);
}

//------------------------------------------------------------------------------
// Write file header
//------------------------------------------------------------------------------
int Header::Write(FILE* a_file, int a_debug)
{
  assert(a_file);

  int  y;
  uint m, d;
  std::tie(y, m, d) = utxx::from_gregorian_time(m_date);

  int rc = fprintf(a_file,
    "#!/usr/bin/env secdb\n"
    "version:  %u\n"
    "date:     %d-%02d-%02d\n"
    "exchange: %s\n"
    "symbol:   %s\n"
    "instr:    %s\n"
    "secid:    %ld\n"
    "depth:    %d\n"
    "px-step:  %.*lf\n"
    "uuid:     %s\n\n",
    m_version,
    y,m,d,
    m_exchange.c_str(),
    m_symbol.c_str(),
    m_instrument.c_str(),
    m_secid,
    m_depth,
    m_px_precision,
    m_px_step,
    boost::uuids::to_string(m_uuid).c_str());

  if (rc < 0)
    throw std::runtime_error
      (std::string("Cannot write header: ") + strerror(errno));

  return rc;
}

//------------------------------------------------------------------------------
// Print file header
//------------------------------------------------------------------------------
std::ostream& Header::Print(std::ostream& out, const std::string& a_indent)
{
  return out
    << a_indent << "Version....: " << m_symbol       << '\n'
    << a_indent << "Date.......: " << utxx::timestamp::to_string
                                      (time_val(utxx::secs(m_date)), utxx::DATE)
                                   << '\n'
    << a_indent << "Exchange...: " << m_exchange     << '\n'
    << a_indent << "Symbol.....: " << m_symbol       << '\n'
    << a_indent << "Instrument.: " << m_instrument   << '\n'
    << a_indent << "SecID......: " << m_secid        << '\n'
    << a_indent << "Depth......: " << m_depth        << '\n'
    << a_indent << "PxStep.....: " << m_px_step      << '\n'
    << a_indent << "PxPrecision: " << m_px_precision << '\n'
    << a_indent << "PxScale....: " << m_px_scale     << '\n'
    << a_indent << "UUID.......: " << boost::uuids::to_string(m_uuid) << '\n';
}

//------------------------------------------------------------------------------
void Header::Set
(
  int                a_ver,
  std::string const& a_xchg,
  std::string const& a_symbol,
  std::string const& a_instr,
  long               a_secid,
  time_t             a_date,
  uint8_t            a_depth,
  double             a_px_step,
  uuid        const& a_uuid
)
{
  m_version        = a_ver;
  m_exchange       = a_xchg;
  m_symbol         = a_symbol;
  m_instrument     = a_instr;
  m_secid          = a_secid;
  m_date           = a_date;
  m_depth          = a_depth;
  m_px_step        = a_px_step;
  m_px_scale       = m_px_step  != 0.0 ? (int)(1.0 / m_px_step + 0.5) : 0;
  m_px_precision   = m_px_scale ? utxx::math::log(m_px_scale,     10) : 0;
  m_uuid           = a_uuid;
}

//==============================================================================
// StreamsMeta
//==============================================================================

int StreamsMeta::Write(FILE* a_file, int a_debug)
{
  std::vector<char> buf;
  buf.push_back(CODE());                    // StreamsMeta identifier
  buf.push_back((char)m_compression);       // Compression type

  if (fwrite(&buf[0], 1, buf.size(), a_file) != buf.size())
    return -1;

  m_data_offset_pos = ftell(a_file);

  buf.clear();
  buf.push_back(0);                         // Reserve 4 bytes for DataOffset
  buf.push_back(0);
  buf.push_back(0);
  buf.push_back(0);
  buf.push_back((char)m_streams.size());    // StreamCount

  for (auto st : m_streams) {
    buf.push_back(StreamMeta::CODE());
    buf.push_back((char)st.StreamID());     // StreamType's
  }

  auto sz = buf.size();
  return (fwrite(&buf[0], 1, sz, a_file) == sz) ? sz : -1;
}

int StreamsMeta::WriteDataOffset(FILE* a_file, uint a_data_offset)
{
    Bookmark bm(a_file, m_data_offset_pos);
    constexpr size_t sz = sizeof(uint);
    char buf[sz];
    utxx::store_be(buf, a_data_offset);
    return (fwrite(&buf[0], 1, sz, a_file) == sz) ? sz : -1;
}

//==============================================================================
// CandleHeader
//==============================================================================

bool CandleHeader::UpdateCandle(uint a_ts, PriceT a_px, int a_qty)
{
  Candle* c = TimeToCandle(a_ts);
  if (!c)
    return false;
  if (c->Open() ==   0)              c->Open(a_px);
  if (c->High() < a_px)              c->High(a_px);
  if (c->Low()  > a_px || !c->Low()) c->Low (a_px);

  c->Close(a_px);

  if (a_qty > 0) c->AddBVolume(a_qty);
  if (a_qty < 0) c->AddSVolume(-a_qty);

  m_last_updated = c;

  return true;
}

//------------------------------------------------------------------------------
bool CandleHeader::CommitCandles(FILE* a_file)
{
  Bookmark b(a_file);  // keeps current file position intact

  // Go to m_data_offset, which is the offset to the beginning of Candles data
  // in the file:
  if (utxx::unlikely(!m_data_offset || fseek(a_file, m_data_offset, SEEK_SET) < 0))
    return false;

  for (auto& c : m_candles) {
    char buf[80];
    auto p = &buf[0];
    utxx::put32le(p, c.Open());
    utxx::put32le(p, c.High());
    utxx::put32le(p, c.Low());
    utxx::put32le(p, c.Close());
    utxx::put32le(p, c.BVolume());
    utxx::put32le(p, c.SVolume());
    utxx::put64le(p, c.DataOffset());
    size_t sz = p - buf;
    assert(sz == sizeof(Candle));
    if (fwrite(&c, 1, sz, a_file) != sz)
      return false;
  }

  return true;
}

//==============================================================================
// CandlesMeta
//==============================================================================
void CandlesMeta::UpdateDataOffset(uint a_ts, uint64_t a_data_offset)
{
  for (auto& c : m_candle_headers) {
    Candle* last_candle = c.LastUpdated();
    Candle* this_candle = c.TimeToCandle(a_ts);

    if (last_candle == this_candle || !this_candle)
      continue;

    // Start a new candle by update the data offset pointed by it
    this_candle->DataOffset(a_data_offset);
    c.LastUpdated(this_candle);
  }
}

//------------------------------------------------------------------------------
bool CandlesMeta::UpdateCandles(uint a_ts, PriceT a_px, int a_qty)
{
  for (auto& c : m_candle_headers)
    if (utxx::unlikely(!c.UpdateCandle(a_ts, a_px, a_qty)))
      return false;

  return true;
}

//------------------------------------------------------------------------------
bool CandlesMeta::CommitCandles(FILE* a_file)
{
  for (auto& c : m_candle_headers)
    if (utxx::unlikely(!c.CommitCandles(a_file)))
      return false;

  return true;
}

//------------------------------------------------------------------------------
int CandlesMeta::Write(FILE* a_file, int a_debug)
{
  char  buf[80];
  char* p = buf;

  *p++ = CODE();                            // CandlesMeta identifier
  *p++ = 0;                                 // Filler
  utxx::put16le(p, Headers().size());        // Number of candle resolutions

  size_t sz = p-buf;

  auto start_pos = ftell(a_file);

  if (fwrite(buf, 1, sz, a_file) != sz)
    return -1;

  std::vector<int> candle_offsets;          // Memorize candle hdr file positions

  for (auto& hdr : Headers()) {
    p    = buf;
    *p++ = CandleHeader::CODE();            // CandlesHeader identifier
    *p++ = 0;                               // Filler
    utxx::put16le(p, hdr.Resolution());     // Resolution
    utxx::put32le(p, hdr.StartTime());      // Start time in secs since midnight
    utxx::put32le(p, hdr.Candles().size()); // Number of candles
    // Reserve space for the offset to be filled later
    utxx::put32le(p, 0);                    // CandleData file offset

    candle_offsets.push_back(ftell(a_file) + 12);  // Pos of CandleHeader.DataOffset

    assert(p <= buf + sizeof(buf));

    sz = p-buf;
    if (fwrite(buf, 1, sz, a_file) != sz)
      return -1;
  }

  int i = 0;

  // Iterate through each block of candles
  for (auto& hdr : Headers()) {
    // Update the proper CandleHeader.CandleData with the corresponding offset
    // to the beginning of this block of Candles.
    uint pos = (uint)ftell(a_file);

    hdr.CandleDataOffset(pos);

    if (fseek(a_file, candle_offsets[i++], SEEK_SET) < 0)
      return -1;

    char cpos[4]; utxx::store_le(cpos, pos);
    if (fwrite(&cpos, 1, sizeof(cpos), a_file) != sizeof(cpos))
      return -1;

    if (fseek(a_file, pos, SEEK_SET) < 0)
      return -1;

    // Write the block of Candles to file
    for (auto& c : hdr.Candles())
      if (fwrite(&c, 1, sizeof(Candle), a_file) != sizeof(Candle))
        return -1;
  }

  return ftell(a_file) - start_pos;
}

//==============================================================================
// TradeSample
//==============================================================================

int TradeSample::Write(FILE* a_file)
{
  char  buf[128];
  char* p = buf;
  // Encode stream header and time
  StreamBase::Write(p);
  p += utxx::encode_uleb128<0>(m_time, p);  // Encode time since last second
  *p++ = *(uint8_t*)&m_mask;                // Encode FieldMask
  p += utxx::encode_sleb128(m_px, p);       // Encode Price - it's always present
  if (HasQty())
    p += utxx::encode_sleb128(m_qty, p);
  if (HasTradeID())
    p += utxx::encode_uleb128<0>(m_trade_id, p);
  if (HasOrderID())
    p += utxx::encode_uleb128<0>(m_order_id, p);

  size_t sz = p - buf;
  assert(sz <= sizeof(buf));
  return fwrite(buf, 1, sz, a_file) == sz ? sz : -1;
}

//------------------------------------------------------------------------------
std::string TradeSample::ToString(double a_px_step) const
{
  utxx::buffered_print buf;
  buf.print(ToChar(Side()), ' ');
  if (HasQty()) buf.print(Qty(), " @ ");
  buf.print(Price() * a_px_step, " Aggr=", (int)Aggr());

  if (HasTradeID()) buf.print(" TrID=",  TradeID());
  if (HasOrderID()) buf.print(" OrdID=", OrderID());

  return buf.to_string();
}

} // namespace secdb