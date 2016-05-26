// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file  sdb_fmt.hpp
//------------------------------------------------------------------------------
/// \brief SDB file format reader/writer
///
/// \see https://github.com/saleyn/sdb/wiki/Data-Format
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
#include <sdb/sdb_fmt.hxx>

using namespace std;

namespace sdb {

/// Convert uuid to string
std::string ToString(uuid const& a) { return boost::uuids::to_string(a); }

//==============================================================================
// Header
//==============================================================================
int Header::Read(FILE* a_file, size_t a_file_size)
{
  assert(a_file);

  int  y, m, d, tz_hh, tz_mm;
  char xchg[32], symb[64], instr[64], uuid[64], tz[8], tznm[64];

  if (fseek(a_file, 0, SEEK_SET) < 0)
    throw std::runtime_error
      (std::string("Cannot rewind to beginning of file header: ") +
       strerror(errno));

  // NB: For some reason fscanf doesn't parse the string that ends with ")\n"
  // correctly, so we exclude the ')' in the time zone name ('utc-date')
  // and remove it by hand later:
  int n = fscanf(a_file,
    "#!/usr/bin/env sdb\n"
    "version:  %u\n"
    "utc-date: %d-%d-%d (%s %s\n"
    "exchange: %s\n"
    "symbol:   %s\n"
    "instr:    %s\n"
    "secid:    %ld\n"
    "depth:    %d\n"
    "px-step:  %lf\n"
    "uuid:     %s",
    &m_version, &y, &m, &d, tz, tznm, xchg, symb, instr,
    &m_secid,   &m_depth,   &m_px_step, uuid);

  if (n != 13)
    throw std::runtime_error("Invalid SDB header!");

  n = ftell(a_file);

  m_date       = utxx::time_val::universal_time(y, m, d, 0, 0, 0);
  m_exchange   = xchg;
  m_symbol     = symb;
  m_instrument = instr;
  m_uuid       = boost::uuids::string_generator()(uuid);

  int tzlen    = strlen(tznm);
  if (strlen(tz) != 5 || tzlen < 3 || tznm[tzlen-1] != ')')
    throw std::runtime_error
      (std::string("SDB header - invalid timezone format: ") + tz);

  utxx::fast_atoi(tz+1, tz+3, tz_hh);
  utxx::fast_atoi(tz+3, tz+5, tz_mm);

  m_px_scale     = m_px_step  != 0.0 ? (int)(1.0 / m_px_step + 0.5) : 0;
  m_px_precision = m_px_scale ? utxx::math::log(m_px_scale,     10) : 0;
  auto offset    = (tz[0] == '-' ? -1 : 1) * (tz_hh*3600 + tz_mm*60);
  SetTZOffset(offset);
  m_tz_name      = std::string(tznm, tzlen-1); // exclude closing ')'
  bool eol       = false;

  while (true) {
    if ((n = fgetc(a_file)) < 0)
      throw std::runtime_error
        (std::string("Error reading SDB header: ")+ strerror(errno));
    if (n != '\n')
      eol = false;
    else if (eol)
      break;
    else
      eol = true;
  }

  n = ftell(a_file);
  return n;
}

//------------------------------------------------------------------------------
// Write file header
//------------------------------------------------------------------------------
int Header::Write(FILE* a_file, int a_debug)
{
  assert(a_file);

  int  y;
  uint m, d;
  time_t sec = m_date.sec();
  std::tie(y, m, d) = utxx::from_gregorian_time(sec);

  int rc = fprintf(a_file,
    "#!/usr/bin/env sdb\n"
    "version:  %u\n"
    "utc-date: %d-%02d-%02d (%s)\n"
    "exchange: %s\n"
    "symbol:   %s\n"
    "instr:    %s\n"
    "secid:    %ld\n"
    "depth:    %d\n"
    "px-step:  %.*lf\n"
    "uuid:     %s\n\n",
    m_version,
    y,m,d, TZ().c_str(),
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
std::ostream& Header::Print(std::ostream& out, const std::string& a_ident) const
{
  char buf[32];
  utxx::timestamp::write_date(buf, m_date.sec(), true, 10, '-');
  buf[10] = ' ';
  utxx::timestamp::write_time(buf+11, m_date, utxx::TIME, true, ':');
  buf[19] = '\0';

  return out
    << a_ident << "Version....: " << m_version       << '\n'
    << a_ident << "Date.......: " << buf << " UTC (" << TZ() << TZName() << ")\n"
    << a_ident << "Exchange...: " << m_exchange      << '\n'
    << a_ident << "Symbol.....: " << m_symbol        << '\n'
    << a_ident << "Instrument.: " << m_instrument    << '\n'
    << a_ident << "SecID......: " << m_secid         << '\n'
    << a_ident << "Depth......: " << m_depth         << '\n'
    << a_ident << "PxStep.....: " << std::fixed
                                  << std::setprecision(m_px_precision)
                                  << m_px_step       << '\n'
    << a_ident << "PxPrecision: " << m_px_precision  << '\n'
    << a_ident << "PxScale....: " << m_px_scale      << '\n'
    << a_ident << "UUID.......: " << boost::uuids::to_string(m_uuid) << '\n';
}

//------------------------------------------------------------------------------
void Header::SetTZOffset(int a_tz_offset)
{
  m_tz_offset = a_tz_offset;

  char c = m_tz_offset < 0 ? '-' : '+';
  int  n = abs(m_tz_offset);
  int  h = n / 3600;
  int  m = n % 3600 / 60;
  char buf[16];
  sprintf(buf, "%c%02d%02d %s", c, h, m, TZName().c_str());
  m_tz_hhmm = buf;
}

//------------------------------------------------------------------------------
void Header::Set
(
  int                a_ver,
  std::string const& a_xchg,
  std::string const& a_symbol,
  std::string const& a_instr,
  long               a_secid,
  time_val           a_date,
  std::string const& a_tz_name,
  int                a_tz_offset,
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
  m_date           = a_date - utxx::secs(a_date.sec() % 86400);
  m_depth          = a_depth;
  m_px_step        = a_px_step;
  m_px_scale       = m_px_step  != 0.0 ? (int)(1.0 / m_px_step + 0.5) : 0;
  m_px_precision   = m_px_scale ? utxx::math::log(m_px_scale,     10) : 0;
  m_tz_name        = a_tz_name;
  m_uuid           = a_uuid;
  SetTZOffset(a_tz_offset);
}

//==============================================================================
// SecondsSample
//==============================================================================
int SecondsSample::Write(FILE* a_file)
{
  char  buf[16];
  char* p = buf;
  StreamBase::Write(p);
  sleb128_encode(m_time, p);
  size_t sz = p - buf;
  return fwrite(buf, 1, sz, a_file) == sz ? sz : -1;
}

//------------------------------------------------------------------------------
int SecondsSample::Read(const char* a_buf, size_t a_sz)
{
#ifndef NDEBUG
  static constexpr uint8_t s_stream_id = uint8_t(StreamType::Seconds);
#endif

  auto begin = a_buf;
  auto end   = a_buf + a_sz;

  // Must be this stream
  assert((*a_buf & 0x7F) == s_stream_id);

  a_buf++;
  long ts = sleb128_decode(a_buf);

  if (a_buf >= end)
    return 0;     // Not enough data

  new (this) SecondsSample(ts);

  return a_buf - begin;
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

//------------------------------------------------------------------------------
int StreamsMeta::WriteDataOffset(FILE* a_file, uint a_data_offset)
{
    Bookmark bm(a_file, m_data_offset_pos);
    constexpr size_t sz = sizeof(uint);
    char buf[sz];
    utxx::store_le(buf, a_data_offset);
    return (fwrite(&buf[0], 1, sz, a_file) == sz) ? sz : -1;
}

//------------------------------------------------------------------------------
void StreamsMeta::Read(FILE* a_file)
{
  auto pos = ftell(a_file);
  if  (pos < 0) throw utxx::io_error(errno, "cannot determine curr file offset");
  m_data_offset_pos = pos + 2;

  std::vector<char> buf(256);
  const char*  p = &buf[0];
  int n = fread((void*)p, 1, 7, a_file);
  if (n < 7)
    throw utxx::io_error(errno, "cannot read StreamsMeta");
  if (*p++ != CODE())
    throw utxx::runtime_error("invalid StreamsMeta code (", (uint)buf[0],
                              ", expected: ", (uint)CODE(), ')');
  m_compression = CompressT(*p++);
  m_data_offset = utxx::get32le(p);
  int     count = *(uint8_t*)p++;  // Stream count

  p = &buf[0];
  n = fread((void*)p, 1, count*2, a_file);

  if (n != count*2 || n > 128)
    throw utxx::io_error
      (errno, "cannot read StreamMeta (n=", n, ", count=", count, ')');

  m_streams.clear();

  for (int i=0; i < count; i++) {
    if (*p++ != StreamMeta::CODE())
      throw utxx::runtime_error("invalid StreamsMeta::Header");
    if (*p >= int(StreamType::INVALID))
      throw utxx::runtime_error("invalid StreamType ", int(*p));

    m_streams.emplace_back(StreamType(*p++));
  }
}

//==============================================================================
// CandleHeader
//==============================================================================
int CandleHeader::CalcSize(int a_start_time, int a_end_time, uint16_t a_res)
{
  int diff = a_end_time - a_start_time;
  assert (diff > 0);
  int n = diff % a_res;
  if (n > 0) diff += n;
  return  diff / a_res;
}

//------------------------------------------------------------------------------
bool CandleHeader::UpdateCandle(int a_ts, PriceT a_px, int a_qty)
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
bool CandleHeader::AddCandleVolume(int a_ts, int a_buy_qty, int a_sell_qty)
{
  Candle* c = TimeToCandle(a_ts);
  if (!c)
    return false;

  c->AddBVolume(a_buy_qty);
  c->AddSVolume(a_sell_qty);

  m_last_updated = c;

  return true;
}

//------------------------------------------------------------------------------
bool CandleHeader::CommitCandles(FILE* a_file)
{
  // keeps current file position intact and jump to m_data_offset
  Bookmark b(a_file, m_data_offset);
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
    size_t sz =   p - buf;
    assert(sz ==  sizeof(Candle));
    if (fwrite(&c, 1, sz, a_file) != sz)
      return false;
  }

  return true;
}

//==============================================================================
// CandlesMeta
//==============================================================================
void CandlesMeta::UpdateDataOffset(int a_ts, uint64_t a_data_offset)
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
void CandlesMeta::UpdateCandles(int a_ts, PriceT a_px, int a_qty)
{
  for (auto& c : m_candle_headers)
    c.UpdateCandle(a_ts, a_px, a_qty);
}

//------------------------------------------------------------------------------
void CandlesMeta::AddCandleVolumes(int a_ts, int a_buy_qty, int a_sell_qty)
{
  for (auto& c : m_candle_headers)
    c.AddCandleVolume(a_ts, a_buy_qty, a_sell_qty);
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
  utxx::put16le(p, Headers().size());       // Number of candle resolutions

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

//------------------------------------------------------------------------------
void sdb::CandlesMeta::Read(FILE* a_file)
{
  std::vector<char> buf(256);
  const char* p  = &buf[0];

  auto n = fread((void*)p, 1, 4, a_file);
  if  (n < 4)
    throw utxx::io_error(errno, "cannot read CandlesMeta");
  if (*p++ != CODE())
    throw utxx::runtime_error("invalid StreamsMeta code (", (uint)*(p-1),
                              ", expected: ", (uint)CODE(), ')');
  if (*p++ != 0)
    throw utxx::runtime_error("invalid StreamsMeta filler (", (uint)*(p-1), ')');

  size_t count = utxx::get16le(p);

  if (buf.size() < 16*count)
    buf.resize(16*count);

  p = &buf[0];

  if (fread((void*)p, 1, count*16, a_file) != count*16)
    throw utxx::io_error(errno, "invalid file format (missing CandleHeaders)");

  m_candle_headers.clear();

  std::vector<int> candle_counts;

  for (uint i = 0; i < count; ++i) {
    if (*p++ != CandleHeader::CODE())
      throw utxx::runtime_error("invalid CandleMeta code (", (uint)*(p-1),
                                ", expected: ", (uint)CandleHeader::CODE(), ')');
    if (*p++ != 0)
      throw utxx::runtime_error("invalid StreamsMeta filler (", (uint)*(p-1), ')');

    uint16_t resolution  =      utxx::get16le(p);
    int      start_time  = (int)utxx::get32le(p);
    uint32_t candle_cnt  =      utxx::get32le(p);
    uint32_t data_offset =      utxx::get32le(p);

    m_candle_headers.emplace_back
      (resolution, start_time, start_time + candle_cnt*resolution, data_offset);

    candle_counts.push_back(candle_cnt);
  }

  for (uint i = 0; i < count; ++i) {
    auto&  ch = m_candle_headers[i];
    uint    n = candle_counts[i];

    buf.resize(n*sizeof(Candle));

    p = &buf[0];

    if (fread((void*)p, 1, n*sizeof(Candle), a_file) != n*sizeof(Candle))
      throw utxx::io_error
        (errno, "invalid file format (cannot read candles of resolution=",
         ch.Resolution(), ")");

    for (uint j=0; j < n; ++j) {
      auto op = utxx::get32le(p);
      auto hi = utxx::get32le(p);
      auto lo = utxx::get32le(p);
      auto cl = utxx::get32le(p);
      auto bv = utxx::get32le(p);
      auto sv = utxx::get32le(p);
      auto of = utxx::get64le(p);

      ch.Candles()[j] = Candle(op, hi, lo, cl, bv, sv, of);
    }
  }

  p = &buf[0];
  auto marker = uint(utxx::get32le(p));

  if (fread((void*)p, 1, 4, a_file) != 4 || marker == BEGIN_STREAM_DATA())
    throw utxx::io_error
      (errno, "invalid file data marker at ", utxx::itoa_hex(marker));
}

//==============================================================================
// TradeSample
//==============================================================================
void TradeSample::
Set(FieldMask a_mask, PriceT a_px, int a_qty, size_t a_tid, size_t a_oid)
{
  m_mask      = a_mask;
  m_px        = a_px;
  m_qty       = a_qty;
  m_trade_id  = a_tid;
  m_order_id  = a_oid;
}

//------------------------------------------------------------------------------
int TradeSample::Write(FILE* a_file)
{
  char  buf[128];
  char* p = buf;
  // Encode stream header and time
  StreamBase::Write(p);
  uleb128_encode(m_time, p);          // Encode time since last second
  *p++ = *(uint8_t*)&m_mask;          // Encode FieldMask
  sleb128_encode(m_px, p);            // Encode Price - it's always present
  if (HasQty())
    sleb128_encode(m_qty, p);
  if (HasTradeID())
    uleb128_encode(m_trade_id, p);
  if (HasOrderID())
    uleb128_encode(m_order_id, p);

  size_t sz  = p - buf;
  assert(sz <= sizeof(buf));
  return fwrite(buf, 1, sz, a_file) == sz ? sz : -1;
}

//------------------------------------------------------------------------------
int TradeSample::Read(const char* a_buf, size_t a_sz,
                      bool a_is_delta, PriceT& a_last_px)
{
#ifndef NDEBUG
  static constexpr uint8_t s_stream_id = uint8_t(StreamType::Trade);
#endif

  auto begin = a_buf;
  auto end   = a_buf + a_sz;

  // Must be this stream
  assert((*a_buf & 0x7F) == s_stream_id);

  a_buf++;
  auto ts = uleb128_decode(a_buf);

  if (a_buf > end)
    return 0;     // Not enough data

  FieldMask mask(*a_buf++);

  PriceT px = sleb128_decode(a_buf);

  // If this is a delta trade, the price value is the diff between last known
  // price and current price, so:
  if (a_is_delta)
    px += a_last_px;

  if (utxx::unlikely(a_buf >= end))
    return 0;

  int qty = 0;

  if (mask.has_qty)
    qty = sleb128_decode(a_buf);

  if (utxx::unlikely(a_buf >= end))
    return 0;

  ulong tid = 0, oid = 0;

  if (mask.has_trade_id)
    tid = uleb128_decode(a_buf);

  if (utxx::unlikely(a_buf >= end))
    return 0;

  if (mask.has_order_id)
    oid = uleb128_decode(a_buf);

  if (utxx::unlikely(a_buf >= end))
    return 0;

  new (this) TradeSample(true, mask, ts, px, qty, tid, oid);

  a_last_px = px;

  return a_buf - begin;
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

} // namespace sdb
