// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file  secdb_fmt_io.hxx
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

#include <secdb/secdb_fmt_io.hpp>

#include <utxx/path.hpp>
#include <utxx/time.hpp>
#include <utxx/print.hpp>
#include <utxx/timestamp.hpp>
#include <utxx/leb128.hpp>
#include <utxx/endian.hpp>
#include <utxx/scope_exit.hpp>
#include <utxx/buffer.hpp>
#include <boost/filesystem.hpp>
#include <sys/stat.h>
#include <fcntl.h>

namespace secdb {

//==============================================================================
// BaseSecDBFileIO
//==============================================================================

//------------------------------------------------------------------------------
// Open SecDB database file
//------------------------------------------------------------------------------
template <uint MaxDepth>
std::string BaseSecDBFileIO<MaxDepth>::
Filename
(
  std::string const& a_dir,
  bool               a_deep_dir,
  std::string const& a_xchg,
  std::string const& a_sym,
  std::string const& a_instr,
  long               a_secid,
  time_t             a_date
)
{
  int      y, m, d;
  std::tie(y, m, d) = utxx::from_gregorian_time(a_date);

  utxx::buffered_print name;
  char c = utxx::path::slash();
  const char suffix[] = ".sdb";

  auto instr = a_instr;
  std::replace(instr.begin(), instr.end(), '/', '-');
  name.print(a_dir, c);

  if (a_deep_dir)
    name.print(a_xchg, c, a_sym, c, y, c,
               utxx::width<2,utxx::RIGHT,int>(m, '0'), c,
               instr, '.', y,
               utxx::width<2,utxx::RIGHT,int>(m, '0'),
               utxx::width<2,utxx::RIGHT,int>(d, '0'));
  else
    name.print(y,
               utxx::width<2,utxx::RIGHT,int>(m, '0'),
               utxx::width<2,utxx::RIGHT,int>(d, '0'),
               '.', a_xchg, '.', a_sym, '.',   instr);

  name.print(suffix);
  return name.to_string();
}

//------------------------------------------------------------------------------
// Open SecDB database file
//------------------------------------------------------------------------------
template <uint MaxDepth>
inline BaseSecDBFileIO<MaxDepth>::
BaseSecDBFileIO(std::string const& a_name, int a_debug)
{
  Open(a_name, a_debug);
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
template <OpenMode Mode>
void BaseSecDBFileIO<MaxDepth>::Open
(
  std::string const& a_path,
  bool               a_deep_dir,
  std::string const& a_xchg,
  std::string const& a_sym,
  std::string const& a_instr,
  long               a_secid,
  time_t             a_date,
  std::string const& a_tz_name,
  int                a_tz_offset,
  uint8_t            a_depth,
  double             a_px_step,
  int                a_perm,
  uuid        const& a_uuid
)
{
  auto name = Filename(a_path, a_deep_dir, a_xchg, a_sym, a_instr, a_secid, a_date);
  auto size = DoOpen<Mode>(name.c_str(), a_perm);

  if (Mode == OpenMode::Write && size < Header::MIN_FILE_SIZE()) {
    try {
      WriteHeader(a_xchg,  a_sym,     a_instr, a_secid,
                  a_date,  a_tz_name, a_tz_offset,
                  a_depth, a_px_step, a_uuid);
    }
    catch (std::exception const& e) {
      UTXX_THROW_RUNTIME_ERROR
        ("Error writing to file ", m_filename, ": ", e.what());
    }
  }
}

//------------------------------------------------------------------------------
// Open SecDB database file
//------------------------------------------------------------------------------
template <uint MaxDepth>
inline void BaseSecDBFileIO<MaxDepth>::
Open(std::string const& a_name, int a_debug)
{
  m_debug  = a_debug;
  int size = DoOpen<OpenMode::Read>(a_name.c_str(), 0640);

  try   { m_header.Read(m_file, size); }
  catch ( std::exception const& e )  {
    UTXX_THROW_RUNTIME_ERROR
      ("Error reading from file ", a_name, ": ", e.what());
  }

  if (a_debug) {
    std::cerr << "File: " << a_name << std::endl;
    m_header.Print(std::cerr);
  }

  if (m_header.Version() != VERSION())
    UTXX_THROW_RUNTIME_ERROR
      ("SecDB version ", m_header.Version(), " not supported (expected: ",
       VERSION(), ')');

  m_streams_meta.Read(m_file);
  m_candles_meta.Read(m_file);

  if (a_debug)
    PrintCandles(std::cerr);
}

//------------------------------------------------------------------------------
// Open SecDB database file
//------------------------------------------------------------------------------
template <uint MaxDepth>
template <OpenMode Mode>
size_t BaseSecDBFileIO<MaxDepth>::
DoOpen(std::string const& a_name, int a_perm)
{
  auto name = a_name; //boost::to_upper_copy(a_name);
  auto dir  = utxx::path::dirname(name);
  try   { boost::filesystem::create_directories(dir); }
  catch ( std::exception const& e ) {
    UTXX_THROW_IO_ERROR(errno, "Cannot create directory ", dir);
  }

  auto mode = Mode == OpenMode::Read ? O_RDONLY : O_RDWR|O_CREAT|O_TRUNC;
  int  fd   = ::open(name.c_str(), mode, a_perm);

  if  (fd < 0)
    UTXX_THROW_IO_ERROR(errno, "Cannot open file ", name.c_str());

  auto on_exit = [=]() { ::close(fd); };
  utxx::scope_exit se(on_exit);

  auto sz = utxx::path::file_size(fd);
  if  (sz < 0)
    UTXX_THROW_IO_ERROR(errno, "Cannot get size of file ", name.c_str());

  if  (Mode == OpenMode::Read && sz < Header::MIN_FILE_SIZE())
    UTXX_THROW_RUNTIME_ERROR
      ("SecDB file ", name.c_str(), " has invalid size ", sz);

  se.disable();

  m_filename = name;
  m_mode     = Mode;
  m_file     = fdopen(fd, Mode == OpenMode::Read ? "r" : "w+");
  return sz;
}

//------------------------------------------------------------------------------
// Close SecDB database file
//------------------------------------------------------------------------------
template <uint MaxDepth>
void BaseSecDBFileIO<MaxDepth>::
Close()
{
  if (!m_file) return;

  if (m_mode == OpenMode::Write)
    m_candles_meta.CommitCandles(m_file);

  if (m_debug > 1)
    PrintCandles(std::cerr);

  ::fclose(m_file);
  m_file = nullptr;

  m_written_state = WriteStateT::Init;

  m_last_ts.clear();
  m_last_sec      = 0;
  m_last_usec     = 0;
  m_last_quote_px = NaN();
  m_last_trade_px = NaN();
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
int BaseSecDBFileIO<MaxDepth>::
ReadHeader()
{
  assert(m_file);
  auto   sz = utxx::path::file_size(fileno(m_file));
  return m_header(m_file, sz);
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
int BaseSecDBFileIO<MaxDepth>::
WriteHeader
(
  std::string const& a_xchg,
  std::string const& a_symbol,
  std::string const& a_instr,
  long               a_secid,
  time_t             a_date,
  std::string const& a_tz_name,
  int                a_tz_offset,
  uint8_t            a_depth,
  double             a_px_step,
  uuid        const& a_uuid
)
{
  if (m_written_state != WriteStateT::Init)
    UTXX_THROW_RUNTIME_ERROR("Header already written to file ", m_filename);

  assert(m_file);
  auto   sz = utxx::path::file_size(fileno(m_file));
  if (sz > 0)
    UTXX_THROW_RUNTIME_ERROR
      ("Cannot write SecDB header to non-empty file ", m_filename);

  m_header.Set(VERSION(), a_xchg,    a_symbol,    a_instr,  a_secid,
               a_date,    a_tz_name, a_tz_offset, a_depth,  a_px_step, a_uuid);

  try { sz = m_header.Write(m_file); }
  catch (std::exception const& e) {
    UTXX_THROW_IO_ERROR(errno, "Error writing header to file ", m_filename);
  }

  m_written_state = WriteStateT::WrHeader;

  return sz;
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
void BaseSecDBFileIO<MaxDepth>::
WriteStreamsMeta(std::vector<StreamType>&& a_types)
{
  if (m_written_state != WriteStateT::WrHeader)
    UTXX_THROW_RUNTIME_ERROR
      ("Streams metadata already written to file ", m_filename);

  auto v = std::vector<StreamsMeta::StreamMeta>();

  for (auto st: a_types) v.emplace_back(st);
  if (m_debug) {
    auto n = ftell(m_file);
    std::cerr << "  StreamsMeta position: " << n << " (" << std::hex << n << ")\n";
  }

  m_streams_meta = StreamsMeta(std::move(v));

  if (m_streams_meta.Write(m_file) < 0)
    UTXX_THROW_IO_ERROR
      (errno, "Error writing streams metadata to file ", m_filename);

  // At this point m_streams_meta contains the DataOffset() field that needs
  // to be updated after the candles are written

  m_written_state = WriteStateT::WrStreamsMeta;
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
void BaseSecDBFileIO<MaxDepth>::
WriteCandlesMeta(CandlesMeta&& a_meta)
{
  if (m_written_state != WriteStateT::WrStreamsMeta)
    UTXX_THROW_RUNTIME_ERROR
      ("Candles metadata already written to file ", m_filename);

  if (a_meta.Write(m_file, m_debug) < 0)
    UTXX_THROW_IO_ERROR(errno, "Error writing candle data to file ", m_filename);

  m_candles_meta  = std::move(a_meta);
  m_written_state = WriteStateT::WrCandlesMeta;

  // Now that we know the beginning of data offset, write it to the
  // StreamsMeta header:
  if (m_streams_meta.WriteDataOffset(m_file, ftell(m_file)) < 0)
    UTXX_THROW_IO_ERROR
      (errno, "Error writing beginning of data offset to file ", m_filename);

  char  buf[4];
  char* p = buf;
  utxx::put32le(p, BEGIN_STREAM_DATA());

  if (m_debug)
    std::cerr << " Begin Stream Marker: " << std::hex << ftell(m_file) << std::endl;

  if (fwrite(buf, 1, sizeof(buf), m_file) != sizeof(buf))
    UTXX_THROW_IO_ERROR
      (errno, "Error writing beginning data marker to file ", m_filename);

  m_written_state = WriteStateT::WrData;
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
bool BaseSecDBFileIO<MaxDepth>::
WriteSeconds(time_val a_now)
{
  auto midnight_ns = a_now - Midnight();
  int  usec        = midnight_ns.usec(); // Microsecs
  m_last_sec       = midnight_ns.sec();  // Seconds since midnight
  m_last_ts        = a_now;
  m_last_usec      = usec;

  // Check if the second changed and we need to write the second stream data
  if (m_next_second == 0 || m_last_sec >= m_next_second) {
    // Possibly update candles with current data offset at this second
    m_candles_meta.UpdateDataOffset(m_last_sec, ftell(m_file));
    // Write the new SecondsSample to file
    if (!SecondsSample(m_last_sec).Write(m_file))
      UTXX_THROW_IO_ERROR(errno, "Error writing seconds to file ",
                          m_filename, " at ", ftell(m_file));
    m_next_second = m_last_sec+1;
    m_last_quote_px = NaN();
    m_last_trade_px = NaN();

    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
template <PriceUnit PU, typename T>
int BaseSecDBFileIO<MaxDepth>::
NormalizePx(T a_px)
{
  return PU == PriceUnit::DoubleVal
      ? (int)(double(a_px) / m_header.PxStep()  + 0.5)
      : PU == PriceUnit::PrecisionVal
      ? (int)(int(a_px)    / m_header.PxScale() + 0.5)
      : PU == PriceUnit::PriceSteps
      ? a_px
      : throw std::logic_error
              ("Undefined price unit " UTXX_FILE_SRC_LOCATION);
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
template <PriceUnit PU, typename PxT>
void BaseSecDBFileIO<MaxDepth>::
WriteQuotes
(
  time_val a_ts,
  PxLevels<MaxDepth, PxT>&& a_bids, size_t a_bid_cnt,
  PxLevels<MaxDepth, PxT>&& a_asks, size_t a_ask_cnt
)
{
  if (utxx::unlikely(a_bid_cnt > MaxDepth || a_ask_cnt > MaxDepth))
    UTXX_THROW_RUNTIME_ERROR("Invalid bid/ask counts: ",a_bid_cnt,' ',a_ask_cnt);

  // Start with the first
  PxT  first_px;
  int  qty;
  PxLevel<PxT>* pb, *pb_end;
  PxLevel<PxT>* pa, *pa_end;

  if (utxx::likely(a_bid_cnt > 0)) {
    auto&  b = a_bids[a_bid_cnt-1];
    first_px = NormalizePx<PU>(b.m_px);
    qty      = b.m_qty;
    pb       = &b;
    // pb pointer points to the next item after the first bid:
    pb_end   = pb-- - a_bid_cnt;
    pa       = &a_asks[0];
    pa_end   = pa + a_ask_cnt;
  } else if (utxx::likely(a_ask_cnt > 0)) {
    auto&  a = a_asks[a_ask_cnt-1];
    first_px = NormalizePx<PU>(a.m_px);
    qty      = a.m_qty;
    // Since there are no bids, set pb to point to the end of the bids range,
    // and the first price is based on a_asks[0], so pa pointer should be set
    // to next item after the first ask:
    pb       = &a_bids[0];
    pb_end   = --pb;
    pa       = &a;
    pa_end   = pa++ + a_ask_cnt;
  } else
    return;

  if (utxx::unlikely(a_ts < m_last_ts))
    UTXX_THROW_RUNTIME_ERROR
      ("Attempt to write an out-of-order timestamp=",
       utxx::timestamp::to_string(a_ts, utxx::DATE_TIME_WITH_USEC),
       ", last=",
       utxx::timestamp::to_string(m_last_ts, utxx::DATE_TIME_WITH_USEC),
       " to file ", m_filename);

  // If the seconds advanced, write the new second since midnight (StreamID=0)
  int  prev_usec  = m_last_usec;
  bool sec_chng   = WriteSeconds(a_ts);

  auto ts         = sec_chng ? m_last_usec : (m_last_usec - prev_usec);

  // StreamBase - when sec_chng is true, this is a Full quote; otherwise: Delta
  bool delta = m_last_quote_px != NaN();

  using QuoteSampleT = QuoteSample<MaxDepth*2, PriceT>;

  auto  book = typename QuoteSampleT::PxLevelsT();
  auto  q    = &book[0];
  q->m_px    = delta ? first_px - m_last_quote_px : first_px;
  q->m_qty   = qty;

  m_last_quote_px = first_px;
  auto    prev_px = first_px;

  // Remaining Bids (a_bids are in descending order, so we go in reverse dir)
  for (++q; pb != pb_end; --pb, ++q) {
    // Calculate differential price, and update it in the PxLevels container
    auto px  = NormalizePx<PU>(pb->m_px);
    q->m_px  = px - prev_px;
    q->m_qty = pb->m_qty;
    prev_px  = px;
  }

  // Asks (a_asks are sorted in ascending order, so we go in forward direction)
  for (; pa != pa_end; ++pa, ++q) {
    // Calculate differential price, and update it in the PxLevels container
    auto px  = NormalizePx<PU>(pa->m_px);
    q->m_px  = px - prev_px;
    q->m_qty = pa->m_qty; // Asks are negative
    prev_px  = px;
  }

  QuoteSampleT qt(delta, ts, std::move(book), a_bid_cnt, a_ask_cnt);

  if (qt.Write(m_file) < 0)
    UTXX_THROW_IO_ERROR
      (errno, "Error writing a quote to file ", m_filename);
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
template <PriceUnit PU, typename PxT>
void BaseSecDBFileIO<MaxDepth>::
WriteTrade
(
  time_val  a_ts,
  SideT     a_side,
  PxT       a_px,
  uint      a_qty,
  AggrT     a_aggr,
  size_t    a_ord_id,
  size_t    a_trade_id
)
{
  if (utxx::unlikely(a_ts < m_last_ts))
    UTXX_THROW_RUNTIME_ERROR
      ("Attempt to write an out-of-order timestamp ",
       utxx::timestamp::to_string(a_ts, utxx::TIME_WITH_USEC),
       "to file ", m_filename);

  // If the seconds advanced, write the new second since midnight (StreamID=0)
  int  prev_usec  = m_last_usec;
  bool sec_chng   = WriteSeconds(a_ts);
  auto ts         = sec_chng ? m_last_usec : (m_last_usec - prev_usec);

  // When seconds changed, this is a Full quote; otherwise: Delta
  bool delta      = m_last_trade_px != NaN();
  auto px         = NormalizePx<PU>(a_px);
  auto px_inc     = delta ? (px - m_last_trade_px) : px;

  m_last_trade_px = px;

  TradeSample tr(delta, ts, a_side, px_inc, a_qty, a_aggr, a_ord_id, a_trade_id);

  int sz = tr.Write(m_file);

  // Update candles using this trade
  int qty = a_side == SideT::Buy ? int(a_qty) : -int(a_qty);
  m_candles_meta.UpdateCandles(m_last_sec, px, qty);

  if (sz < 0)
    UTXX_THROW_IO_ERROR
      (errno, "Error writing a trade ", tr.ToString(), " to file ", m_filename);
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
void BaseSecDBFileIO<MaxDepth>::
UpdateCandles(int a_ts, PriceT a_px, int a_qty)
{
  m_candles_meta.UpdateCandles(a_ts, a_px, a_qty);
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
void BaseSecDBFileIO<MaxDepth>::
AddCandleVolumes(int a_ts, int a_buy_qty, int a_sell_qty)
{
  m_candles_meta.AddCandleVolumes(a_ts, a_buy_qty, a_sell_qty);
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
void BaseSecDBFileIO<MaxDepth>::
PrintCandles(std::ostream& out, int a_resolution) const
{
  if (m_debug)
    out << "  Candle Resolutions: " << m_candles_meta.Headers().size()
        << std::endl;

  bool found = a_resolution == -1;
  uint idx   = 0;
  for (auto& ch : m_candles_meta.Headers()) {
    // If requested specific candle resolution - skip the rest
    if (a_resolution != -1 && a_resolution != ch.Resolution())
      continue;

    auto n = ch.Candles().size();
    int  s = ch.StartTime() + TZOffset();
    int  e = s + ch.Resolution()*n;
    char buf[80];
    sprintf(buf, "# Resolution: %ds %02d:%02d - %02d:%02d %s (UTC: %ld)\n",
            ch.Resolution(),
            s / 3600, s % 3600 / 60,
            e / 3600, e % 3600 / 60,
            TZ().c_str(), Date() + ch.StartTime());
    out << buf <<
      "#Time    Open   High   Low    Close     BuyVol   SellVol DataOffset\n";

    for (auto& c : ch.Candles()) {
      int  ts = ch.CandleToTime(idx++) + TZOffset();
      uint h  = ts / 3600, m = ts % 3600 / 60, s = ts % 60;
      //if (c.DataOffset() == 0)
      //  continue;
      sprintf(buf, "%02d:%02d:%02d ", h,m,s);
      out << buf
          << std::setprecision(Header().PxPrecision())     << std::fixed
          << NormalPxToDouble(c.Open())    << ' '
          << NormalPxToDouble(c.High())    << ' '
          << NormalPxToDouble(c.Low())     << ' '
          << NormalPxToDouble(c.Close())   << ' '
          << std::setw(9)   << c.BVolume() << ' '
          << std::setw(9)   << c.SVolume();
      if (m_debug)
        out << " [" << c.DataOffset() << "]\n";
      else
        out << std::endl;
    }

    found = true;
  }

  if (!found)
    UTXX_THROW_RUNTIME_ERROR
      ("Requested candle resolution ", a_resolution, "not found in ", m_filename);
}

//------------------------------------------------------------------------------
template <uint MaxDepth>
template <typename OnSample>
void BaseSecDBFileIO<MaxDepth>::
Read(OnSample a_fun)
{
  if (fseek(m_file, m_streams_meta.DataOffset(), SEEK_SET) < 0)
    UTXX_THROW_IO_ERROR
      (errno, "Can't find file data offset ", m_streams_meta.DataOffset(), ": ",
       m_filename);

  // Read the beginning of data marker
  {
    char buf[4];
    if (fread(buf, 1, sizeof(buf), m_file) != sizeof(buf))
      UTXX_THROW_IO_ERROR
        (errno, "Can't read beginning of data marker ", m_filename);

    if (uint(utxx::cast32le(buf)) != BEGIN_STREAM_DATA())
      UTXX_THROW_RUNTIME_ERROR
        ("Invalid beginning of data marker: ", m_filename);
  }

  utxx::dynamic_io_buffer buf(4096);

  m_last_quote_px = NaN();
  m_last_trade_px = NaN();

  while (true) {
    long n = fread(buf.wr_ptr(), 1, buf.capacity(), m_file);

    if  (n == 0)
      break;

    buf.commit(n);

    while (n > 0) {
      if (buf.size() < 2)
        break;
      auto x         = *(uint8_t*)buf.rd_ptr();
      auto base      = (StreamBase*)&x;
      bool is_delta  = base->Delta();
      auto stream_tp = base->Type();

      switch (stream_tp) {
        case StreamType::Seconds: {
          SecondsSample ss;
          n = ss.Read(buf.rd_ptr(), buf.size());
          if (n > 0) {
            time_t secs     = m_header.Midnight().sec() + ss.Time();
            m_last_ts.set(secs);
            m_last_sec      = secs;
            m_last_usec     = 0;
            m_next_second   = m_last_sec + 1;
            m_last_quote_px = NaN();
            m_last_trade_px = NaN();
          }
          break;
        }
        case StreamType::Quotes: {
          QuoteSample<MaxDepth, PriceT> qs;
          n = qs.Read(buf.rd_ptr(), buf.size(), is_delta, m_last_quote_px);
          if (n <= 0)
            break;
          m_last_usec += qs.Time();
          m_last_ts.usec(m_last_usec);
          a_fun(qs);
          break;
        }
        case StreamType::Trade: {
          TradeSample ts;
          n = ts.Read(buf.rd_ptr(), buf.size(), is_delta, m_last_trade_px);
          if (n <= 0)
            break;
          m_last_usec += ts.Time();
          m_last_ts.usec(m_last_usec);
          a_fun(ts);
          break;
        }
        case StreamType::Order:
        case StreamType::Summary:
        case StreamType::Message:
          UTXX_THROW_RUNTIME_ERROR("Not supported: ", int(stream_tp), " stream");
        default:
          UTXX_THROW_RUNTIME_ERROR("Invalid stream type: ", int(stream_tp));
      }

      if (n == 0) break;
      if (n <  0)
        UTXX_THROW_IO_ERROR(errno, "Error reading from file ", m_filename);

      buf.read(n);
    }

    buf.crunch();
  }
}

//------------------------------------------------------------------------------
} // namespace secdb
