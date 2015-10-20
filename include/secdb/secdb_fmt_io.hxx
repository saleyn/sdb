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

  name.print(a_dir, c);

  if (a_deep_dir)
    name.print(a_xchg, c, a_sym, c, y, c,
               utxx::width<2,utxx::RIGHT,int>(m, '0'), c,
               a_instr, '.', y,
               utxx::width<2,utxx::RIGHT,int>(m, '0'),
               utxx::width<2,utxx::RIGHT,int>(d, '0'));
  else
    name.print(y,
               utxx::width<2,utxx::RIGHT,int>(m, '0'),
               utxx::width<2,utxx::RIGHT,int>(d, '0'),
               '.', a_xchg, '.', a_sym, '.', a_instr);

  name.print(suffix);
  return name.to_string();
}

//------------------------------------------------------------------------------
// Open SecDB database file
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
      WriteHeader(a_xchg,a_sym,a_instr,a_secid,a_date,a_depth,a_px_step,a_uuid);
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
Open(std::string const& a_name)
{
  int size = DoOpen<OpenMode::Read>(a_name.c_str(), 0640);

  try   { m_header.Read(m_file, size); }
  catch ( std::exception const& e )  {
    UTXX_THROW_RUNTIME_ERROR
      ("Error reading from file ", a_name, ": ", e.what());
  }
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

  auto mode = Mode == OpenMode::Read ? O_RDONLY : O_RDWR|O_CREAT;
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

  ::fclose(m_file);
  m_file = nullptr;

  m_written_state = WriteStateT::Init;

  m_last_ts.clear();
  m_last_sec      = 0;
  m_last_usec     = 0;
  m_last_quote_px = 0;
  m_last_trade_px = 0;
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

  m_header.Set(VERSION(), a_xchg, a_symbol, a_instr,
               a_secid,   a_date, a_depth,  a_px_step, a_uuid);

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
  using SecondsSample = SecondsSample;

  auto midnight_ns = a_now - Date();
  int  usec        = midnight_ns.usec(); // Microsecs
  m_last_sec       = midnight_ns.sec();  // Seconds since midnight
  m_last_ts        = a_now;
  m_last_usec      = usec;

  // Check if the second changed and we need to write the second stream data
  if (m_next_second == 0 || m_last_sec >= m_next_second) {
    // Possibly update candles with current data offset at this second
    m_candles_meta.UpdateDataOffset(m_last_sec, ftell(m_file));
    // Write the new SecondsSample
    if (!SecondsSample(m_last_sec).Write<false>(m_file))
      UTXX_THROW_IO_ERROR(errno, "Error writing seconds to file ",
                          m_filename, " at ", ftell(m_file));
    m_next_second = m_last_sec+1;

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
WriteQuotes(time_val a_ts, PxLevels<MaxDepth*2, PxT>&& a_book, size_t a_count)
{
  if (utxx::unlikely(a_count == 0))
    return;

  assert(a_count <= MaxDepth*2);

  if (utxx::unlikely(a_ts < m_last_ts))
    UTXX_THROW_RUNTIME_ERROR
      ("Attempt to write an out-of-order timestamp ",
       utxx::timestamp::to_string(a_ts, utxx::TIME_WITH_USEC),
       "to file ", m_filename);

  // If the seconds advanced, write the new second since midnight (StreamID=0)
  int  prev_usec  = m_last_usec;
  bool sec_chng   = WriteSeconds(a_ts);

  // Start with the first
  auto first_px   = NormalizePx<PU>(a_book[0].m_px);
  auto prev_px    = sec_chng ? first_px    : (first_px - m_last_quote_px);
  auto ts         = sec_chng ? m_last_usec : (m_last_usec - prev_usec);
  m_last_quote_px = first_px;

  // StreamBase - when sec_chng is true, this is a Full quote; otherwise: Delta
  bool delta = !sec_chng;

  using QuoteSampleT = QuoteSample<MaxDepth*2, PriceT>;

  auto book = typename QuoteSampleT::PxLevelsT();
  auto q    = &book[0];
  q->m_px   = prev_px;
  q->m_qty  = a_book[0].m_qty;
  q++;

  // Remaining PxLevels
  for (auto p = &a_book[1], e = p + a_count; p != e; ++p, ++q) {
    // Calculate differential price, and update it in the PxLevels container
    auto px  = NormalizePx<PU>(p->m_px);
    q->m_px  = px - prev_px;
    q->m_qty = p->m_qty;
    prev_px  = px;
  }

  QuoteSampleT qt(delta, ts, std::move(book), a_count);

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
  int       a_qty,
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

  auto px         = NormalizePx<PU>(a_px);
  auto px_inc     = sec_chng ? px          : (px -    m_last_trade_px);
  auto ts         = sec_chng ? m_last_usec : (m_last_usec - prev_usec);
  m_last_trade_px = px;

  // StreamBase - when sec_chng is true, this is a Full quote; otherwise: Delta
  bool delta = !sec_chng;

  TradeSample tr(delta, ts, a_side, px_inc, a_qty, a_aggr, a_ord_id, a_trade_id);

  int sz = tr.Write(m_file);

  m_candles_meta.UpdateCandles(m_last_sec, px, a_qty);

  if (sz < 0)
    UTXX_THROW_IO_ERROR
      (errno, "Error writing a trade ", tr.ToString(), " to file ", m_filename);
}

} // namespace secdb