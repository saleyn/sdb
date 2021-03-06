// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file  sdb_fmt_io.h
//------------------------------------------------------------------------------
/// \brief SDB file format reader/writer
///
/// \see https://github.com/saleyn/sdb/wiki/Data-Format
//------------------------------------------------------------------------------
// Copyright (c) 2015 Omnibius, LLC
// Author:  Serge Aleynikov <saleyn@gmail.com>
// Created: 2015-10-15
//------------------------------------------------------------------------------
#pragma once

#include <cstdint>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/random_generator.hpp>
#include <utxx/time_val.hpp>
#include <utxx/enum.hpp>
#include <utxx/leb128.hpp>
#include <stdio.h>
#include <sdb/sdb_fmt.hpp>

namespace sdb {

//------------------------------------------------------------------------------
/// SDB file read/write I/O operations handler
//------------------------------------------------------------------------------
template <uint MaxDepth = 10>
struct BaseSDBFileIO {

  //----------------------------------------------------------------------------
  // Public API
  //----------------------------------------------------------------------------

  BaseSDBFileIO()  { static_assert(MaxDepth < 128, "MaxDepth is too large"); }

  /// Open \a a_filename for reading
  BaseSDBFileIO(std::string const& a_file, int a_debug = 0);

  ~BaseSDBFileIO() { Close(); }

  static constexpr uint MAX_DEPTH()         { return MaxDepth;              }

  bool                IsOpen()        const { return m_file != nullptr;     }

  Header      const&  Info()          const { return m_header;              }
  time_t              Date()          const { return m_header.Date();       }
  time_val    const&  Midnight()      const { return m_header.Midnight();   }
  std::string const&  Filename()      const { return m_filename;            }

  std::string         TZ()            const { return m_header.TZ();         }
  std::string const&  TZName()        const { return m_header.TZName();     }
  int                 TZOffset()      const { return m_header.TZOffset();   }

  int                 Debug()         const { return m_debug;               }
  void                Debug(int a)          { m_debug = a;                  }

  time_val    const&  Time()          const { return m_last_ts;             }

  /// Minimal price step (e.g. 0.0001)
  double              PxStep()        const { return m_header.PxStep();     }
  /// Price scale (e.g. 10000)
  int                 PxScale()       const { return m_header.PxScale();    }
  /// Price precision in digits after the decimal point (e.g. 4)
  int                 PxPrecision()   const { return m_header.PxPrecision();}

  /// After opening a file this method tells if appending data to existing file
  bool                Existing()      const { return m_existing;            }

  /// Get filename based on given arguments
  static std::string  Filename
  (
    std::string const& a_dir,
    bool               a_deep_dir,
    std::string const& a_xchg,
    std::string const& a_symbol,
    std::string const& a_instr,
    long               a_secid,
    time_val           a_date
  );

  /// Open file for reading or writing
  /// @param a_path      base directory of SDB database
  /// @param a_deep_dir  when true the output file is created inside a nested
  ///                    directory tree as specified by the SDB file naming
  ///                    convention.  Otherwise the \a a_path dir is used.
  /// @param a_xchg      exchange name
  /// @param a_symbol    company-specific security name
  /// @param a_instr     exchange-specific security name
  /// @param a_date      UTC date of the file
  /// @param a_tz_offset Local TZ offset
  /// @param a_perm      file permissions (used when creating a file for writing)
  template <OpenMode Mode>
  void Open
  (
    std::string const& a_path,
    bool               a_deep_dir,
    std::string const& a_xchg,
    std::string const& a_symbol,
    std::string const& a_instr,
    long               a_secid,
    time_val           a_date,
    std::string const& a_tz_name,
    int                a_tz_offset,
    uint8_t            a_depth   = 5,
    double             a_px_step = 0.0001,
    int                a_perm    = 0640,
    uuid        const& a_uuid    = boost::uuids::random_generator()()
  );

  /// Open existing file for reading
  void Open(std::string const& a_filename, int a_debug = 0);
  void Close();

  /// Write file header
  int WriteHeader
  (
    std::string const& a_xchg,
    std::string const& a_symbol,
    std::string const& a_instr,
    long               a_secid,
    time_val           a_date,
    std::string const& a_tz_name,
    int                a_tz_offset,
    uint8_t            a_depth,
    double             a_px_step,
    uuid        const& a_uuid    = boost::uuids::random_generator()()
  );

  void WriteStreamsMeta(std::vector<StreamType>&& a_types);

  void WriteCandlesMeta(CandlesMeta&& a_meta);

  /// Read file header
  int ReadHeader();

  /// Write a snapshot of a market data book.
  /// @param a_ts timestamp of this quote
  /// @param a_bids array of bids sorted in descending order
  /// @param a_bid_cnt total number of items in the \a a_bids array
  /// @param a_asks array of asks sorted in ascending order
  /// @param a_bid_cnt total number of items in the \a a_asks array
  /// @return number of bytes written
  template <PriceUnit PU, typename PxT>
  void WriteQuotes
  (
    time_val a_ts,
    PxLevels<MaxDepth, PxT>&& a_bids, size_t a_bid_cnt,
    PxLevels<MaxDepth, PxT>&& a_asks, size_t a_ask_cnt
  );

  /// Write a trade data
  /// @return number of bytes written
  template <PriceUnit PU, typename PxT>
  void WriteTrade
  (
    time_val  a_ts,
    SideT     a_side,
    PxT       a_px,
    uint      a_qty,
    AggrT     a_aggr     = AggrT::Undefined,
    size_t    a_ord_id   = 0,
    size_t    a_trade_id = 0
  );

  /// Update the candle corresponding to \a a_ts time
  /// @param a_qty bought (a_qty > 0) or sold (a_qty < 0) quantity
  /// @return true on success or false if \a a_ts is outside of range
  void UpdateCandles(int a_ts, PriceT a_px, int a_qty);

  /// Add the buy/sell volume in the candle corresponding to \a a_ts time
  void AddCandleVolumes(int a_ts, int a_buy_qty, int a_sell_qty);

  /// Write string message
  /// @return number of bytes written
  int WriteMsg(time_val a_ts, const char* a_msg, size_t a_sz);

  /// Write market and trading summary information
  /// @return number of bytes written
  int WriteSummary
  (
    time_val  a_ts,
    int       a_bid_qty        = -1,
    int       a_ask_qty        = -1,
    bool      a_has_open_pos   = false,
    int       a_open_pos       = 0,
    bool      a_has_risk       = false,
    double    a_risk           = 0.0
  );

  /// Flush the unwritten data to file stream
  void Flush()                 { if (m_file) ::fflush(m_file); }

  /// Print candles to an output stream
  /// @param out output stream
  /// @param a_resolution output only cangles matching this resolution
  void PrintCandles(std::ostream& out, int a_resolution = -1) const;

  /// Read all samples from file and invoke \a a_fun callback for each record.
  /// @param a_fun functor (auto& record) -> bool.
  template <typename Visitor>
  void Read(Visitor a_fun);

private:
  static constexpr int NaN() { return std::numeric_limits<int>::lowest(); }

  FILE*       m_file          = nullptr;
  OpenMode    m_mode          = OpenMode::Read;
  int         m_debug         = 0;
  bool        m_existing      = false; ///< True when opened existing file
  std::string m_filename;
  Header      m_header;
  time_val    m_last_ts;            ///< Last timestmap written
  int         m_last_sec      = 0;  ///< Last sec  from midnight
  int         m_last_usec     = 0;  ///< Last usec within last written sec
  int         m_next_second   = 0;  ///< Next second since midnight to be written

  int         m_last_quote_px = NaN();
  int         m_last_trade_px = NaN();

  StreamsMeta m_streams_meta;
  CandlesMeta m_candles_meta;

  enum class WriteStateT { Init, WrHeader, WrStreamsMeta, WrCandlesMeta, WrData };

  WriteStateT m_written_state = WriteStateT::Init;

  /// Normalize price given in price steps to price units \a PU
  template <PriceUnit PU, typename T>
  int    NormalizePx(T a_px);

  template <OpenMode Mode>
  size_t DoOpen(std::string const& a_filename, int a_perm = 0640);

  /// @return pair{IsNewSecondSinceMidnight, NowSecSinceMidnight}
  bool   WriteSeconds(time_val a_now);

  double NormalPxToDouble(int a_px) const { return a_px * m_header.PxStep(); }
};

} // namespace sdb