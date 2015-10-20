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
#pragma once

#include <cstdint>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/random_generator.hpp>
#include <utxx/time_val.hpp>
#include <utxx/leb128.hpp>
#include <stdio.h>

namespace secdb {

using utxx::time_val;
using uuid = boost::uuids::uuid;

enum class SideT : char { Buy, Sell };

inline const char ToChar(SideT a) {
  static const char s[] = {'B','S'}; return s[int(a)];
}

enum class AggrT : char { Undefined, Aggressor, Passive };

inline const char* ToString(AggrT a) {
  static const char* s[] = {"Undef", "Aggr", "Pass"};
  return s[int(a)];
}

/// File opening mode
enum OpenMode  { Read, Write };

enum PriceUnit {
    DoubleVal     // Price expressed in floating decimal point (e.g. px=0.01)
  , PrecisionVal  // Adjusted value with precision (e.g. precision=2, px=100)
  , PriceSteps    // Value expressed as int price steps (e.g. step=0.01, px=1)
};

//------------------------------------------------------------------------------
enum class StreamType : char {
    Seconds   // Mandatory stream
  , Quotes
  , Trade
  , Order
  , Summary
  , Message
};

using PriceT = int;

inline uuid UUID(std::string const& a) { return boost::uuids::string_generator()(a); }
/// Convert UUID to string
std::string ToString(uuid const& a);

//------------------------------------------------------------------------------
// SecDB data format (version 1) reader/writer
//------------------------------------------------------------------------------
static constexpr uint VERSION() { return 1; }

//------------------------------------------------------------------------------
// Bookmark class that saves/restores current file position
//------------------------------------------------------------------------------
struct Bookmark {
  Bookmark(FILE* a_file, int64_t a_new_pos = -1) : m_file(a_file) {
    fgetpos(a_file, &m_pos);
    if (a_new_pos != -1 && fseek(a_file, a_new_pos, SEEK_SET) < 0)
      throw std::runtime_error
        ("Cannot set file position to " + std::to_string(a_new_pos));
  }
  ~Bookmark() { fsetpos(m_file, &m_pos); }
private:
  FILE*  m_file;
  fpos_t m_pos;
};

//------------------------------------------------------------------------------
// File header
//------------------------------------------------------------------------------
struct Header  {
  /// Minimum expected header length
  static constexpr uint MIN_FILE_SIZE() { return 165; }

  Header() {}

  uint32_t    Version()     const { return m_version;       }
  time_t      Date()        const { return m_date;          }
  int         Depth()       const { return m_depth;         }

  /// Minimal price step (e.g. 0.0001)
  double      PxStep()      const { return m_px_step;       }
  /// Price scale (e.g. 10000)
  int         PxScale()     const { return m_px_scale;      }
  /// Price precision in digits after the decimal point (e.g. 4)
  int         PxPrecision() const { return m_px_precision;  }
  std::string Exchange()    const { return m_exchange;      }
  std::string Symbol()      const { return m_symbol;        }
  std::string Instrument()  const { return m_instrument;    }
  long        SecID()       const { return m_secid;         }
  uuid        UUID()        const { return m_uuid;          }

  /// Set values of file header
  void Set
  (
    int                a_ver,
    std::string const& a_xchg,
    std::string const& a_symbol,
    std::string const& a_instr,
    long               a_secid,
    time_t             a_date,
    uint8_t            a_depth,
    double             a_px_step,
    uuid        const& a_uuid = boost::uuids::random_generator()()
  );

  /// Read header from a file descriptor
  int Read(FILE* a_file, size_t a_file_size);

  /// Write header to a file descriptor
  int Write(FILE* a_file, int a_debug = 0);

  std::ostream& Print(std::ostream& out, const std::string& a_indent = "");

private:
  uint32_t    m_version       = 0;
  std::string m_exchange;
  std::string m_symbol;
  std::string m_instrument;
  long        m_secid         = 0;
  time_t      m_date          = 0;
  int         m_depth         = 10;
  double      m_px_step       = 0.01;
  int         m_px_scale      = 100;
  int         m_px_precision  = 2;
  uuid        m_uuid          = boost::uuids::nil_generator()();
};

//------------------------------------------------------------------------------
/// Metadata about streams contained in the file
//------------------------------------------------------------------------------
struct StreamsMeta {
  static constexpr uint8_t CODE() { return 0x1; }

  enum class CompressT {
    None,   // No compression
    GZip    // Gzip compression
  };

  struct StreamMeta {
    static constexpr uint8_t CODE() { return 0x2; }

    StreamMeta() {}
    StreamMeta(StreamType a_tp) : m_header(CODE()), m_stream_id(a_tp) {}

    StreamType  StreamID() const { return m_stream_id; }
  private:
    char        m_header;
    StreamType  m_stream_id;
  };

  using StreamsVec = std::vector<StreamMeta>;

  StreamsMeta() {}
  StreamsMeta
  (
    std::initializer_list<StreamType> a_streams,
    uint32_t                          a_data_offset = 0,
    CompressT                         a_cmp         = CompressT::None
  )
    : m_compression(a_cmp)
    , m_data_offset(a_data_offset)
  {
    for (auto st : a_streams)
      m_streams.emplace_back(st);
  }

  StreamsMeta
  (
    StreamsVec&&                      a_streams,
    uint32_t                          a_data_offset = 0,
    CompressT                         a_cmp         = CompressT::None
  )
    : m_compression(a_cmp)
    , m_data_offset(a_data_offset)
    , m_streams    (std::move(a_streams))
  {}

  /// Position in file of Beginning of Data Marker (BEGIN_STREAM_DATA)
  uint          DataOffset()      const { return m_data_offset;     }

  /// Position in file of the DataOffset field
  uint          DataOffsetPos()   const { return m_data_offset_pos; }

  int           Count()           const { return m_streams.size();  }

  /// Write StreamsMeta to file
  int Write(FILE* a_file, int a_debug = 0);
  /// Update beginning of data offset in the StreamsMeta header
  int WriteDataOffset(FILE* a_file, uint a_data_offset);

private:
  CompressT     m_compression     = CompressT::None;
  uint          m_data_offset_pos = 0;
  uint          m_data_offset     = 0;
  StreamsVec    m_streams;
};


//------------------------------------------------------------------------------
/// Candle
//------------------------------------------------------------------------------
struct Candle {

  Candle() { memset(this, 0, sizeof(Candle)); }

  Candle
  (
    int  a_open, int a_high, int a_low, int a_close,
    uint a_bvol, uint a_svol, uint64_t a_data_offset = 0
  )
    : m_open       (a_open)
    , m_high       (a_high)
    , m_low        (a_low)
    , m_close      (a_close)
    , m_buy_vol    (a_bvol)
    , m_sell_vol   (a_svol)
    , m_data_offset(a_data_offset)
  {}

  int      Open()       const { return m_open;                 }
  int      High()       const { return m_high;                 }
  int      Low()        const { return m_low;                  }
  int      Close()      const { return m_close;                }
  uint32_t BVolume()    const { return m_buy_vol;              }
  uint32_t SVolume()    const { return m_sell_vol;             }
  uint32_t Volume()     const { return m_buy_vol + m_sell_vol; }
  uint64_t DataOffset() const { return m_data_offset;          }

  void     Open      (int a)          { m_open        = a;     }
  void     High      (int a)          { m_high        = a;     }
  void     Low       (int a)          { m_low         = a;     }
  void     Close     (int a)          { m_close       = a;     }
  void     AddBVolume(int a)          { m_buy_vol    += a;     }
  void     AddSVolume(int a)          { m_sell_vol   += a;     }
  void     DataOffset(uint64_t a_pos) { m_data_offset = a_pos; }

private:
  int      m_open;
  int      m_high;
  int      m_low;
  int      m_close;

  uint32_t m_buy_vol;
  uint32_t m_sell_vol;
  uint64_t m_data_offset;
};

//------------------------------------------------------------------------------
/// Candle Block Metadata
//------------------------------------------------------------------------------
struct CandleHeader {
  static constexpr uint8_t CODE() { return 0x4; }

  using CandlesVec  = std::vector<Candle>;

  /// @param a_resolution  candle period in seconds
  /// @param a_start_time  start time in seconds from midnight UTC
  /// @param a_end_time    end   time in seconds from midnight UTC
  /// @param a_data_offset absolute offset to data in file
  CandleHeader
  (
    uint16_t a_resolution,
    uint     a_start_time,
    uint     a_end_time,
    uint32_t a_data_offset = 0
  )
    : m_resolution (a_resolution)
    , m_start_time (a_start_time)
    , m_data_offset(a_data_offset)
    , m_candles    ((a_end_time - a_start_time) / a_resolution)
  {
    assert(a_end_time > a_start_time);
  }

  /// Update m_data_offset if \a a_ts corresponds to the beginning of candle
  void UpdateDataOffset(uint a_ts, uint64_t a_data_offset);

  /// Update the candle corresponding to \a a_ts time
  /// @return true on success or false if \a a_ts is outside of range
  bool UpdateCandle(uint a_ts, PriceT a_px, int a_qty);

  /// Update the file with the current candles data
  /// @return true on success or false if there was a problem writing data
  bool CommitCandles(FILE* a_file);

  uint16_t          Resolution()              const { return m_resolution;  }
  uint              StartTime()               const { return m_start_time;  }
  size_t            CandleDataOffset()        const { return m_data_offset; }
  void              CandleDataOffset(size_t a_pos)  { m_data_offset = a_pos;}

  CandlesVec&       Candles()                       { return m_candles;     }
  CandlesVec const& Candles()                 const { return m_candles;     }

  /// The Candle last updated by the UpdateCandle() call
  Candle*           LastUpdated()             const { return m_last_updated;}
  void              LastUpdated(Candle* a)          { m_last_updated = a;   }
  Candle*           TimeToCandle(uint a_ts);

private:
  uint16_t   m_resolution;
  uint       m_start_time;
  size_t     m_data_offset;
  Candle*    m_last_updated = nullptr; // Last updated candle
  CandlesVec m_candles;
};

//------------------------------------------------------------------------------
/// Candle Block Metadata
//------------------------------------------------------------------------------
struct CandlesMeta {
  static constexpr uint8_t CODE() { return 0x3; }

  using CandleHeaderVec = std::vector<CandleHeader>;

  CandlesMeta() {}

  CandlesMeta(std::initializer_list<CandleHeader> a_hdrs)
    : m_candle_headers(a_hdrs)
  {}

  CandleHeaderVec&        Headers()       { return m_candle_headers; }
  CandleHeaderVec const&  Headers() const { return m_candle_headers; }

  int Write(FILE* a_file, int a_debug = 0);

  /// @param a_ts second since midnight
  /// @param a_data_offset position of file corrsponding to \a a_ts second
  void UpdateDataOffset(uint a_ts, uint64_t a_data_offset);

  /// Update the candles corresponding to \a a_ts time in each candle resolution
  /// @param a_ts time in seconds since midnight
  /// @return true on success or false if \a a_ts is outside of range
  bool UpdateCandles(uint a_ts, PriceT a_px, int a_qty);

  /// Update the file with the current candles data for all candle resolutions
  /// @return true on success or false if there was a problem writing data
  bool CommitCandles(FILE* a_file);
private:
  CandleHeaderVec m_candle_headers;
};

//------------------------------------------------------------------------------
/// Base type of each stream's header
//------------------------------------------------------------------------------
struct StreamBase {
  StreamBase() {}
  StreamBase(bool a_dlt, StreamType a_tp)
    : m_delta(a_dlt), m_type(char(a_tp))
  {
    static_assert(sizeof(StreamBase) == sizeof(uint8_t), "Invalid size");
  }

  bool       Delta() const { return m_delta;            }
  StreamType Type()  const { return StreamType(m_type); }

  void       Write(char*& a) { *a++ = *(uint8_t*)this;  }
private:
  bool       m_delta: 1;
  char       m_type : 7;
};

//------------------------------------------------------------------------------
/// Representation of seconds from midnight
//------------------------------------------------------------------------------
struct SecondsSample : public StreamBase {
  SecondsSample() {}
  SecondsSample(int a_now)
    : StreamBase(0, StreamType::Seconds), m_time(a_now)
  {
    assert(a_now < ((1 << 24) - 1));
  }

  int Time() const { return m_time; }

  template <bool TimeAsLEB>
  int Write(FILE* a_file) {
    char  buf[4];
    char* p = buf;
    StreamBase::Write(p);
    if (TimeAsLEB) {
      utxx::encode_sleb128(m_time, p);
    } else {
      long tm  = m_time;
      *p++ =  tm        & 0xFF;
      *p++ = (tm >>  8) & 0xFF;
      *p++ = (tm >> 16) & 0xFF;
    }
    return fwrite(buf, 1, sizeof(buf), a_file) == sizeof(buf) ? sizeof(buf):-1;
  }
private:
  uint m_time : 24;
};

//------------------------------------------------------------------------------
/// Representation of a price level
//------------------------------------------------------------------------------
template <typename PxT>
struct PxLevel {
  PxT m_px;
  int m_qty;
};

template <uint MaxDepth, typename PxT>
using PxLevels = std::array<PxLevel<PxT>, MaxDepth>;

//------------------------------------------------------------------------------
/// Representation of Quote sample
//------------------------------------------------------------------------------
template <uint MaxDepth, typename PxT>
struct QuoteSample : public StreamBase {
  using PxLevelsT = PxLevels<MaxDepth, PxT>;

  QuoteSample() {}
  QuoteSample(bool a_delta, uint a_ts, PxLevelsT&& a_lev, size_t a_count)
    : StreamBase(a_delta, StreamType::Quotes)
    , m_time    (a_ts)
    , m_levels  (std::move(a_lev))
    , m_count   (a_count)
  {}

  int    Time()  const { return m_time;  }
  size_t Count() const { return m_count; }

  PxLevelsT const& Levels() const { return m_levels; }

  int    Write(FILE* a_file);

private:
  uint        m_time;
  PxLevelsT&& m_levels;
  size_t      m_count;
};

//------------------------------------------------------------------------------
/// Representation of Trade sample
//------------------------------------------------------------------------------
struct TradeSample : public StreamBase {
  struct FieldMask {
    bool internal     : 1;
    char aggr         : 2;
    char side         : 1;
    bool has_qty      : 1;
    bool has_trade_id : 1;
    bool has_order_id : 1;
    bool _unused      : 1;

    FieldMask() { *(uint8_t*)this = 0; }
    FieldMask(bool a_internal, AggrT a_aggr,    SideT a_sd,
              bool a_has_qty,  bool  a_has_oid, bool  a_has_trid)
      : internal    (a_internal)
      , aggr        ((int)a_aggr)
      , side        (bool(a_sd == SideT::Sell))
      , has_qty     (a_has_qty)
      , has_trade_id(a_has_trid)
      , has_order_id(a_has_oid)
    {
      static_assert(sizeof(FieldMask) == sizeof(uint8_t), "Invalid size");
    }
  };

  TradeSample() {}
  TradeSample(bool   a_delta, uint a_ts, SideT  a_sd, PriceT a_px, int a_qty,
              AggrT  a_aggr = AggrT::Undefined,
              size_t a_ord_id   = 0,    size_t a_trade_id = 0,
              bool   a_internal = false)
    : StreamBase(a_delta, StreamType::Trade)
    , m_mask    (a_internal, a_aggr, a_sd, a_qty != 0, a_ord_id != 0,
                  a_trade_id != 0)
    , m_time    (a_ts)
    , m_trade_id(a_trade_id)
    , m_order_id(a_ord_id)
    , m_px      (a_px)
    , m_qty     (a_qty)
  {
    assert(a_ts < ((1 << 24) - 1));
  }

  uint      Time()        const { return m_time;              }
  bool      HasTradeID()  const { return m_mask.has_trade_id; }
  bool      HasOrderID()  const { return m_mask.has_order_id; }
  bool      HasQty()      const { return m_mask.has_qty;      }

  bool      Internal()    const { return m_mask.internal;     }
  AggrT     Aggr()        const { return AggrT(m_mask.aggr);  }
  SideT     Side()        const { return SideT(m_mask.side);  }

  size_t    TradeID()     const { return m_trade_id;          }
  size_t    OrderID()     const { return m_order_id;          }
  PriceT    Price()       const { return m_px;                }
  int       Qty()         const { return m_qty;               }

  void      TradeID(size_t a) { m_trade_id = a; m_mask.has_trade_id = true; }
  void      OrderID(size_t a) { m_order_id = a; m_mask.has_order_id = true; }
  void      Price  (PriceT a) { m_px       = a; }
  void      Qty    (int    a) { m_qty      = a; m_mask.has_qty      = true; }

  int       Write(FILE* a_file);

  std::string ToString(double a_px_step=1) const;

private:
  FieldMask m_mask;
  int       m_time;
  size_t    m_trade_id;
  size_t    m_order_id;
  PriceT    m_px;
  int       m_qty;
};

} // namespace secdb

#include <secdb/secdb_fmt.hxx> // Include implementation