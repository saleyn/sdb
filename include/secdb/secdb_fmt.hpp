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

inline char ToChar(SideT a) {
  static const char s[] = {'B','S'}; return s[int(a)];
}

enum class AggrT : char { Undefined, Aggressor, Passive };

inline char ToChar(AggrT a) {
  static const char s[] = {' ', 'A', 'P'};
  return s[int(a)];
}

inline const char* ToString(AggrT a) {
  static const char* s[] = {"Undef", "Aggr", "Pass"};
  return s[int(a)];
}

inline int64_t sleb128_decode(const char*& a_buf) {
  return utxx::decode_sleb128(a_buf);
}

inline uint64_t uleb128_decode(const char*& a_buf) {
  return utxx::decode_uleb128(a_buf);
}

inline int sleb128_encode(int64_t a_value, char*& a_buf) {
  int sz = utxx::encode_sleb128(a_value, a_buf);
  a_buf += sz;
  return sz;
}

inline int uleb128_encode(uint64_t a_value, char*& a_buf) {
  int sz = utxx::encode_uleb128<0>(a_value, a_buf);
  a_buf += sz;
  return sz;
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
  , INVALID
};

using PriceT = int;

inline uuid UUID(std::string const& a) { return boost::uuids::string_generator()(a); }
/// Convert UUID to string
std::string ToString(uuid const& a);

//------------------------------------------------------------------------------
// SecDB data format constants
//------------------------------------------------------------------------------
/// SecDB version
static constexpr uint VERSION()           { return 1;          }
/// SecDB marker indicating beginning of stream data section
static constexpr uint BEGIN_STREAM_DATA() { return 0xABBABABA; }


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
/// File header
/// \see https://github.com/saleyn/secdb/wiki/Data-Format#file-header
//------------------------------------------------------------------------------
struct Header  {
  /// Minimum expected header length
  static constexpr uint MIN_FILE_SIZE() { return 165; }

  Header() {}

  uint32_t              Version()     const { return m_version;       }

  /// UTC Date of the file
  time_t                Date()        const { return m_date;          }
  /// UTC Midnight corresponding to the file date
  time_t                Midnight()    const { return m_date;          }

  int                   TZOffset()    const { return m_tz_offset;     }
  int                   Depth()       const { return m_depth;         }

  /// Minimal price step (e.g. 0.0001)
  double                PxStep()      const { return m_px_step;       }
  /// Price scale (e.g. 10000)
  int                   PxScale()     const { return m_px_scale;      }
  /// Price precision in digits after the decimal point (e.g. 4)
  int                   PxPrecision() const { return m_px_precision;  }
  std::string const&    Exchange()    const { return m_exchange;      }
  std::string const&    Symbol()      const { return m_symbol;        }
  std::string const&    Instrument()  const { return m_instrument;    }
  long                  SecID()       const { return m_secid;         }
  uuid                  UUID()        const { return m_uuid;          }

  std::string const&    TZName()      const { return m_tz_name;       }
  void                  TZName(const char* a)      { m_tz_name = a;   }

  std::string           TZ()          const;

  /// Set values of file header
  /// @param a_tz_name local time zone name
  void Set
  (
    int                 a_ver,
    std::string const&  a_xchg,
    std::string const&  a_symbol,
    std::string const&  a_instr,
    long                a_secid,
    time_t              a_date,
    std::string const&  a_tz_name,
    int                 a_tz_offset,
    uint8_t             a_depth,
    double              a_px_step,
    uuid        const&  a_uuid   = boost::uuids::random_generator()()
  );

  /// Read header from a file descriptor
  int Read(FILE* a_file, size_t a_file_size);

  /// Write header to a file descriptor
  int Write(FILE* a_file, int a_debug = 0);

  std::ostream& Print(std::ostream& out, const std::string& a_ident = "") const;

private:
  uint32_t    m_version       = 0;
  std::string m_exchange;
  std::string m_symbol;
  std::string m_instrument;
  long        m_secid         = 0;
  time_t      m_date          = 0;
  int         m_tz_offset     = 0;
  std::string m_tz_name;
  int         m_depth         = 10;
  double      m_px_step       = 0.01;
  int         m_px_scale      = 100;
  int         m_px_precision  = 2;
  uuid        m_uuid          = boost::uuids::nil_generator()();
};

//------------------------------------------------------------------------------
/// Metadata about streams contained in the file
//------------------------------------------------------------------------------
// https://github.com/saleyn/secdb/wiki/Data-Format#streamsmeta-streams-metadata
//------------------------------------------------------------------------------
struct StreamsMeta {
  static constexpr uint8_t CODE() { return 0x1; }

  enum class CompressT {
    None,   // No compression
    GZip    // Gzip compression
  };

  //----------------------------------------------------------------------------
  /// Stream Metadata
  //----------------------------------------------------------------------------
  // https://github.com/saleyn/secdb/wiki/Data-Format#streammeta-stream-metadata
  //----------------------------------------------------------------------------
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

  /// Read StreamsMeta from file
  /// This method must be called right after reading the file's header!
  void Read(FILE* a_file);
  /// Write StreamsMeta to file
  int  Write(FILE* a_file, int a_debug = 0);
  /// Update beginning of data offset in the StreamsMeta header
  int  WriteDataOffset(FILE* a_file, uint a_data_offset);

private:
  CompressT     m_compression     = CompressT::None;
  uint          m_data_offset_pos = 0;
  uint          m_data_offset     = 0;
  StreamsVec    m_streams;
};


//------------------------------------------------------------------------------
/// Candle
/// \see https://github.com/saleyn/secdb/wiki/Data-Format#candle-candle-data
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
// https://github.com/saleyn/secdb/wiki/Data-Format#candleheader-candle-metadata
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
    int      a_start_time,
    int      a_end_time,
    uint32_t a_data_offset = 0
  )
    : m_resolution (a_resolution)
    , m_start_time (a_start_time)
    , m_data_offset(a_data_offset)
    , m_candles    (CalcSize(a_start_time, a_end_time, a_resolution))
  {
    assert(a_end_time > a_start_time);
  }

  /// Update m_data_offset if \a a_ts corresponds to the beginning of candle
  void UpdateDataOffset(int a_ts, uint64_t a_data_offset);

  /// Update the candle corresponding to \a a_ts time
  /// @return true on success or false if \a a_ts is outside of range
  bool UpdateCandle(int a_ts, PriceT a_px, int a_qty);

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
  Candle*           TimeToCandle(int a_ts);

  /// Convert candle index \a a_idx to StartTime since UTC
  int               CandleToTime(uint a_idx) const;
private:
  uint16_t   m_resolution;
  int        m_start_time;
  size_t     m_data_offset;
  Candle*    m_last_updated = nullptr; // Last updated candle
  CandlesVec m_candles;

  static int CalcSize(int a_start_time, int a_end_time, uint16_t a_resolution);
};

//------------------------------------------------------------------------------
/// Candle Block Metadata
//------------------------------------------------------------------------------
// https://github.com/saleyn/secdb/wiki/Data-Format#candlesmeta-candles-metadata
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

  /// Read StreamsMeta from file.
  /// This method must be called right after reading the file's header!
  void Read(FILE* a_file);

  /// Write CandlesMeta and Candle data to file.
  /// It's permissible to call this method when Candle data is empty, in which
  /// case it'll reserve space in file for updating the candles later using
  /// CommitCandles() method.
  int  Write(FILE* a_file, int a_debug = 0);

  /// @param a_ts second since UTC midnight
  /// @param a_data_offset position of file corrsponding to \a a_ts second
  void UpdateDataOffset(int a_ts, uint64_t a_data_offset);

  /// Update the candles corresponding to \a a_ts time in each candle resolution
  /// @param a_ts time in seconds since midnight
  /// @return true on success or false if \a a_ts is outside of range
  void UpdateCandles(int a_ts, PriceT a_px, int a_qty);

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
    : m_type(char(a_tp)), m_delta(a_dlt)
  {
    static_assert(sizeof(StreamBase) == sizeof(uint8_t), "Invalid size");
  }

  bool       Delta()    const { return m_delta;            }
  StreamType Type()     const { return StreamType(m_type); }

  void       Write(char*& a)  { *a++ = *(uint8_t*)this;    }
private:
  char       m_type : 7;
  bool       m_delta: 1;
};

//------------------------------------------------------------------------------
/// Representation of seconds from midnight
/// \see https://github.com/saleyn/secdb/wiki/Data-Format#seconds-stream
//------------------------------------------------------------------------------
struct SecondsSample : public StreamBase {
  SecondsSample() {}
  SecondsSample(int a_now)
    : StreamBase(0, StreamType::Seconds), m_time(a_now)
  {
    assert(a_now < ((1 << 24) - 1));
  }

  int  Time()             const { return m_time;      }
  void Time(int a_midsecs)      { m_time = a_midsecs; }

  int  Write(FILE* a_file);

  int  Read(const char* a_buf, size_t a_sz);
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

  int Encode(char*& a_buf, const char* a_end) const {
    int sz  = sleb128_encode(m_px,  a_buf);
        sz += sleb128_encode(m_qty, a_buf);
    return sz;
  }

  int Decode(const char*& a_buf, const char* a_end) {
    m_px  = sleb128_decode(a_buf);
    m_qty = sleb128_decode(a_buf);
    return a_buf - a_end;
  };
};

template <uint MaxDepth, typename PxT>
using PxLevels = std::array<PxLevel<PxT>, MaxDepth>;

//------------------------------------------------------------------------------
/// Representation of Quote sample
/// \see https://github.com/saleyn/secdb/wiki/Data-Format#quotes-stream
//------------------------------------------------------------------------------
template <uint MaxDepth, typename PxT>
struct QuoteSample : public StreamBase {
  using PxLevelT  = PxLevel<PxT>;
  using PxLevelsT = PxLevels<MaxDepth*2, PxT>;

  QuoteSample() : m_levels(PxLevelsT()) {}
  QuoteSample
  (
    bool        a_delta, uint   a_ts,
    PxLevelsT&& a_lev,   size_t a_bid_cnt = 0, size_t a_ask_cnt = 0
  )
    : StreamBase(a_delta, StreamType::Quotes)
    , m_time    (a_ts)
    , m_levels  (std::move(a_lev))
    , m_bid_cnt (a_bid_cnt)
    , m_ask_cnt (a_ask_cnt)
  {
    assert(m_bid_cnt + m_ask_cnt <= int(MaxDepth));
  }

  int              Time()       const { return m_time;    }
  int              BidCount()   const { return m_bid_cnt; }
  int              AskCount()   const { return m_ask_cnt; }

  PxLevelsT const& Levels()     const { return m_levels; }

  PxLevelT  const* BestBid()    const { return &m_levels[m_bid_cnt-1]; }
  PxLevelT  const* BestAsk()    const { return &m_levels[m_bid_cnt];   }

  PxLevelT  const* EndBid()     const { return &m_levels[0] - 1;       }
  PxLevelT  const* EndAsk()     const { return &m_levels[m_bid_cnt+m_ask_cnt]; }

  static void  NextBid(PxLevelT const*& p) { --p; }
  static void  NextAsk(PxLevelT const*& p) { ++p; }

  int   Write(FILE* a_file);

  int   Read(const char* a_buf, size_t a_sz, bool a_is_delta, PxT& a_last_px);
private:
  uint      m_time;
  PxLevelsT m_levels;
  int       m_bid_cnt;
  int       m_ask_cnt;
};

//------------------------------------------------------------------------------
/// Representation of Trade sample
/// \see https://github.com/saleyn/secdb/wiki/Data-Format#trade-stream
//------------------------------------------------------------------------------
struct TradeSample : public StreamBase {
  struct FieldMask {
    bool internal     : 1;
    char aggr         : 2;
    bool side         : 1;
    bool has_qty      : 1;
    bool has_trade_id : 1;
    bool has_order_id : 1;
    bool _unused      : 1;

    FieldMask()           { *(uint8_t*)this = 0; }
    FieldMask(uint8_t a)  { *(uint8_t*)this = a; }
    FieldMask(FieldMask const&) = default;
    FieldMask(bool a_internal, AggrT a_aggr,    SideT a_sd,
              bool a_has_qty,  bool  a_has_oid, bool  a_has_trid)
      : internal    (a_internal)
      , aggr        ((int)a_aggr)
      , side        (a_sd == SideT::Sell)
      , has_qty     (a_has_qty)
      , has_trade_id(a_has_trid)
      , has_order_id(a_has_oid)
      ,_unused      (false)
    {
      static_assert(sizeof(FieldMask) == sizeof(uint8_t), "Invalid size");
    }
  };

  TradeSample() {}
  TradeSample(bool a_delta, FieldMask a_mask, uint a_ts, PriceT a_px, uint a_qty,
              size_t a_ord_id = 0, size_t a_trade_id = 0)
    : StreamBase(a_delta, StreamType::Trade)
    , m_mask    (a_mask)
    , m_time    (a_ts)
    , m_trade_id(a_trade_id)
    , m_order_id(a_ord_id)
    , m_px      (a_px)
    , m_qty     (a_qty)
  {
    assert(a_ts < ((1 << 24) - 1));
  }

  TradeSample(bool   a_delta, uint a_ts, SideT  a_sd, PriceT a_px, int a_qty,
              AggrT  a_aggr = AggrT::Undefined,
              size_t a_ord_id   = 0,     size_t a_trade_id = 0,
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

  void      TradeID(size_t a)   { m_trade_id = a; m_mask.has_trade_id = true; }
  void      OrderID(size_t a)   { m_order_id = a; m_mask.has_order_id = true; }
  void      Price  (PriceT a)   { m_px       = a; }
  void      Qty    (uint   a)   { m_qty      = a; m_mask.has_qty      = true; }

  void      Set(FieldMask a, PriceT a_px, int a_qty, size_t a_tid, size_t a_oid);

  int       Write(FILE* a_file);
  int       Read (const char* a_buf, size_t  a_sz,
                  bool   a_is_delta, PriceT& a_last_px);

  std::string ToString(double a_px_step=1) const;

private:
  FieldMask m_mask;
  int       m_time;
  size_t    m_trade_id;
  size_t    m_order_id;
  PriceT    m_px;
  uint      m_qty;
};

} // namespace secdb

#include <secdb/secdb_fmt.hxx> // Include implementation