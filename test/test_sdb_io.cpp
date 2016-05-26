// vim:ts=2:sw=2:et
//------------------------------------------------------------------------------
/// @file   test_sdb_io.cpp
/// @author Serge Aleynikov
//------------------------------------------------------------------------------
/// @brief Test module for SDB file I/O.
//------------------------------------------------------------------------------
// Copyright (c) 2015 Omnibius, LLC
// Created:  2015-10-05
//------------------------------------------------------------------------------

#include <boost/test/unit_test.hpp>
#include <utxx/test_helper.hpp>
#include <utxx/path.hpp>
#include <stdio.h>
#include <sdb/sdb_fmt_io.hxx>

using namespace std;
using namespace sdb;

using SDBFileIO = BaseSDBFileIO<10>;

struct Printer {
  Printer(SDBFileIO& a_file) : m_file(a_file) {}

  bool operator()(SecondsSample const& a_sec) {
    int snum = m_seconds++;
    switch (snum) {
      case 0:
        BOOST_CHECK_EQUAL(3600, a_sec.Time());
        break;
      case 1:
        BOOST_CHECK_EQUAL(3605, a_sec.Time());
        break;
      default:
        BOOST_CHECK(false);
        break;
    }
    return true;
  }

  bool operator()(QuoteSample<SDBFileIO::MAX_DEPTH(), int> const& a) {
    auto pb = a.BestBid();
    auto pa = a.BestAsk();
    auto eb = a.EndBid();
    auto ea = a.EndAsk();

    int qnum = m_quotes++;
    time_val tv;

    switch (qnum) {
      case 0:
        tv = utxx::time_val::universal_time(2015, 10, 15, 1, 0, 0, 0);
        BOOST_CHECK(tv == m_file.Time());
        break;
      case 1:
        tv = utxx::time_val::universal_time(2015, 10, 15, 1, 0, 5, 0);
        BOOST_CHECK(tv == m_file.Time());
        break;
      default:
        BOOST_CHECK(false);
        break;
    }

    for (int i = 0; pb != eb; a.NextBid(pb), ++i) {
      auto px  = pb->m_px * m_file.PxStep();
      auto qty = pb->m_qty;
      if (qnum == 0) {
        //BOOST_CHECK_EQUAL(m_file.Time());
        switch (i) {
          case 0:
            BOOST_CHECK_EQUAL(1.10, px);
            BOOST_CHECK_EQUAL(30,   qty);
            break;
          case 1:
            BOOST_CHECK_EQUAL(1.05, px);
            BOOST_CHECK_EQUAL(20,   qty);
            break;
          case 2:
            BOOST_CHECK_EQUAL(1.00, px);
            BOOST_CHECK_EQUAL(10,   qty);
            break;
          default:
            BOOST_CHECK(false);
        }
      } else if (qnum == 1) {
        switch (i) {
          case 0:
            BOOST_CHECK_EQUAL(1.11, px);
            BOOST_CHECK_EQUAL(31,   qty);
            break;
          case 1:
            BOOST_CHECK_EQUAL(1.06, px);
            BOOST_CHECK_EQUAL(21,   qty);
            break;
          default:
            BOOST_CHECK(false);
        }
      } else {
        BOOST_CHECK(false);
      }
    }
    for (int i = 0; pa != ea; a.NextAsk(pa), ++i) {
      auto px  = pa->m_px * m_file.PxStep();
      auto qty = pa->m_qty;
      if (qnum == 0) {
        switch (i) {
          case 0:
            BOOST_CHECK_EQUAL(1.11, px);
            BOOST_CHECK_EQUAL(20,   qty);
            break;
          case 1:
            BOOST_CHECK_EQUAL(1.16, px);
            BOOST_CHECK_EQUAL(40,   qty);
            break;
          case 2:
            BOOST_CHECK_EQUAL(1.20, px);
            BOOST_CHECK_EQUAL(60,   qty);
            break;
          default:
            BOOST_CHECK(false);
        }
      } else if (qnum == 1) {
        switch (i) {
          case 0:
            BOOST_CHECK_EQUAL(1.12, px);
            BOOST_CHECK_EQUAL(21,   qty);
            break;
          case 1:
            BOOST_CHECK_EQUAL(1.16, px);
            BOOST_CHECK_EQUAL(41,   qty);
            break;
          default:
            BOOST_CHECK(false);
        }
      } else {
        BOOST_CHECK(false);
      }
    }
    return true;
  }

  bool operator()(TradeSample const& a_trade) {
    BOOST_CHECK(false);
    return true;
  }

  template <typename T>
  bool operator()(T const& a_other) {
    UTXX_THROW_RUNTIME_ERROR("Unsupported stream type");
    return true;
  }

private:
  SDBFileIO& m_file;
  int        m_quotes  = 0;
  int        m_seconds = 0;
};

static std::string TempPath(const std::string& a_add_str = "") {
    #if defined(__windows__) || defined(_WIN32) || defined(_WIN64) || defined(_MSC_VER)
    auto    p = getenv("TEMP");
    if (!p) p = "";
    #else
    auto    p = P_tmpdir;
    #endif
    auto r = std::string(p);
    return (a_add_str.empty()) ? r : r + utxx::path::slash_str() + a_add_str;
}

BOOST_AUTO_TEST_CASE( test_sdb )
{
  auto  dir = TempPath();
  std::string file;

  auto date = utxx::time_val::universal_time(2015, 10, 15, 0, 0, 0, 0);
  auto uuid = UUID("0f7f69c9-fc9d-4517-8318-706e3e58dadd");
  {
    SDBFileIO sdb;

    utxx::path::file_unlink
      (sdb.Filename(dir,false,"KRX","KR4101","KR4101K60008",1,date));

    UTXX_CHECK_NO_THROW
      (sdb.Open<OpenMode::Write>
        (dir,  false, "KRX", "KR4101", "KR4101K60008", 1, date, "KST", 3600*9,
         5,    0.01,  0640,  uuid));

    sdb.WriteStreamsMeta({StreamType::Quotes, StreamType::Trade});
    sdb.WriteCandlesMeta({CandleHeader(300, 3600*9, 3600*15)});

    file = sdb.Filename();
  }

  {
    SDBFileIO sdb;
    UTXX_CHECK_NO_THROW(sdb.Open(file));

    BOOST_CHECK_EQUAL(date,            sdb.Info().Midnight());
    BOOST_CHECK_EQUAL(5,               sdb.Info().Depth());
    BOOST_CHECK_EQUAL(0.01,            sdb.Info().PxStep());
    BOOST_CHECK_EQUAL(100,             sdb.Info().PxScale());
    BOOST_CHECK_EQUAL(2,               sdb.Info().PxPrecision());
    BOOST_CHECK_EQUAL("KRX",           sdb.Info().Exchange());
    BOOST_CHECK_EQUAL("KR4101",        sdb.Info().Symbol());
    BOOST_CHECK_EQUAL("KR4101K60008",  sdb.Info().Instrument());
    BOOST_CHECK_EQUAL(1,               sdb.Info().SecID());
    BOOST_CHECK_EQUAL(ToString(uuid),  ToString(sdb.Info().UUID()));
  }

  BOOST_CHECK_EQUAL(2546, utxx::path::file_size(file));

  utxx::path::file_unlink(file);
}

BOOST_AUTO_TEST_CASE( test_sdb_no_candles )
{
  auto  dir = TempPath();
  std::string file;

  auto date = utxx::time_val::universal_time(2015, 10, 15, 0, 0, 0, 0);
  auto uuid = UUID("0f7f69c9-fc9d-4517-8318-706e3e58dadd");
  {
    SDBFileIO sdb;

    utxx::path::file_unlink
      (sdb.Filename(dir,false,"KRX","KR4101","KR4101K60008",1,date));

    UTXX_CHECK_NO_THROW
      (sdb.Open<OpenMode::Write>
        (dir,  false, "KRX", "KR4101", "KR4101K60008", 1, date, "KST", 3600*9,
         5,    0.01,  0640,  uuid));

    file = sdb.Filename();

    sdb.WriteStreamsMeta({StreamType::Quotes, StreamType::Trade});
    sdb.WriteCandlesMeta({});

    PxLevels<10, double> bids;
    PxLevels<10, double> asks;

    bids[0].Set(1.10, 30);  asks[0].Set(1.11, 20);
    bids[1].Set(1.05, 20);  asks[1].Set(1.16, 40);
    bids[2].Set(1.00, 10);  asks[2].Set(1.20, 60);

    auto dt = date + utxx::secs(3600);

    sdb.WriteQuotes<PriceUnit::DoubleVal>
      (dt, std::move(bids), 3, std::move(asks), 3);

    bids[0].Set(1.11, 31);  asks[0].Set(1.12, 21);
    bids[1].Set(1.06, 21);  asks[1].Set(1.16, 41);

    dt = date + utxx::secs(3605);

    sdb.WriteQuotes<PriceUnit::DoubleVal>
      (dt, std::move(bids), 2, std::move(asks), 2);
  }

  {
    SDBFileIO sdb;
    Printer printer(sdb);
    UTXX_CHECK_NO_THROW(sdb.Open(file));

    BOOST_CHECK_EQUAL(date, sdb.Info().Midnight());

    sdb.Read(printer);
  }

  BOOST_CHECK_EQUAL(258, utxx::path::file_size(file));

  utxx::path::file_unlink(file);
}
