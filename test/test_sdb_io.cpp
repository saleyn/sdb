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
    BaseSDBFileIO<10> sdb;

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
    BaseSDBFileIO<10> sdb;
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
}
