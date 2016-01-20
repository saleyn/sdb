// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file  sdb_krx_fmt.hpp
//------------------------------------------------------------------------------
/// \brief SecDB file format reader/writer
///
/// \see https://github.com/saleyn/sdb/wiki/Data-Format
//------------------------------------------------------------------------------
// Copyright (c) 2015 Omnibius, LLC
// Author:  Serge Aleynikov <saleyn@gmail.com>
// Created: 2015-10-15
//------------------------------------------------------------------------------
#include <sdb/sdb.hpp>
#include <utxx/get_option.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/progress.hpp>

using namespace std;
using namespace sdb;

//------------------------------------------------------------------------------
void Usage(std::string const& a_text = "")
{
  if (!a_text.empty())
    cout << a_text << endl << endl;

  cout << "KRX to SecDB file format converter\n"
       << "Copyright (c) 2015 Omnibius, LLC\n\n"
       << "Usage: " << utxx::path::program::name()
       << " -f MDFilename -x Exchange -s Symbol -i Instr -n SecID -y Date\n"
       << "       [-o|-O OutputDir] [-d] [-q]\n"
       << "\nOptions:\n"
       << "  -f MDFilename        - filename with KRX market data\n"
       << "  -o|--dir OutDir      - output directory (def: MDFilename's dir)\n"
       << "  -O|--full-dir OutDir - deep output directory (same as -o option,\n"
       << "                         except the subdirectory structure is\n"
       << "                         created inside OutDir according to\n"
       << "                         SecDB directory specification format\n"
       << "  -d                   - enable debug printouts\n"
       << "  -q                   - quiet mode (don't display a progress bar)\n"
       << "  -x|--xchg Exchange   - name of the financial exchange\n"
       << "  -s Symbol            - company-specific symbol name\n"
       << "  -i Instr             - exchange-specific instrument name\n"
       << "  -n|--secid SecID     - exchange-specific security id\n"
       << "  -y|--date YYYYMMDD   - date of market data in this file\n"
       << "\nExample:\n\n"
       << "Convert data.txt to /tmp/20150626.KRX.KR4101.KR4101K90008.sdb:\n"
       << "  " << utxx::path::program::name()
       << " -f data.txt -o /tmp -q -x KRX -s KR4101 -i KR4101K90008 -n 4101 "
       << "-y 20150626\n\n"
       << "Source file (data.txt):\n"
       << "# UTCTime     |    Bid L1BVo L2BVo L3BVo |    Ask L1AVo L2AVo L3AVo | LstPx LstQty | NBids NAsks TotBV TotAV\n"
       << "1435276800566 | 253.70    81    11   118 | 253.80    15    16    26 | 253.80     1 |   918  1174  7014  9164\n"
       << "1435276800566 | 253.70    81    11   118 | 253.80    14    16    26 | 253.80     1 |   918  1174  7014  9163\n"
       << "1435276800567 | 253.70    81    11   118 | 253.80    14    16    26 |   0.00     0 |   918  1175  7014  9164\n"
       << "1435276800588 | 253.70    81    11   118 | 253.80    13    16    26 | 253.80     1 |   921  1174  7017  9163\n"
       << "1435276800588 | 253.70    81    11   118 | 253.80    13    16    26 |   0.00     0 |   921  1175  7017  9164\n"
       << "1435276800600 | 253.70    31    11   118 | 253.80    13    16    26 | 253.70   -50 |   907  1175  6975  9164\n"
       << "\n";
  exit(1);
}

//------------------------------------------------------------------------------
void UnhandledException() {
  auto p = current_exception();
  try    { rethrow_exception(p); }
  catch  ( exception& e ) { cerr << e.what() << endl; }
  catch  ( ... )          { cerr << "Unknown exception" << endl; }
  exit(1);
}

// Fields in the KRX data file
enum class MD {
    UTCTime
  , Bid
  , L1BVo
  , L2BVo
  , L3BVo
  , Ask
  , L1AVo
  , L2AVo
  , L3AVo
  , LstPx
  , LstQty
  , NBids
  , NAsks
  , TotBV
  , TotAV
  , SIZE
};

//------------------------------------------------------------------------------
int main(int argc, char* argv[])
//------------------------------------------------------------------------------
{
  if (argc < 2)
    Usage("Missing required option(s)");

  set_terminate(&UnhandledException);

  std::string filename;
  bool        quiet     = false;
  int         debug     = 0;
  bool        subdirs   = false;
  std::string outdir;
  std::string xchg;
  std::string symbol;
  std::string instr;
  std::string dtstr;
  std::string tz("Asia/Seoul");
  time_val    date;
  long        secid   = 0;

  bool        valid   = false;

  utxx::opts_parser opts(argc, argv);
  while  (opts.next()) {
      if (opts.match("-f", "",           &filename)) continue;
      if (opts.match("-d", "--debug"))             { debug++; continue; }
      if (opts.match("-q", "--quiet",    &quiet))    continue;
      if (opts.match("-o", "--dir",      &outdir))   continue;
      if (opts.match("-O", "--full-dir", &outdir)) { subdirs = true; continue; }
      if (opts.match("-x", "--xchg",     &xchg))     continue;
      if (opts.match("-s", "--symbol",   &symbol))   continue;
      if (opts.match("-i", "--instr",    &instr))    continue;
      if (opts.match("-n", "--secid",    &secid))    continue;
      if (opts.match("-t", "--tzone",    &tz))       continue;
      if (opts.match("-y", "--date",     &dtstr))  {
        if (dtstr.size() != 8)
          Usage("Invalid date format (expected: YYYYMMDD)");
        int y = stoi(dtstr.substr(0, 4));
        int m = stoi(dtstr.substr(4, 2));
        int d = stoi(dtstr.substr(6, 2));
        date = utxx::time_val::universal_time(y, m, d, 0, 0, 0, 0);
        continue;
      }

      if (opts.is_help()) Usage();

      Usage(string("Invalid option: ") + opts());
  }

  if (filename.empty()) Usage("Missing required option -f");
  if (xchg.empty())     Usage("Missing required option -e");
  if (symbol.empty())   Usage("Missing required option -s");
  if (instr.empty())    Usage("Missing required option -i");
  if (!date)            Usage("Missing required option -y");
  if (secid == 0)       Usage("Missing required option -n");

  if (outdir.empty())   outdir = utxx::path::dirname(filename);

  struct tm lt = {0};
  time_t     t = date.sec();

  bool have_tz = !tz.empty();

  if (have_tz)
    setenv("TZ", tz.c_str(), 1);

  localtime_r(&t, &lt);

  if (have_tz && lt.tm_zone[0] == '\0')
    UTXX_THROW_RUNTIME_ERROR("Invalid time zone ", tz);
  else
    tz = lt.tm_zone;

  auto tz_offset = lt.tm_gmtoff;

  if (debug)
    cerr << "UTC offset: " << tz_offset << "s (" << (tz_offset/3600) << "h) "
         << tz << endl;

  auto file = fopen(filename.c_str(), "r");

  if (!file) {
    cerr << "Cannot open file " << filename << ": " << strerror(errno) << endl;
    exit(1);
  }

  std::shared_ptr<boost::progress_display> show_progress;

  long file_size = utxx::path::file_size(filename);
  long file_pos  = 0;

  BaseSecDBFileIO<3> output;

  output.Debug(debug);

  auto out_name = output.Filename(outdir, subdirs, xchg, symbol, instr, secid, date);
  utxx::path::file_unlink(out_name);

  if (!quiet) {
    cerr << filename << " -> " << out_name << endl;
    if (file_size > 0)
      show_progress.reset(new boost::progress_display(file_size, cerr));
  }

  char buf[512];

  while (fgets(buf, sizeof(buf), file)) {
    if (buf[0] == '#') continue;  // This is a comment

    vector<string> fields;

    auto value = [&](MD a_fld) { return fields[int(a_fld)]; };

    boost::split(fields, buf, boost::is_any_of(" |"),
        boost::algorithm::token_compress_on);

    if (fields.size() == 19) {
      // This is the format containing 5 price levels rather than 3.
      // Throw out the extra two levels
      fields.erase(fields.begin()+11, fields.begin()+13); // Remove L4AVo,L5AVo
      fields.erase(fields.begin()+5,  fields.begin()+7);  // Remove L4BVo,L5BVo
    }
    else if (fields.size() != int(MD::SIZE)) {
      cerr << "Invalid record format (expected " << int(MD::SIZE)
           << " fields, got " << fields.size() << "):\n  " << buf
           << endl;
      continue;
    }

    auto msec = stol(value(MD::UTCTime));
    time_val now(msec / 1000, (msec % 1000) * 1000);

    if (!valid) {
      time_val d = now - utxx::secs(now.sec() % 86400);

      if (d != date) {
        cerr << "Invalid date (expected: "
             << utxx::timestamp::to_string(date, utxx::DATE)
             << ", got: "
             << utxx::timestamp::to_string(now, utxx::DATE) << '\n';
        exit(1);
      }

      valid = true;

      output.Open<OpenMode::Write>
        (outdir, subdirs, xchg, symbol, instr, secid, date, tz, tz_offset,
         3,      0.05,    0664);

      output.WriteStreamsMeta({StreamType::Quotes, StreamType::Trade});
      // 1min candles from 9:00am to 15:00pm KST time
      auto start_tm  =  9*3600 - output.TZOffset();
      auto end_tm    = 15*3600 + 60 - output.TZOffset();
      output.WriteCandlesMeta
        ({CandleHeader(60, start_tm, end_tm)});

      output.Flush();
    }

    float bid =       stof(value(MD::Bid));
    float ask =       stof(value(MD::Ask));

    auto bids = PxLevels<3, float>
    {{
        {bid        , stoi(value(MD::L1BVo))}
      , {bid - 0.05f, stoi(value(MD::L2BVo))}
      , {bid - 0.10f, stoi(value(MD::L3BVo))}
    }};
    auto asks = PxLevels<3, float>
    {{
        {ask        , stoi(value(MD::L1AVo))}
      , {ask + 0.05f, stoi(value(MD::L2AVo))}
      , {ask + 0.10f, stoi(value(MD::L3AVo))}
    }};

    float last_px  = stof(value(MD::LstPx));
    int   last_qty = stoi(value(MD::LstQty));

    // Write the quote info
    output.WriteQuotes<PriceUnit::DoubleVal>
      (now, std::move(bids), 3, std::move(asks), 3);

    if (last_qty != 0) {
      // Write trade details
      SideT side = last_qty < 0 ? SideT::Sell : SideT::Buy;
      AggrT aggr = ((side == SideT::Buy  && abs(last_px - ask) < 0.001) ||
                    (side == SideT::Sell && abs(last_px - bid) < 0.001))
                 ? AggrT::Aggressor : AggrT::Passive;

      output.WriteTrade<PriceUnit::DoubleVal>
        (now, side, last_px, abs(last_qty), aggr, 0, 0);
    }

    if (show_progress) {
      auto pos = ftell(file);
      auto len = pos - file_pos;
      *show_progress += len;
      file_pos = pos;
    }
  }

  output.Close();

  return 0;
}
