// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file  secdb_krx_fmt.hpp
//------------------------------------------------------------------------------
/// \brief SecDB file format reader/writer
///
/// \see https://github.com/saleyn/secdb/wiki/Data-Format
//------------------------------------------------------------------------------
// Copyright (c) 2015 Omnibius, LLC
// Author:  Serge Aleynikov <saleyn@gmail.com>
// Created: 2015-10-15
//------------------------------------------------------------------------------
#include <secdb/secdb.hpp>
#include <utxx/get_option.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/progress.hpp>

using namespace std;
using namespace secdb;

//------------------------------------------------------------------------------
void Usage(std::string const& a_text = "")
{
  if (!a_text.empty())
    cout << a_text << endl << endl;

  cout << "SecDB file format reader\n"
       << "Copyright (c) 2015 Omnibius, LLC\n\n"
       << "Usage: " << utxx::path::program::name()
       << " -f MDFilename [-o|-O OutputDir] [-d] [-q]\n"
       << "\nOptions:\n"
       << "  -f MDFilename        - filename with KRX market data\n"
       << "  -o|--dir OutFile     - output filename (def: stdout)\n"
       << "  -d                   - enable debug printouts\n"
       << "  -q                   - quiet mode (don't display a progress bar)\n"
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

//------------------------------------------------------------------------------
int main(int argc, char* argv[])
//------------------------------------------------------------------------------
{
  if (argc < 2)
    Usage("Missing required option(s)");

  set_terminate(&UnhandledException);

  std::string filename;
  bool        quiet   = false;
  int         debug   = 0;
  std::string outfile;

  utxx::opts_parser opts(argc, argv);
  while  (opts.next()) {
      if (opts.match("-f", "", &filename))        continue;
      if (opts.match("-d", string("--debug")))    { debug++; continue; }
      if (opts.match("-q", "--quiet",  &quiet))   continue;
      if (opts.match("-o", "--output", &outfile)) continue;

      if (opts.is_help()) Usage();

      Usage(string("Invalid option: ") + opts());
  }

  if (filename.empty()) Usage("Missing required option -f");

  if (outdir.empty())   outdir = utxx::path::dirname(filename);

  auto file = fopen(filename.c_str(), "r");

  if (!file) {
    cerr << "Cannot open file " << filename << ": " << strerror(errno) << endl;
    exit(1);
  }

  std::shared_ptr<boost::progress_display> show_progress;

  BaseSecDBFileIO<3> output(filename);

  char buf[512];

  while (fgets(buf, sizeof(buf), file)) {
    if (buf[0] == '#') continue;  // This is a comment

    vector<string> fields;

    auto value = [&](MD a_fld) { return fields[int(a_fld)]; };

    boost::split(fields, buf, boost::is_any_of(" |"),
        boost::algorithm::token_compress_on);

    if (fields.size() != int(MD::SIZE)) {
      cerr << "Invalid record format:\n  " << buf << endl;
      continue;
    }

    time_val now(stol(value(MD::UTCTime)) / 1000, 0);

    if (!valid) {
      time_t d = now.sec() - now.sec() % 86400;

      if (d != date) {
        cerr << "Invalid date (expected: "
             << utxx::timestamp::to_string(time_val(date, 0), utxx::DATE)
             << ", got: "
             << utxx::timestamp::to_string(now, utxx::DATE) << '\n';
        exit(1);
      }

      valid = true;

      output.Open<OpenMode::Write>
        (outdir, subdirs, xchg, symbol, instr, secid, date, 3, 0.01, 0664);

      output.WriteStreamsMeta({StreamType::Quotes, StreamType::Trade});
      // 1min candles from 9am to 15pm
      output.WriteCandlesMeta({CandleHeader(60, 3600*9, 3600*15)});

      output.Flush();
    }

    float bid =      stof(value(MD::Bid));
    float ask =      stof(value(MD::Ask));

    auto book = PxLevels<6, float>
    {{
        {bid - 0.10f,  stoi(value(MD::L3BVo))}
      , {bid - 0.05f,  stoi(value(MD::L2BVo))}
      , {bid        ,  stoi(value(MD::L1BVo))}
      , {ask        , -stoi(value(MD::L1AVo))}
      , {ask + 0.05f, -stoi(value(MD::L2AVo))}
      , {ask + 0.10f, -stoi(value(MD::L3AVo))}
    }};

    float last_px  = stof(value(MD::LstPx));
    int   last_qty = stoi(value(MD::LstQty));

    // Write the quote info
    output.WriteQuotes<PriceUnit::DoubleVal>(now, std::move(book), 6);

    if (last_qty != 0) {
      // Write trade details
      SideT side = last_qty < 0 ? SideT::Sell : SideT::Buy;
      AggrT aggr = ((side == SideT::Buy  && abs(last_px - ask) < 0.001) ||
                    (side == SideT::Sell && abs(last_px - bid) < 0.001))
                 ? AggrT::Aggressor : AggrT::Passive;

      output.WriteTrade<PriceUnit::DoubleVal>
        (now, side, last_px, last_qty, aggr, 0, 0);
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