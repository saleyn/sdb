// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file  sdbdump.hpp
//------------------------------------------------------------------------------
/// \brief SDB file reader
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
#include <iostream>
#include <fstream>

using namespace std;
using namespace sdb;

using SDBFileIO = BaseSDBFileIO<10>;

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
void Usage(std::string const& a_text = "")
{
  if (!a_text.empty())
    cout << a_text << endl << endl;

  cout << "SDB file reader\n"
       << "Copyright (c) 2015 Omnibius, LLC\n\n"
       << "Usage: " << utxx::path::program::name()
       << " -f MDFilename [-o|-O OutputFile] [-d] [-q]\n"
       << "\nOptions:\n"
       << "  -f MDFilename         - filename with KRX market data\n"
       << "  -o|--output OutFile   - output filename (def: stdout)\n"
       << "  -d                    - enable debug printouts\n"
       << "  -q                    - quiet mode (don't display a progress bar)\n"
       << "  -m|--max-depth Levels - limit max book depth to number of Levels\n"
       << "  -D                    - include YYYYMMDD in timestamp output\n"
       << "  --msec                - use millisecond time resolution (def usec)\n"
       << "  --tz-local            - format time in the file's local time zone\n"
       << "  --tz-utc              - format time in the UTC time zone (default)\n"
       << "  -p|--px-only          - don't display quantity information\n"
       << "  -S|--symbol           - include symbol name in the output\n"
       << "  -X|--xchg             - include exchange name in the output\n"
       << "  -I|--instr            - include instrument name in the output\n"
       << "  -Q|--quotes           - print quotes\n"
       << "  -T|--trades           - print trades\n"
       << "  -C|--candles Resol    - print candles of given resolution\n"
       << "                             Valid resolutions: Ny, where:\n"
       << "                               N - resolution interval\n"
       << "                               y - (s)econds, (m)inutes, (h)ours\n"
       << "                          Example: 10m - ten minutes, 1h - one hour\n"
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
struct Printer {
  Printer
  (
    SDBFileIO& a_file, ostream& a_out, uint a_stream_mask,
    utxx::stamp_type a_time_fmt, std::string const& a_xchg,
    std::string const& a_symbol, std::string const& a_instr,
    bool a_tz_local, int    a_max_depth = 100,    bool   a_px_only = false
  )
    : m_file(a_file), m_out(a_out), m_stream_mask(a_stream_mask)
    , m_datefmt(a_time_fmt)
    , m_xchg(a_xchg)
    , m_symbol(a_symbol)
    , m_instr (a_instr)
    , m_max_depth(a_max_depth)
    , m_px_only(a_px_only)
    , m_tz_local(a_tz_local)
  {
    if ((m_stream_mask & (1 << int(StreamType::Quotes))) != 0)
      m_out << "#" << (m_tz_local ? "Local"      : "UTC") << "Time|Q|"
            << (m_xchg.empty()    ? "Xchg|"      : "")
            << (m_symbol.empty()  ? "Symbol|"    : "")
            << (m_instr.empty()   ? "Insrument|" : "")    << "Bids|Asks" << endl;
    if ((m_stream_mask & (1 << int(StreamType::Trade))) != 0)
      m_out << '#' << (m_tz_local ? "Local"      : "UTC") << "Time|T|"
            << (m_symbol.empty() ? "Symbol|"     : "")
            << (m_instr.empty()  ? "Insrument|"  : "")
            << "Side|Price|Qty|TradeID|OrderID" << endl;
  }

  bool operator()(SecondsSample const& a_sec) {
    return true;
  }

  bool operator()(QuoteSample<SDBFileIO::MAX_DEPTH(), int> const& a) {
    if ((m_stream_mask & (1 << int(StreamType::Quotes))) != 0) {
      auto time = m_tz_local
                ? (m_file.Time() + utxx::secs(m_file.Info().TZOffset()))
                : m_file.Time();
      m_out << utxx::timestamp::to_string(time, m_datefmt, true)
            << "|Q|";
      if (!m_xchg.empty())   m_out << m_xchg   << '|';
      if (!m_symbol.empty()) m_out << m_symbol << '|';
      if (!m_instr.empty())  m_out << m_instr  << '|';
      int i = 0;
      auto eb = a.EndBid();
      auto ea = a.EndAsk();
      for (auto p = a.BestBid(); p != eb && i < m_max_depth; a.NextBid(p), ++i) {
        m_out << (i ? " " : "");
        if (!m_px_only) m_out << p->m_qty << '@';
        m_out << std::setprecision(m_file.PxPrecision()) << std::fixed
              << (m_file.PxStep() * p->m_px);
      }
      m_out << '|';
      i = 0;
      for (auto p = a.BestAsk(); p != ea && i < m_max_depth; a.NextAsk(p), ++i) {
        m_out << (i ? " " : "");
        if (!m_px_only) m_out << p->m_qty << '@';
        m_out << std::setprecision(m_file.PxPrecision()) << std::fixed
              << (m_file.PxStep() * p->m_px);
      }
      m_out << endl;
    }
    return true;
  }

  bool operator()(TradeSample const& a_trade) {
    if ((m_stream_mask & (1 << int(StreamType::Trade))) != 0) {
      auto time = m_tz_local
                ? (m_file.Time() + m_file.Info().TZOffset()) : m_file.Time();
      m_out << utxx::timestamp::to_string(time, m_datefmt, m_tz_local)
            << "|T|";
      if (!m_symbol.empty()) m_out << m_symbol << '|';
      if (!m_instr.empty())  m_out << m_instr  << '|';
      m_out << ToChar(a_trade.Side()) << '|'
            << std::setprecision(m_file.PxPrecision()) << std::fixed
            << (m_file.PxStep() * a_trade.Price())
            << '|' << a_trade.Qty() << '|' << ToChar(a_trade.Aggr()) << '|';
      if (a_trade.HasTradeID()) m_out << a_trade.TradeID();
      m_out << '|';
      if (a_trade.HasOrderID()) m_out << a_trade.OrderID();
      m_out << endl;
    }
    return true;
  }

  template <typename T>
  bool operator()(T const& a_other) {
    UTXX_THROW_RUNTIME_ERROR("Unsupported stream type");
    return true;
  }

private:
  SDBFileIO&      m_file;
  ostream&          m_out;
  uint              m_stream_mask;
  utxx::stamp_type  m_datefmt;
  std::string       m_xchg;
  std::string       m_symbol;
  std::string       m_instr;
  int               m_max_depth;
  bool              m_px_only;
  bool              m_tz_local;
};

//------------------------------------------------------------------------------
int main(int argc, char* argv[])
//------------------------------------------------------------------------------
{
  if (argc < 2)
    Usage("Missing required option(s)");

  set_terminate(&UnhandledException);

  std::string filename;
  bool        info        = false;
  bool        fulldate    = false;
  bool        msec_time   = false;
  bool        quiet       = false;
  bool        with_symbol = false;
  bool        with_instr  = false;
  bool        with_xchg   = false;
  bool        px_only     = false;
  bool        tz_local    = false;
  int         debug       = 0;
  int         max_depth   = 100;
  std::string outfile;
  std::string sresol;
  int         resol       = 0;
  uint        stream_mask = 0;

  //----------------------------------------------------------------------------
  // Parse options
  //----------------------------------------------------------------------------
  utxx::opts_parser opts(argc, argv);
  while  (opts.next()) {
      if (opts.match("-f", "",            &filename)) continue;
      if (opts.match("-i", "--info",          &info)) continue;
      if (opts.match("-m", "--max-depth",&max_depth)) continue;
      if (opts.match("-d", "--debug"))   { debug++;   continue; }
      if (opts.match("-D", "--full-date", &fulldate)) continue;
      if (opts.match("-q", "--quiet",        &quiet)) continue;
      if (opts.match("-p", "--px-only",    &px_only)) continue;
      if (opts.match("-o", "--output",     &outfile)) continue;
      if (opts.match("-S", "--symbol", &with_symbol)) continue;
      if (opts.match("-X", "--xchg",     &with_xchg)) continue;
      if (opts.match("-I", "--instr",   &with_instr)) continue;
      if (opts.match("", "--tz-local",    &tz_local)) continue;
      if (opts.match("", "--tz-utc"))   { tz_local=0; continue; }
      if (opts.match("", "--msec",       &msec_time)) continue;
      if (opts.match("-Q", "--quotes")) {
        stream_mask |= 1u << int(StreamType::Quotes);
        continue;
      }
      if (opts.match("-T", "--trades")) {
        stream_mask |= 1u << int(StreamType::Trade);
        continue;
      }
      if (opts.match("-C", "--candles", &sresol))     continue;

      if (opts.is_help()) Usage();

      Usage(string("Invalid option: ") + opts());
  }

  if (filename.empty())                   Usage("Missing required option -f");
  if (!info) {
    if (!stream_mask && !sresol.empty())  Usage("Missing -Q|-T|-C");
    if (!sresol.empty()) {
      auto s =  utxx::fast_atoi<int, false>
                (sresol.c_str(), sresol.c_str()+sresol.size(), resol);
      if (!s || resol < 1 || resol > 60)
        UTXX_THROW_RUNTIME_ERROR("Invalid candle resolution requested: ", resol);

      if      (toupper(*s) == 'S') resol *= 1;
      else if (toupper(*s) == 'M') resol *= 60;
      else if (toupper(*s) == 'H') resol *= 3600;
      else UTXX_THROW_RUNTIME_ERROR("Invalid candle resolution: ", resol);
    }
  }

  auto file = fopen(filename.c_str(), "r");

  if (!file)
    UTXX_THROW_IO_ERROR(errno, "Cannot open file ", filename);

  long file_size = utxx::path::file_size(filename);

  //----------------------------------------------------------------------------
  // Create output stream
  //----------------------------------------------------------------------------
  ofstream out;
  std::streambuf* coutbuf = nullptr;

  // If output file not given, use stdout
  if (!outfile.empty() && outfile != "-") {
    auto dir = utxx::path::dirname(outfile);
    if (!utxx::path::create_directories(dir))
      UTXX_THROW_IO_ERROR(errno, "Cannot create directory ", dir);

    out.open(outfile, std::ios_base::out | std::ios_base::trunc);
    if (!out.is_open())
      UTXX_THROW_IO_ERROR(errno, "Cannot create output file ", outfile);
    coutbuf = cout.rdbuf();       // save old buf
    std::cout.rdbuf(out.rdbuf()); // assign new buf
  }

  // Optionally show progress bar if quiet option is not set and we are not
  // writing to stdout:
  std::shared_ptr<boost::progress_display> show_progress;

  if (!quiet) {
    cerr << filename << " -> " << outfile << endl;
    if (!outfile.empty())
      show_progress.reset(new boost::progress_display(file_size, cerr));
  }

  //----------------------------------------------------------------------------
  // Open SDB file for reading
  //----------------------------------------------------------------------------
  {
    SDBFileIO output(filename, debug);

    if (info) {
      if (!debug)
        output.Info().Print(std::cout);
    } else if (resol)
      output.PrintCandles(out, resol);
    else {
      auto date_fmt =  fulldate &&  msec_time ? utxx::DATE_TIME_WITH_MSEC
                    :  fulldate && !msec_time ? utxx::DATE_TIME_WITH_USEC
                    : !fulldate &&  msec_time ? utxx::TIME_WITH_MSEC
                    : utxx::TIME_WITH_USEC;

      Printer printer
      (
        output, cout, stream_mask, date_fmt,
        with_xchg   ? output.Info().Exchange()   : "",
        with_symbol ? output.Info().Symbol()     : "",
        with_instr  ? output.Info().Instrument() : "",
        tz_local, max_depth, px_only
      );
      output.Read(printer);
    }
  }

  if (coutbuf)
    std::cout.rdbuf(coutbuf);  // restore old buf

  return 0;
}

//------------------------------------------------------------------------------