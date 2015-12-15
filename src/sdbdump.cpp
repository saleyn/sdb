// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file  sdbdump.hpp
//------------------------------------------------------------------------------
/// \brief SecDB file reader
///
/// \see https://github.com/saleyn/secdb/wiki/Data-Format
//------------------------------------------------------------------------------
// Copyright (c) 2015 Omnibius, LLC
// Author:  Serge Aleynikov <saleyn@gmail.com>
// Created: 2015-10-15
//------------------------------------------------------------------------------
#include <secdb/secdb.hpp>
#include <utxx/get_option.hpp>
#include <utxx/path.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/progress.hpp>
#include <iostream>
#include <fstream>

using namespace std;
using namespace secdb;

using SecDBFileIO = BaseSecDBFileIO<10>;

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
void Usage(std::string const& a_text = "")
{
  if (!a_text.empty())
    cout << a_text << endl << endl;

  cout << "SecDB file reader\n"
       << "Copyright (c) 2015 Omnibius, LLC\n\n"
       << "Usage: " << utxx::path::program::name()
       << " -f MDFilename [-o|-O OutputFile] [-d] [-q]\n"
       << "\nOptions:\n"
       << "  -f MDFilename         - filename with KRX market data\n"
       << "  -o|--output OutFile   - output filename (def: stdout)\n"
       << "  -d                    - enable debug printouts\n"
       << "  -q                    - quiet mode (don't display a progress bar)\n"
       << "  -D                    - include YYYYMMDD in timestamp output\n"
       << "  -S|--symbol           - include symbol name in the output\n"
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
    SecDBFileIO& a_file, ostream& a_out, uint a_stream_mask,
    bool a_fulldate, std::string const& a_symbol, std::string const& a_instr
  )
    : m_file(a_file), m_out(a_out), m_stream_mask(a_stream_mask)
    , m_datefmt(a_fulldate ? utxx::DATE_TIME_WITH_USEC : utxx::TIME_WITH_USEC)
    , m_symbol(a_symbol)
    , m_instr (a_instr)
  {
    if ((m_stream_mask & (1 << int(StreamType::Quotes))) != 0)
      m_out << "#Time|M|" << (m_symbol.empty() ? "Symbol|" : "")
            << (m_instr.empty() ? "Insrument|" : "") << "Bids|Asks" << endl;
    if ((m_stream_mask & (1 << int(StreamType::Trade))) != 0)
      m_out << "#Time|T|"  << (m_symbol.empty() ? "Symbol|" : "")
            << (m_instr.empty() ? "Insrument|" : "")
            << "Side|Price|Qty|TradeID|OrderID" << endl;
  }

  bool operator()(SecondsSample const& a_sec) {
    return true;
  }

  bool operator()(QuoteSample<SecDBFileIO::MAX_DEPTH(), int> const& a) {
    if ((m_stream_mask & (1 << int(StreamType::Quotes))) != 0) {
      m_out << utxx::timestamp::to_string(m_file.Time(), m_datefmt, true)
            << "|M|";
      if (!m_symbol.empty()) m_out << m_symbol << '|';
      if (!m_instr.empty())  m_out << m_instr  << '|';
      int i = 0;
      for (auto p = a.BestBid(), e = a.EndBid(); p != e; a.NextBid(p), ++i)
        m_out << (i ? " " : "")
              << std::setprecision(m_file.PxPrecision()) << std::fixed
              << p->m_qty << '@' << (m_file.PxStep() * p->m_px);
      m_out << '|';
      i = 0;
      for (auto p = a.BestAsk(), e = a.EndAsk(); p != e; a.NextAsk(p), ++i)
        m_out << (i ? " " : "")
              << std::setprecision(m_file.PxPrecision()) << std::fixed
              << p->m_qty << '@' << (m_file.PxStep() * p->m_px);
      m_out << endl;
    }
    return true;
  }

  bool operator()(TradeSample const& a_trade) {
    if ((m_stream_mask & (1 << int(StreamType::Trade))) != 0) {
      m_out << utxx::timestamp::to_string(m_file.Time(), m_datefmt, true)
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
  SecDBFileIO&      m_file;
  ostream&          m_out;
  uint              m_stream_mask;
  utxx::stamp_type  m_datefmt;
  std::string       m_symbol;
  std::string       m_instr;
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
  bool        quiet       = false;
  bool        with_symbol = false;
  bool        with_instr  = false;
  int         debug       = 0;
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
      if (opts.match("-d", "--debug"))  { debug++;    continue; }
      if (opts.match("-D", "--full-date", &fulldate)) continue;
      if (opts.match("-q", "--quiet",        &quiet)) continue;
      if (opts.match("-o", "--output",     &outfile)) continue;
      if (opts.match("-S", "--symbol", &with_symbol)) continue;
      if (opts.match("-I", "--instr",   &with_instr)) continue;
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

  // If output file not given, use stdout
  if (!outfile.empty()) {
    auto dir = utxx::path::dirname(outfile);
    try   { boost::filesystem::create_directories(dir); }
    catch ( std::exception const& e ) {
      UTXX_THROW_IO_ERROR(errno, "Cannot create directory ", dir);
    }
    out.open(outfile, std::ios_base::out | std::ios_base::trunc);
    if (!out.is_open())
      UTXX_THROW_IO_ERROR(errno, "Cannot create output file ", outfile);
    std::cout.rdbuf(out.rdbuf());
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
  // Open SecDB file for reading
  //----------------------------------------------------------------------------
  SecDBFileIO output(filename, debug);

  if (info) {
    if (!debug)
      output.Info().Print(std::cout);
  } else if (resol)
    output.PrintCandles(out, resol);
  else {
    Printer printer
    (
      output, out, stream_mask, fulldate,
      with_symbol ? output.Info().Symbol() : "",
      with_instr  ? output.Info().Instrument() : ""
    );
    output.Read(printer);
  }

  output.Close();

  return 0;
}

//------------------------------------------------------------------------------
