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
#include <utxx/path.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/progress.hpp>
#include <iostream>
#include <fstream>

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
      if (opts.match("-f", "",         &filename)) continue;
      if (opts.match("-d", "--debug"))  { debug++; continue; }
      if (opts.match("-q", "--quiet",     &quiet)) continue;
      if (opts.match("-o", "--output",  &outfile)) continue;

      if (opts.is_help()) Usage();

      Usage(string("Invalid option: ") + opts());
  }

  if (filename.empty()) Usage("Missing required option -f");

  auto file = fopen(filename.c_str(), "r");

  if (!file)
    UTXX_THROW_IO_ERROR(errno, "Cannot open file ", outfile);

  long file_size = utxx::path::file_size(filename);

  // Outfile file stream
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

  // Open SecDB file for reading
  BaseSecDBFileIO<3> output(filename, debug);

  output.Close();

  return 0;
}