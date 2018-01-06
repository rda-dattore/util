#include <iostream>
#include <iomanip>
#include <string>
#include <list>
#include <regex>
#include <bfstream.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

struct Args {
  Args() : files(),verbose(false) {}

  std::list<std::string> files;
  bool verbose;
} args;
std::string myerror="";
std::string mywarning="";

void parse_args(int argc,char **argv)
{
  auto unix_args=unix_args_string(argc,argv,'!');
  auto sp=strutils::split(unix_args,"!");
  for (size_t n=0; n < sp.size(); ++n) {
    if (sp[n] == "-v") {
	args.verbose=true;
    }
    else if (std::regex_search(sp[n],std::regex("^-"))) {
	std::cerr << "Error: invalid flag " << sp[n] << std::endl;
	exit(1);
    }
    else {
	args.files.push_back(sp[n]);
    }
  }
}

int main(int argc,char **argv)
{
  if (argc < 2) {
    std::cerr << "usage: " << argv[0] << " [-v] files" << std::endl;
    std::cerr << std::endl;
    std::cerr << "function:  " << argv[0] << " provides information about a COS-blocked dataset(s)" << std::endl;
    std::cerr << std::endl;
    std::cerr << "options:" << std::endl;
    std::cerr << "  -v    provides additional information about the record sizes in the" << std::endl;
    std::cerr << "        dataset(s)" << std::endl;
    exit(1);
  }
  parse_args(argc,argv);
  const size_t BUF_LEN=100000;
  std::unique_ptr<unsigned char []> buf(new unsigned char[BUF_LEN]);
  auto eof_recs=0;
  int eof_min=0x7fffffff,eof_max=0;
  size_t type[]={0,0};
  for (const auto& file  : args.files) {
// open the COS-blocked dataset
    icstream istream;
    if (!istream.open(file.c_str())) {
	std::cerr << "Error opening " << file << std::endl;
	exit(1);
    }
    double eof_bytes=0.;
    auto eof_num=0;
    long long eod_bytes=0;
    auto eod_recs=0;
    auto eod_min=0x7fffffff;
    auto eod_max=0;
    std::cout << "\nProcessing dataset: " << file << std::endl;
// read to the end of the COS-blocked dataset
    int num_bytes;
    while ( (num_bytes=istream.read(buf.get(),BUF_LEN)) != craystream::eod) {
	if (num_bytes == bfstream::error) {
	  std::cerr << "\nRead error on record " << eof_recs+1 << " - may not be COS-blocked" << std::endl;
	  exit(1);
	}
	auto last_len=-1;
// read the current file
	auto last_written=false;
	do {
// handle a double EOF
	  if (num_bytes == craystream::eof) {
	    eof_min=0;
	    break;
	  }
	  ++eof_recs;
	  ++eod_recs;
	  eof_bytes+=num_bytes;
	  eod_bytes+=num_bytes;
	  if (num_bytes < eof_min) eof_min=num_bytes;
	  if (num_bytes > eof_max) eof_max=num_bytes;
	  if (num_bytes < eod_min) eod_min=num_bytes;
	  if (num_bytes > eod_max) eod_max=num_bytes;
	  last_written=false;
	  if (args.verbose && num_bytes != last_len) {
	    if (last_len == -1) {
		std::cout << "\n     Rec#    Bytes" << std::endl;
	    }
	    std::cout << "  " << std::setw(7) << eof_recs << " " << std::setw(7) << num_bytes << std::endl;
	    last_written=true;
	  }
	  last_len=num_bytes;
	  for (int n=0; n < num_bytes; ++n) {
	    if (buf[n] < 0x20 || buf[n] > 0x7e) {
		++type[0];
	    }
	    else {
		++type[1];
	    }
	  }
	} while ( (num_bytes=istream.read(buf.get(),BUF_LEN)) != craystream::eof);
	if (args.verbose && eof_recs > 0 && !last_written) {
	  std::cout << "  " << std::setw(7) << eof_recs << " " << std::setw(7) << last_len << std::endl;
	}
// summarize for the current file
	std::cout << "  EOF " << ++eof_num << ": Recs=" << eof_recs << " Min=" << eof_min << " Max=" << eof_max << " Avg=";
	if (eof_recs > 0) {
	  std::cout << lroundf(eof_bytes/eof_recs);
	}
	else {
	  std::cout << "0";
	}
	std::cout << " Bytes=" << static_cast<long long>(eof_bytes) << std::endl;
	if (eof_bytes > 0) {
	  if (type[0] == 0) {
	    std::cout << "       Type=ASCII" << std::endl;
	  }
	  else {
	    if (type[1] == 0) {
		std::cout << "       Type=Binary" << std::endl;
	    }
	    else {
		type[0]=type[0]*100./eof_bytes;
		type[1]=100-type[0];
		std::cout << "       Type=Binary or mixed -- Binary= " << type[0] << "% ASCII= " << type[1] << "%" << std::endl;
	    }
	  }
	}
	std::cout << std::endl;
// reset
	eof_bytes=0;
	eof_recs=0;
	eof_min=0x7fffffff;
	eof_max=0;
	type[0]=type[1]=0;
    }
    istream.close();
// summarize for the dataset
    std::cout << "  EOD. Min=" << eod_min << " Max=" << eod_max << " Records=" << eod_recs << " Bytes=" << eod_bytes << std::endl;
  }
  return 0;
}
