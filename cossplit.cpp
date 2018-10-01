#include <iostream>
#include <string>
#include <bfstream.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

struct Args {
  Args() : maxf(0x7fffffff),prefix(),input_file() {}

  size_t maxf;
  std::string prefix,input_file;
} args;
std::string myerror="";
std::string mywarning="";

void parse_args(int argc,char **argv,Args& args)
{
  auto unix_args=unixutils::unix_args_string(argc,argv,'!');
  auto sp=strutils::split(unix_args,"!");
  for (size_t n=0; n < sp.size()-1; ++n) {
    if (sp[n] == "-m") {
	args.maxf=std::stoi(sp[++n]);
    }
    else if (sp[n] == "-p") {
	args.prefix=sp[++n];
    }
    else {
	std::cerr << "Error: invalid option " << sp[n] << std::endl;
	exit(1);
    }
  }
  args.input_file=sp.back();
}

int main(int argc,char **argv)
{
  icstream istream;
  ocstream ostream;
  const size_t BUF_LEN=1000000;
  unsigned char buffer[BUF_LEN];
  int num_bytes;
  std::string output_file;
  size_t file_num=1;

  if (argc < 2) {
    std::cerr << "usage: " << argv[0] << " [-m maxFiles] [-p prefix] file" << std::endl;
    std::cerr << "\nfunction:  " << argv[0] << " splits multiple-file COS-blocked datasets into single-file" << std::endl;
    std::cerr << "           COS-blocked files" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "  -m maxFiles  specifies the maximum number of files to split, where \"maxf\" is" << std::endl;
    std::cerr << "               an integer - if \"maxf\" is omitted, all files will be split" << std::endl;
    std::cerr << std::endl;
    std::cerr << "  -p prefix    specifies the prefix for the single-file COS-blocked datasets -" << std::endl;
    std::cerr << "               if specified, the file names will have the form prefix.f00[n]," << std::endl;
    std::cerr << "               where n is an integer, starting with 1" << std::endl;
    std::cerr << "\nexamples:" << std::endl;
    std::cerr << "  cossplit mydataset" << std::endl;
    std::cerr << "     splits the multiple-file COS-blocked dataset \"mydataset\" into single" << std::endl;
    std::cerr << "     COS-blocked files with names of the form \"f00[n]\"" << std::endl;
    std::cerr << std::endl;
    std::cerr << "  cossplit -m 5 mydataset" << std::endl;
    std::cerr << "     splits only the first 5 files from \"mydataset\" into files f001 through" << std::endl;
    std::cerr << "     f005" << std::endl;
    std::cerr << std::endl;
    std::cerr << "  cossplit -p myprefix mydataset" << std::endl;
    std::cerr << "     splits \"mydataset\" into single files with names of the form" << std::endl;
    std::cerr << "     \"myprefix.f00[n]\"" << std::endl;
    exit(1);
  }
  parse_args(argc,argv,args);
  if (!istream.open(args.input_file.c_str())) {
    std::cerr << "Error opening " << args.input_file << std::endl;
    exit(1);
  }
  while (file_num <= args.maxf && (num_bytes=istream.read(buffer,BUF_LEN)) != craystream::eod) {
    if (!ostream.is_open()) {
	output_file="f"+strutils::ftos(file_num,3,0,'0');
	if (args.prefix.length() > 0) {
	  output_file=args.prefix+"."+output_file;
	}
	if (!ostream.open(output_file.c_str())) {
	  std::cerr << "Error opening " << output_file << std::endl;
	  exit(1);
	}
    }
    if (num_bytes == craystream::eof) {
	ostream.close();
	file_num++;
    }
    else {
	if (ostream.write(buffer,num_bytes) < 0) {
	  std::cerr << "Write error in " << output_file << " on record " << ostream.number_written()+1 << std::endl;
	}
    }
  }
}
