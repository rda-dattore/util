#include <iostream>
#include <bfstream.hpp>
#include <grid.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bits.hpp>
#include <tempfile.hpp>
#include <myerror.hpp>

struct ArgList {
  ArgList() : recln(0),conv(' '),big_endian(),cosfile(),non_cosfile() {}

  int recln;
  char conv;
  bool big_endian;
  std::string cosfile,non_cosfile;
} args;

std::string myerror="";
std::string mywarning="";

void parse_args(int argc,char **argv)
{
  auto next=1;
  if (argv[1][0] == '-') {
    args.conv=argv[next][1];
    if ((args.conv == 'B' || args.conv == 'b') && strutils::is_numeric(argv[next+1])) {
	args.recln=atoi(argv[++next]);
    }
    else if (args.conv == 'f') {
	++next;
	if (std::string(argv[next]) == "big") {
	  args.big_endian=true;
	}
	else if (std::string(argv[next]) == "little") {
	  args.big_endian=false;
	}
	else {
	  std::cerr << "Error: invalid endianness specified" << std::endl;
	  exit(1);
	}
    }
    if (args.conv == 'B' && args.recln == 0) {
	args.recln=32768;
    }
    ++next;
  }
  else {
    std::cerr << "Error: no convert flag specified" << std::endl;
    exit(1);
  }
  if (next >= argc) {
    std::cerr << "Error: no COS-blocked filename given" << std::endl;
    exit(1);
  }
  args.cosfile=argv[next++];
  if (next < argc)
    args.non_cosfile=argv[next];
}

void cos_to_6_bit()
{
  icstream istream;
  if (!istream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  TempFile *tfile=nullptr;
  if (args.non_cosfile.length() == 0) {
    tfile=new TempFile(".");
    args.non_cosfile=tfile->name();
  }
  std::ofstream ofs(args.non_cosfile.c_str());
  if (!ofs.is_open()) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  const int BUF_LEN=500000;
  std::unique_ptr<unsigned char []> buffer(new unsigned char[BUF_LEN]);
  std::unique_ptr<unsigned char []> buffer2(new unsigned char[BUF_LEN]);
  int num_remain=0;
  unsigned char buf_remain = 0x0;
  int num_written=0;
  int num_bytes;
  while ( (num_bytes=istream.read(buffer.get(),BUF_LEN)) > 0) {
    if (num_bytes == BUF_LEN) {
	std::cerr << "Warning: buffer not large enough on record " << istream.number_read() << std::endl;
    }
    auto num6=num_bytes*8/6;
    num6*=6;
    auto num8=num_bytes*8;
    if (num_remain == 0) {
	if (num6 == num8) {
	  ofs.write(reinterpret_cast<char *>(buffer.get()),num_bytes);
	  num_remain=0;
	}
	else {
	  ofs.write(reinterpret_cast<char *>(buffer.get()),num6/8);
	  num_remain=num6 % 8;
	  bits::get(buffer.get(),buf_remain,(num6/8)*8,num_remain);
	}
    }
    else {
	bits::set(buffer2.get(),buf_remain,0,num_remain);
	bits::set(buffer2.get(),buffer.get(),num_remain,8,0,num_bytes);
	num6+=num_remain;
	if (num6 == num8) {
	  ofs.write(reinterpret_cast<char *>(buffer2.get()),num_bytes);
	  num_remain=0;
	}
	else {
std::cerr << "Error: can't get past here" << std::endl;
exit(1);
	}
    }
    ++num_written;
  }
  if (num_remain > 0) {
    bits::set(buffer2.get(),buf_remain,0,num_remain);
    bits::set(buffer2.get(),0,num_remain,8-num_remain);
    ofs.write(reinterpret_cast<char *>(buffer2.get()),1);
  }
  ofs.close();
  if (tfile != nullptr) {
    system(("mv "+tfile->name()+" "+args.cosfile).c_str());
  }
  std::cout << "\n  COS-blocked records read: " << istream.number_read() << std::endl;
  std::cout << "  Binary records written: " << num_written << std::endl;
}

void cos_to_binary()
{
  icstream istream;
  if (!istream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << " for input" << std::endl;
    exit(1);
  }
  TempFile *tfile=nullptr;
  if (args.non_cosfile.length() == 0) {
    tfile=new TempFile(".");
    args.non_cosfile=tfile->name();
  }
  std::ofstream ofs(args.non_cosfile.c_str());
  if (!ofs.is_open()) {
    std::cerr << "Error opening " << args.non_cosfile << " for output" << std::endl;
    exit(1);
  }
  std::unique_ptr<unsigned char []> blank(new unsigned char[args.recln]);
  for (int n=0; n < args.recln; ++n) {
    blank[n]=0;
  }
  int BUF_LEN = 0;
  std::unique_ptr<unsigned char []> buffer;
  int num_bytes;
  size_t num_written=0;
  while ( (num_bytes = istream.peek()) > 0) {
    if (num_bytes > BUF_LEN) {
      BUF_LEN = num_bytes;
      buffer.reset(new unsigned char[BUF_LEN]);
    }
    num_bytes = istream.read(buffer.get(), BUF_LEN);
    if (args.recln == 0) {
	ofs.write(reinterpret_cast<char *>(buffer.get()),num_bytes);
    }
    else {
	if (args.recln > num_bytes) {
	  ofs.write(reinterpret_cast<char *>(buffer.get()),num_bytes);
	  auto fill=args.recln-num_bytes;
	  ofs.write(reinterpret_cast<char *>(blank.get()),fill);
	}
	else {
	  ofs.write(reinterpret_cast<char *>(buffer.get()),args.recln);
	}
    }
    ++num_written;
  }
  if (tfile != nullptr) {
    system(("mv "+tfile->name()+" "+args.cosfile).c_str());
  }
  std::cout << "\n  COS-blocked records read: " << istream.number_read() << std::endl;
  std::cout << "  Binary records written: " << num_written << std::endl;
}

void binary_to_cos()
{
  FILE *fp;
  if ( (fp=fopen(args.non_cosfile.c_str(),"r")) == NULL) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  ocstream ostream;
  if (!ostream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  std::unique_ptr<unsigned char []> buffer(new unsigned char[args.recln]);
  size_t len;
  size_t num_read=0,num_written=0;
  while ( (len=fread(buffer.get(),1,args.recln,fp)) > 0) {
    ++num_read;
    ostream.write(buffer.get(),len);
    ++num_written;
  }
  ostream.close();
  std::cout << "\n  Binary records read: " << num_read << std::endl;
  std::cout << "  COS-blocked records written: " << num_written << std::endl;
}

void cos_to_unix()
{
  icstream istream;
  if (!istream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  TempFile *tfile=nullptr;
  if (args.non_cosfile.length() == 0) {
    tfile=new TempFile(".");
    args.non_cosfile=tfile->name();
  }
  FILE *fp;
  if ( (fp=fopen(args.non_cosfile.c_str(),"w")) == NULL) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  const int BUF_LEN=500000;
  std::unique_ptr<unsigned char []> buffer(new unsigned char[BUF_LEN]);
  int num_bytes;
  size_t num_written=0;
  while ( (num_bytes=istream.read(buffer.get(),BUF_LEN)) >= 0) {
    if (num_bytes+1 == BUF_LEN)
	std::cerr << "Warning: buffer not large enough on record " << istream.number_read() << std::endl;

    buffer[num_bytes]=0xa;
    fwrite(buffer.get(),1,num_bytes+1,fp);

    ++num_written;
  }
  fclose(fp);
  if (tfile != nullptr) {
    system(("mv "+tfile->name()+" "+args.cosfile).c_str());
  }
  std::cout << "\n  COS-blocked records read: " << istream.number_read() << std::endl;
  std::cout << "  Unix records written: " << num_written << std::endl;
}

void unix_to_cos()
{
  std::ifstream ifs;
  ifs.open(args.non_cosfile.c_str());
  if (!ifs.is_open()) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  ocstream ostream;
  if (!ostream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  const size_t BUF_LEN=32768;
  std::unique_ptr<char []> buffer(new char[BUF_LEN]);
  size_t num_read=0,num_written=0;
  ifs.getline(buffer.get(),BUF_LEN);
  while (!ifs.eof()) {
    ++num_read;
    ostream.write(reinterpret_cast<unsigned char *>(buffer.get()),ifs.gcount()-1);
    ++num_written;
    ifs.getline(buffer.get(),BUF_LEN);
  }
  ostream.close();
  std::cout << "\n  UNIX records read: " << num_read << std::endl;
  std::cout << "  COS-blocked records written: " << num_written << std::endl;
}

void cos_to_f77()
{
  auto sys_is_big_endian=unixutils::system_is_big_endian();
  icstream istream;
  if (!istream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  TempFile *tfile=nullptr;
  if (args.non_cosfile.length() == 0) {
    tfile=new TempFile(".");
    args.non_cosfile=tfile->name();
  }
  std::ofstream ostream(args.non_cosfile.c_str());
  if (!ostream.is_open()) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  const int BUF_LEN=500000;
  std::unique_ptr<unsigned char []> buffer(new unsigned char[BUF_LEN]);
  size_t num_written=0;
  int num_bytes;
  char nbuf[4];
  while ( (num_bytes=istream.read(buffer.get(),BUF_LEN)) != bfstream::eof) {
    if (num_bytes == BUF_LEN) {
	std::cerr << "Error: buffer not large enough" << std::endl;
	exit(1);
    }
    if (sys_is_big_endian == args.big_endian) {
	ostream.write(reinterpret_cast<char *>(&num_bytes),4);
    }
    else {
	bits::set(nbuf,num_bytes,0,32);
	ostream.write(nbuf,4);
    }
    ostream.write(reinterpret_cast<char *>(buffer.get()),num_bytes);
    if (sys_is_big_endian == args.big_endian) {
	ostream.write(reinterpret_cast<char *>(&num_bytes),4);
    }
    else {
	bits::set(nbuf,num_bytes,0,32);
	ostream.write(nbuf,4);
    }
    ++num_written;
  }
  ostream.close();
  if (tfile != nullptr) {
    system(("mv "+tfile->name()+" "+args.cosfile).c_str());
  }
  std::cout << "\n  COS-blocked records read: " << istream.number_read() << std::endl;
  std::cout << "  F77 records written: " << num_written << std::endl;
}

void f77_to_cos()
{
  if77stream istream;
  if (!istream.open(args.non_cosfile.c_str())) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  ocstream ostream;
  if (!ostream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  const int BUF_LEN=500000;
  std::unique_ptr<unsigned char []> buffer(new unsigned char[BUF_LEN+8]);
  int num_bytes;
  size_t num_written=0;
  while ( (num_bytes=istream.read(&buffer[4],BUF_LEN)) != bfstream::eof) {
    if (num_bytes == BUF_LEN) {
	std::cerr << "Error: buffer not large enough" << std::endl;
	exit(1);
    }

    bits::set(buffer.get(),num_bytes,0,32);
    bits::set(buffer.get(),num_bytes,(num_bytes+4)*8,32);
    ostream.write(buffer.get(),num_bytes+8);
    ++num_written;
  }
  ostream.close();
  std::cout << "\n  F77 records read: " << istream.number_read() << std::endl;
  std::cout << "  COS-blocked records written: " << num_written << std::endl;
}

void grib_to_cos()
{
  InputGRIBStream grid_stream;
  if (!grid_stream.open(args.non_cosfile.c_str())) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  ocstream ostream;
  if (!ostream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  const size_t BUF_LEN=5000000;
  std::unique_ptr<unsigned char []> buffer(new unsigned char[BUF_LEN]);
  size_t num_written=0;
  int num_bytes;
  while ( (num_bytes=grid_stream.read(buffer.get(),BUF_LEN)) != bfstream::eof) {
    ostream.write(buffer.get(),num_bytes);
    ++num_written;
  }
  ostream.close();
  std::cout << "\n  GRIB grids read: " << grid_stream.number_read() << std::endl;
  std::cout << "  COS-blocked records written: " << num_written << std::endl;
}

void cos_to_rptout()
{
  icstream istream;
  if (!istream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  TempFile *tfile=nullptr;
  if (args.non_cosfile.length() == 0) {
    tfile=new TempFile(".");
    args.non_cosfile=tfile->name();
  }
  orstream ostream;
  if (!ostream.open(args.non_cosfile.c_str())) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  const int BUF_LEN=8000;
  std::unique_ptr<unsigned char []> buffer(new unsigned char[BUF_LEN]);
  int num_bytes;
  while ( (num_bytes=istream.read(buffer.get(),BUF_LEN)) > 0) {
    if (num_bytes == BUF_LEN) {
	std::cerr << "Warning: buffer not large enough on record " << istream.number_read() << std::endl;
    }
    size_t block_len;
    bits::get(buffer.get(),block_len,0,12);
    ostream.write(buffer.get(),block_len);
  }
  ostream.close();
  if (tfile != nullptr) {
    system(("mv "+tfile->name()+" "+args.cosfile).c_str());
  }
  std::cout << "\n  COS-blocked records read: " << istream.number_read() << std::endl;
  std::cout << "  Binary Rptout records written: " << ostream.number_written() << std::endl;
}

void rptout_to_cos()
{
  FILE *fp;
  if ( (fp=fopen(args.non_cosfile.c_str(),"r")) == NULL) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  ocstream ostream;
  if (!ostream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  std::unique_ptr<unsigned char []> buffer(new unsigned char[8000]);
  size_t num_read=0,num_written=0;
  while (fread(buffer.get(),1,8,fp) > 0) {
    size_t num_bytes;
    bits::get(buffer.get(),num_bytes,32,32);
    num_bytes*=8;
    fread(&buffer[8],1,num_bytes-8,fp);
    ++num_read;
    ostream.write(buffer.get(),num_bytes);
    ++num_written;
  }
  ostream.close();
  std::cout << "\n  Rptout blocks read: " << num_read << std::endl;
  std::cout << "  COS-blocked records written: " << num_written << std::endl;
}

void cos_to_vbs()
{
  icstream istream;
  if (!istream.open(args.cosfile.c_str())) {
    std::cerr << "Error opening " << args.cosfile << std::endl;
    exit(1);
  }
  TempFile *tfile=nullptr;
  if (args.non_cosfile.length() == 0) {
    tfile=new TempFile(".");
    args.non_cosfile=tfile->name();
  }
  FILE *fp;
  if ( (fp=fopen(args.non_cosfile.c_str(),"w")) == NULL) {
    std::cerr << "Error opening " << args.non_cosfile << std::endl;
    exit(1);
  }
  const int BUF_LEN=500000;
  std::unique_ptr<unsigned char []> buffer(new unsigned char[BUF_LEN]);
  int num_written=0;
  int num_bytes;
  while ( (num_bytes=istream.read(buffer.get(),BUF_LEN)) > 0) {
    if (num_bytes == BUF_LEN) {
	std::cerr << "Warning: buffer not large enough on record " << istream.number_read() << std::endl;
    }
    size_t block_len;
    bits::get(buffer.get(),block_len,0,16);
    fwrite(buffer.get(),1,block_len,fp);
    ++num_written;
  }
  fclose(fp);
  if (tfile != nullptr) {
    system(("mv "+tfile->name()+" "+args.cosfile).c_str());
  }
  std::cout << "\n  COS-blocked records read: " << istream.number_read() << std::endl;
  std::cout << "  Binary VBS records written: " << num_written << std::endl;
}

int main(int argc,char **argv)
{
  if (argc < 3) {
    std::cerr << "usage: " << argv[0] << " flag cosfile {non-cosfile}" << std::endl;
    std::cerr << "\nfunction: to convert files between COS-blocked and other formats" << std::endl;
    std::cerr << std::endl;
    std::cerr << "a conversion flag is required (choose one):" << std::endl;
    std::cerr << "-6          convert COS-blocked binary to 6-bit stream" << std::endl;
    std::cerr << "-b <recln>  convert COS-blocked binary to plain binary, specifying an optional" << std::endl;
    std::cerr << "              <recln> to force the record length" << std::endl;
    std::cerr << "-B <recln>  convert plain binary to COS-blocked binary, specifying an optional" << std::endl;
    std::cerr << "              <recln> for the binary record length (default is 32768)" << std::endl;
    std::cerr << "-c          convert COS-blocked ASCII to UNIX ASCII" << std::endl;
    std::cerr << "-C          convert UNIX ASCII to COS-blocked ASCII" << std::endl;
    std::cerr << "-f <endian> convert COS-blocked binary to F77 <endian>-endian binary, where" << std::endl;
    std::cerr << "              <endian> is \"big\" or \"little\"" << std::endl;
    std::cerr << "-F          convert F77 binary to COS-blocked F77 binary" << std::endl;
    std::cerr << "-G          convert plain binary GRIB to COS-blocked GRIB" << std::endl;
    std::cerr << "-r          convert COS-blocked rptout to binary rptout" << std::endl;
    std::cerr << "-R          convert binary rptout to COS-blocked rptout" << std::endl;
    std::cerr << "-v          convert COS-blocked IBM VBS to binary IBM VBS" << std::endl;
    std::cerr << std::endl;
    std::cerr << "file name inclusion:" << std::endl;
    std::cerr << "cosfile: (the name of the COS-blocked file) is always required" << std::endl;
    std::cerr << "non-cosfile: (the name of the non-COS-blocked file) is:" << std::endl;
    std::cerr << "  required when ADDING COS-blocking" << std::endl;
    std::cerr << "  optional when REMOVING COS-blocking (if not included, cosfile will be" << std::endl;
    std::cerr << "    overwritten)" << std::endl;
    exit(1);
  }
  parse_args(argc,argv);
  switch (args.conv) {
    case '6':
    {
	cos_to_6_bit();
	break;
    }
    case 'b':
    {
	cos_to_binary();
	break;
    }
    case 'B':
    {
	binary_to_cos();
	break;
    }
    case 'c':
    {
	cos_to_unix();
	break;
    }
    case 'C':
    {
	unix_to_cos();
	break;
    }
    case 'f':
    {
	cos_to_f77();
	break;
    }
    case 'F':
    {
	f77_to_cos();
	break;
    }
    case 'G':
    {
	grib_to_cos();
	break;
    }
    case 'r':
    {
	cos_to_rptout();
	break;
    }
    case 'R':
    {
	rptout_to_cos();
	break;
    }
    case 'v':
    {
	cos_to_vbs();
	break;
    }
    default:
    {
	std::cerr << "Error: conversion flag -" << args.conv << " not supported" << std::endl;
	exit(1);
    }
  }
}
