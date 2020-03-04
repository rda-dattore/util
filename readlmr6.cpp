/*
** This program decodes the ICOADS LMR6 (Long Marine Record version 6) format
** and prints selected fields from each record in the file. The LMR6 format is
** described here:
** https://icoads.noaa.gov/e-doc/lmr
** You will need to reference this document and modify this program to decode
** and print fields that are not already handled.
**
** compile with:
** g++ -o readlmr6 readlmr6.cpp
*/

#include <iostream>
#include <iomanip>
#include <fstream>
#include <memory>
#include <cmath>

namespace bits {

template <class MaskType>
inline void create_mask(MaskType& mask,size_t size)
{
  mask=1;
  for (size_t n=1; n < size; n++)
    (mask<<=1)++;
}

template <class BufType,class LocType>
inline void extract(const BufType *buf,LocType *loc,size_t off,const size_t bits,size_t skip = 0,const size_t num = 1)
{
// create a mask to use when right-shifting (necessary because different
// compilers do different things when right-shifting a signed bit-field)
  auto loc_size=sizeof(LocType)*8;
  auto buf_size=sizeof(BufType)*8;
  if (bits > loc_size) {
    std::cerr << "Error: trying to unpack " << bits << " bits into a " << loc_size << "-bit location" << std::endl;
    exit(1);
  }
  else {
    BufType bmask;
    create_mask(bmask,buf_size);
    skip+=bits;
    if (loc_size <= buf_size) {
	for (size_t n=0; n < num; ++n) {
// skip to the word containing the packed field
	  auto wskip=off/buf_size;
// right shift the bits in the packed buffer word to eliminate unneeded bits
	  int rshift=buf_size-(off % buf_size)-bits;
// check for a packed field spanning two words
	  if (rshift < 0) {
	    loc[n]=(buf[wskip]<<-rshift);
	    loc[n]+=(buf[++wskip]>>(buf_size+rshift))&~(bmask<<-rshift);
	  }
	  else {
	    loc[n]=(buf[wskip]>>rshift);
	  }
// remove any unneeded leading bits
	  if (bits != buf_size) loc[n]&=~(bmask<<bits);
	  off+=skip;
	}
    }
    else {
	LocType lmask;
	create_mask(lmask,loc_size);
// get difference in bits between word size of packed buffer and word size of
// unpacked location
	for (size_t n=0; n < num; ++n) {
// skip to the word containing the packed field
	  auto wskip=off/buf_size;
// right shift the bits in the packed buffer word to eliminate unneeded bits
	  int rshift=buf_size-(off % buf_size)-bits;
// check for a packed field spanning multiple words
	  if (rshift < 0) {
	    LocType not_bmask;
	    create_mask(not_bmask,loc_size-buf_size);
	    not_bmask=~(not_bmask<<buf_size);
	    loc[n]=0;
	    while (rshift < 0) {
		auto temp=buf[wskip++]&not_bmask;
		loc[n]+=(temp<<-rshift);
		rshift+=buf_size;
	    }
	    if (rshift != 0) {
		loc[n]+=(buf[wskip]>>rshift)&~(bmask<<(buf_size-rshift));
	    }
	    else {
		loc[n]+=buf[wskip]&not_bmask;
	    }
	  }
	  else {
	    loc[n]=(buf[wskip]>>rshift);
	  }
// remove any unneeded leading bits
	  if (bits != loc_size) loc[n]&=~(lmask<<bits);
	  off+=skip;
	}
    }
  }
}

template <class BufType,class LocType>
void get(const BufType *buf,LocType *loc,const size_t off,const size_t bits,const size_t skip,const size_t num)
{
  if (bits == 0) {
// no work to do
    return;
  }
  extract(buf,loc,off,bits,skip,num);
}

template <class BufType,class LocType>
void get(const BufType *buf,LocType& loc,const size_t off,const size_t bits)
{
  if (bits == 0) {
// no work to do
    return;
  }
  extract(buf,&loc,off,bits);
}

} // end namespace bits

namespace dateutils {

bool is_leap_year(size_t year,std::string calendar = "")
{
  if (calendar.empty() || calendar == "standard" || calendar == "gregorian" || calendar == "proleptic_gregorian") {
    if ( (year % 4) == 0 && ( (year % 100 != 0) || (year % 400) == 0)) {
	return true;
    }
    else {
	return false;
    }
  }
  else if (calendar == "366_day" || calendar == "all_leap") {
    return true;
  }
  else {
    return false;
  }
}

} // end namespace dateutils

class DateTime
{
public:
  static const size_t days_in_month_noleap[13];
  static const size_t days_in_month_360_day[13];
  DateTime() : year_(0),month_(0),day_(0),hour_(0),minute_(0),second_(0),utc_offset_(0),weekday_(-1) {}
  DateTime(long long YYYYMMDDHHMMSS) : DateTime() {
    set(YYYYMMDDHHMMSS);
  }
  int years_since(const DateTime& reference) const
  {
    if (*this < reference) {
	return -1;
    }
    auto years=this->year_-reference.year_;
    if (this->month_ < reference.month_) {
	--years;
    }
    else if (this->month_ == reference.month_) {
	if (this->day_ < reference.day_ || (this->day_ == reference.day_ && this->time() < reference.time())) {
	  years--;
	}
    }
    return years;
  }
  int months_since(const DateTime& reference) const
  {
    if (*this < reference) {
	return -1;
    }
    int months=12*this->years_since(reference);
    months+=(this->month_-reference.month_);
    if (this->month_ < reference.month_ || (this->month_ == reference.month_ && (this->day_ < reference.day_ || (this->day_ == reference.day_ && this->time() < reference.time())))) {
	months+=12;
    }
    if (this->day_ < reference.day_ || (this->day_ == reference.day_ && this->time() < reference.time())) {
	--months;
    }
    return months;
  }
  int days_since(const DateTime& reference,std::string calendar = "") const
  {
    if (*this < reference) {
	return -1;
    }
    if (*this == reference) {
	return 0;
    }
    size_t days,*num_days;
    if (calendar == "360_day") {
	days=360*this->years_since(reference);
	num_days=const_cast<size_t *>(days_in_month_360_day);
    }
    else {
	days=365*this->years_since(reference);
	num_days=const_cast<size_t *>(days_in_month_noleap);
    }
    if (days > 0 && dateutils::is_leap_year(reference.year_,calendar) && reference.month_ <= 2) {
	++days;
    }
    for (short n=reference.year_+1; n < this->year_; ++n) {
	if (dateutils::is_leap_year(n,calendar)) {
	  ++days;
	}
    }
    if (dateutils::is_leap_year(this->year_,calendar) && this->month_ > 2 && (reference.year_ < this->year_ || reference.month_ <= 2)) {
	++days;
    }
    days+=(this->day_-reference.day_);
    if (this->months_since(reference) > 0) {
	if (this->month_ < reference.month_ || (this->month_ == reference.month_ && (this->day_ < reference.day_ || (this->day_ == reference.day_ && this->time() < reference.time())))) {
	  for (short n=reference.month_; n <= 12; ++n) {
	    days+=num_days[n];
	  }
	  for (short n=1; n < this->month_; ++n) {
	    days+=num_days[n];
	  }
	}
	else {
	  for (short n=reference.month_; n < this->month_; ++n) {
	    days+=num_days[n];
	  }
	}
    }
    else if (this->day_ < reference.day_ || (days == 0 && this->month_ != reference.month_)) {
	days+=num_days[reference.month_];
    }
    if (this->time() < reference.time()) {
	--days;
    }
    return days;
  }
  void set_time(size_t hhmmss)
  {
    second_=hhmmss % 100;
    hhmmss/=100;
    minute_=hhmmss % 100;
    hhmmss/=100;
    hour_=hhmmss;
  }
  void set_utc_offset(short utc_offset_as_hhmm)
  {
    if (utc_offset_as_hhmm < -2400 || (utc_offset_as_hhmm > -100 && utc_offset_as_hhmm < 100 && utc_offset_as_hhmm != 0) || utc_offset_as_hhmm > 2400) {
	std::cerr << "Error: bad offset from UTC specified: " << utc_offset_as_hhmm << std::endl;
	exit(1);
    }
    utc_offset_=utc_offset_as_hhmm;
  }
  void set(short year,short month = 0,short day = 0,size_t hhmmss = 0,short utc_offset_as_hhmm = 0) {
    DateTime base;
    base.year_=1970;
    base.month_=1;
    base.day_=4;
    year_=year;
    month_=month;
    day_=day;
    if (*this == base) {
	weekday_=0;
    }
    else if (*this > base) {
	weekday_=(days_since(base) % 7);
    }
    else {
	weekday_=((7-(base.days_since(*this) % 7)) % 7);
    }
    set_time(hhmmss);
    set_utc_offset(utc_offset_as_hhmm);
  }
  void set(long long YYYYMMDDHHMMSS)
  {
    size_t time=YYYYMMDDHHMMSS % 1000000;
    YYYYMMDDHHMMSS/=1000000;
    short day;
    if (YYYYMMDDHHMMSS > 0) {
	day=YYYYMMDDHHMMSS % 100;
	YYYYMMDDHHMMSS/=100;
    }
    else {
	day=0;
    }
    short month;
    if (YYYYMMDDHHMMSS > 0) {
	month=YYYYMMDDHHMMSS % 100;
	YYYYMMDDHHMMSS/=100;
    }
    else {
	month=0;
    }
    short year;
    if (YYYYMMDDHHMMSS > 0) {
	year=YYYYMMDDHHMMSS;
    }
    else {
	year=0;
    }
    set(year,month,day,time);
  }
  size_t time() const
  {
     return hour_*10000+minute_*100+second_;
  }
  std::string to_string(const char *format) const
  {
    std::stringstream dt_str;
    dt_str << std::setfill('0') << std::setw(4) << year_ << "-" << std::setw(2) << month_ << "-" << std::setw(2) << day_ << " " << std::setw(2) << hour_ << ":" << std::setw(2) << minute_ << ":" << std::setw(2) << second_ << " ";
    if (utc_offset_ < 0) {
	if (utc_offset_ > -2400) {
	  dt_str << "-" << std::setw(4) << abs(utc_offset_);
	}
	else if (utc_offset_ == -2400) {
	  dt_str << "LST  ";
	}
    }
    else {
	if (utc_offset_ < 2400) {
	  dt_str << "+" << std::setw(4) << abs(utc_offset_);
	}
	else if (utc_offset_ == 2400) {
	  dt_str << "LT   ";
	}
    }
    return dt_str.str();
  }
  friend bool operator>(const DateTime& source1,const DateTime& source2)
  {
    if (source1.year_ > source2.year_) {
	return true;
    }
    else if (source1.year_ == source2.year_) {
	if (source1.month_ > source2.month_) {
	  return true;
	}
	else if (source1.month_ == source2.month_) {
	  if (source1.day_ > source2.day_) {
	    return true;
	  }
	  else if (source1.day_ == source2.day_) {
	    if (source1.hour_ > source2.hour_) {
		return true;
	    }
	    else if (source1.hour_ == source2.hour_) {
		if (source1.minute_ > source2.minute_) {
		  return true;
		}
		else if (source1.minute_ == source2.minute_) {
		  if (source1.second_ > source2.second_) {
		    return true;
		  }
		}
	    }
	  }
	}
    }
    return false;
  }
  friend bool operator!=(const DateTime& source1,const DateTime& source2)
  {
    if (source1.year_ != source2.year_) {
	return true;
    }
    if (source1.month_ != source2.month_) {
	return true;
    }
    if (source1.day_ != source2.day_) {
	return true;
    }
    if (source1.hour_ != source2.hour_) {
	return true;
    }
    if (source1.minute_ != source2.minute_) {
	return true;
    }
    if (source1.second_ != source2.second_) {
	return true;
    }
    return false;
  }
  friend bool operator==(const DateTime& source1,const DateTime& source2)
  { 
    return !(source1 != source2);
  }
  friend bool operator<(const DateTime& source1,const DateTime& source2)
  {
    if (source1.year_ < source2.year_) {
	return true;
    }
    else if (source1.year_ == source2.year_) {
	if (source1.month_ < source2.month_) {
	  return true;
	}
	else if (source1.month_ == source2.month_) {
	  if (source1.day_ < source2.day_) {
	    return true;
	  }
	  else if (source1.day_ == source2.day_) {
	    if (source1.hour_ < source2.hour_) {
		return true;
	    }
	    else if (source1.hour_ == source2.hour_) {
		if (source1.minute_ < source2.minute_) {
		  return true;
		}
		else if (source1.minute_ == source2.minute_) {
		  if (source1.second_ < source2.second_) {
		    return true;
		  }
		}
	    }
	  }
	}
    }
    return false;
  }

private:
  short year_,month_,day_,hour_,minute_,second_,utc_offset_,weekday_;
};
const size_t DateTime::days_in_month_noleap[13]={0,31,28,31,30,31,30,31,31,30,31,30,31};
const size_t DateTime::days_in_month_360_day[13]={0,30,30,30,30,30,30,30,30,30,30,30,30};

long long checksum(const unsigned char *buf,size_t num_words,size_t word_size,long long& sum)
{
// on return from checksum, the difference between the computed add and carry
//  logical checksum and the one packed into a record is returned, sum points
//  to the location containing the computed checksum(s), and num_sums gives the
//  number of locations containing the checksum(s)
  size_t n;
  long long err;
  long long *cp,over;
  long long OVER=0x10000000;

  OVER=(OVER<<32);
  sum=0;
  cp=new long long[num_words];

// unpack the words in the record
  bits::get(buf,cp,0,word_size,0,num_words);

// loop through and sum the words
  for (n=0; n < num_words-1; n++) {
    switch (word_size) {
	case 60:
	{
	  sum+=cp[n];
	  while (sum >= OVER) {
	    over=sum/OVER;
	    sum-=OVER;
	    sum+=over;
	  }
	  break;
	}
	default:
	{
	  sum+=cp[n];
	}
    }
  }
// compute the error (difference between sums and checksum)
  err=cp[num_words-1]-sum;
  delete[] cp;
  return err;
}

class bfstream {
public:
  enum {eof=-2,error};

  size_t block_count() const { return num_blocks; }
  virtual void close()=0;
  virtual bool is_open() const { return fs.is_open(); }
  size_t maximum_block_size() const { return max_block_len; }
  virtual void rewind()=0;

protected:
  bfstream() : fs(),file_buf(nullptr),file_buf_len(0),file_buf_pos(0),num_blocks(0),max_block_len(0),file_name() {}
  bfstream(const bfstream& source) : bfstream() { *this=source; }
  virtual ~bfstream()
  {
    if (file_buf != nullptr) {
	file_buf.reset(nullptr);
    }
    if (fs.is_open()) {
	fs.close();
    }
  }
  bfstream& operator=(const bfstream& source) { return *this; }

  std::fstream fs;
  std::unique_ptr<unsigned char[]> file_buf;
  size_t file_buf_len,file_buf_pos;
  size_t num_blocks,max_block_len;
  std::string file_name;
};

class ibfstream : virtual public bfstream
{
public:
  ibfstream() : num_read(0) {}
  virtual int ignore()=0;
  size_t number_read() const { return num_read; }
  virtual bool open(std::string filename)
  {
// opening a stream while another is open is a fatal error
    if (is_open()) {
	std::cerr << "Error: an open stream already exists" << std::endl;
	exit(1);
    }
    file_name=filename;
    fs.open(file_name.c_str(),std::ios::in);
    if (!fs.is_open()) {
	return false;
    }
    num_read=num_blocks=max_block_len=0;
    return true;
  }
  virtual int peek()=0;
  virtual int read(unsigned char *buffer,size_t buffer_length)=0;

protected:
  size_t num_read;
};

class craystream : virtual public bfstream
{
public:
  static const int eod;

protected:
  craystream()
  {
    file_buf_len=cray_block_size;
    file_buf.reset(new unsigned char[file_buf_len]);
  }

  static const size_t cray_block_size,cray_word_size;
  static const short cw_bcw,cw_eor,cw_eof,cw_eod;
  short cw_type;
};
const int craystream::eod=-5;
const size_t craystream::cray_block_size=4096;
const size_t craystream::cray_word_size=8;
const short craystream::cw_bcw=0;
const short craystream::cw_eor=0x8;
const short craystream::cw_eof=0xe;
const short craystream::cw_eod=0xf;

class icstream : public ibfstream, virtual public craystream
{
public:
  icstream() : at_eod(false) {}
  icstream(std::string filename) : icstream() { open(filename); at_eod=false; }
  icstream(const icstream& source) : icstream() { *this=source; }
  icstream& operator=(const icstream& source);
  void close()
  {
    if (!is_open()) {
	return;
    }
    fs.close();
    file_name="";
  }
  int ignore()
  {
    return read(nullptr,0);
  }
  bool open(std::string filename)
  {
    if (!ibfstream::open(filename)) {
	return false;
    }
    file_buf_pos=file_buf_len;
    return true;
  }
  int peek()
  {
    static unsigned char *fb=new unsigned char[file_buf_len];
    auto loc=fs.tellg();
    std::copy(&file_buf[0],&file_buf[file_buf_len],fb);
    short cwt=cw_type;
    size_t fbp=file_buf_pos;
    size_t nr=num_read;
    size_t nb=num_blocks;
    auto rec_len=ignore();
    if (loc >= 0) {
	fs.clear();
	fs.seekg(loc,std::ios_base::beg);
    }
    std::copy(&fb[0],&fb[file_buf_len],file_buf.get());
    file_buf_pos=fbp;
    cw_type=cwt;
    num_read=nr;
    num_blocks=nb;
    return rec_len;
  }
  int read(unsigned char *buffer,size_t buffer_length)
  {
    static int total_len=0,iterator=0;
    ++iterator;
// if the current position in the file buffer is at the end of the buffer, read
// a new block of data from the disk file
    if (file_buf_pos == file_buf_len) {
	read_from_disk();
	if (cw_type == cw_eof) {
	  return eof;
	}
    }
    int num_copied=0;
    switch (cw_type) {
	case cw_bcw:
	case cw_eor:
	case cw_eof: {
// get the length of the current block of data
	  size_t len;
	  bits::get(&file_buf[file_buf_pos],len,55,9);
// move the buffer pointer to the next control word
	  auto copy_start=file_buf_pos+cray_word_size;
	  file_buf_pos+=(len+1)*cray_word_size;
	  size_t ub;
	  if (file_buf_pos < file_buf_len) {
// if the file buffer position is inside the file buffer, get the number of
// unused bits in the end of the current block of data
	    bits::get(&file_buf[file_buf_pos],ub,4,6);
	    ub/=8;
	  }
	  else {
	    ub=0;
	  }
// copy the data to the user-specified buffer
	  num_copied=len*cray_word_size-ub;
	  total_len+=num_copied;
	  if (buffer != nullptr) {
	    if (num_copied > static_cast<int>(buffer_length)) {
		num_copied=buffer_length;
	    }
	    if (num_copied > 0) {
		std::copy(&file_buf[copy_start],&file_buf[copy_start+num_copied],buffer);
	    }
	  }
	  if (file_buf_pos < file_buf_len) {
// if the file buffer position is inside the file buffer, get the control word
	    bits::get(&file_buf[file_buf_pos],cw_type,0,4);
	    if (cw_type == cw_bcw) {
		return craystream::error;
	    }
	  }
	  else {
// otherwise, must be at the end of a Cray block, so next control word assumed
// to be a BCW
	    cw_type=cw_bcw;
	  }
	  break;
	}
    }
    switch (cw_type) {
	case cw_bcw: {
	  if (buffer != nullptr) {
	    num_copied+=read(buffer+num_copied,buffer_length-num_copied);
	  }
	  else {
	    num_copied+=read(nullptr,0);
	  }
	  --iterator;
	  if (iterator == 0) {
	    if (num_copied > total_len) {
		num_copied=total_len;
	    }
	    total_len=0;
	  }
	  return num_copied;
	}
	case cw_eor: {
	  ++num_read;
	  --iterator;
	  if (iterator == 0) {
	    if (num_copied > total_len) {
		num_copied=total_len;
	    }
	    total_len=0;
	  }
	  return num_copied;
	}
	case cw_eof: {
	  return eof;
	}
	case cw_eod: {
	  return craystream::eod;
	}
	default: {
	  return error;
	}
    }
  }
  void rewind()
  {
    fs.clear();
    fs.seekg(std::ios_base::beg);
    num_read=num_blocks=0;
    read_from_disk();
  }

private:
  int read_from_disk()
  {
    if (at_eod) {
	cw_type=cw_eod;
	return eod;
    }
    fs.read(reinterpret_cast<char *>(file_buf.get()),file_buf_len);
    size_t bytes_read=fs.gcount();
    if (num_blocks == 0 && bytes_read < file_buf_len) {
	cw_type=-1;
	return error;
    }
    file_buf_pos=0;
    bits::get(file_buf.get(),cw_type,0,4);
    if (cw_type == cw_eod) {
	at_eod=true;
    }
    if (num_blocks == 0) {
	size_t unused[2];
	bits::get(file_buf.get(),unused[0],4,7);
	bits::get(file_buf.get(),unused[1],12,19);
	size_t previous_block;
	bits::get(file_buf.get(),previous_block,31,24);
	if (cw_type != cw_bcw || unused[0] != 0 || unused[1] != 0 || previous_block != 0) {
	  cw_type=cw_eod+1;
	}
    }
    ++num_blocks;
    return 0;
  }
  bool at_eod;
};

class rptoutstream : virtual public bfstream
{
public:
  size_t block_length() const { return block_len; }
  bool is_new_block() const { return new_block; }

protected:
  rptoutstream() : block_len(0),word_count(0),new_block(false) { file_buf.reset(new unsigned char[8000]); }

  size_t block_len,word_count;
  bool new_block;
};

class irstream : public ibfstream, virtual public rptoutstream
{
public:
  irstream() : icosstream(nullptr),_flag() {}
  bool open(std::string filename)
  {
    if (is_open()) {
	std::cerr << "Error: currently connected to another file stream" << std::endl;
	exit(1);
    }
// test for COS-blocked file
    icosstream.reset(new icstream);
    if (!icosstream->open(filename)) {
	icosstream.reset(nullptr);
	return false;
    }
    unsigned char test_buffer;
    if (icosstream->read(&test_buffer,1) < 0) {
	icosstream->close();
	icosstream.reset(nullptr);
	fs.open(filename.c_str(),std::ios::in);
	if (!fs.is_open()) {
	  return false;
	}
    }
    else {
	icosstream->rewind();
    }
    file_name=filename;
    file_buf_len=file_buf_pos=0;
    num_read=num_blocks=0;
    word_count=0;
    new_block=false;
    return true;
  }
  irstream(std::string filename) : irstream()
  {
     open(filename);
  }
  void close() {
    if (!is_open()) {
	return;
    }
    if (icosstream != nullptr) {
	icosstream.reset(nullptr);
    }
    else if (fs.is_open()) {
	fs.close();
    }
  }
  int ignore()
  {
    if (file_buf_pos == file_buf_len) {
	if (icosstream != nullptr) {
	  if (icosstream->read(file_buf.get(),8000) == eof) {
	    return eof;
	  }
	  bits::get(file_buf.get(),_flag,0,4);
	  bits::get(file_buf.get(),block_len,28+_flag*4,32);
	  file_buf_len=(block_len-1)*(60+_flag*4);
	}
	else {
	  if (fs.is_open()) {
	    std::cerr << "Error: no irstream has been opened" << std::endl;
	    exit(1);
	  }
	  fs.read(reinterpret_cast<char *>(file_buf.get()),8);
	  if (fs.gcount() > 0) {
	    bits::get(file_buf.get(),_flag,0,4);
	    if (_flag != 1) {
		return error;
	    }
	    bits::get(file_buf.get(),block_len,28+_flag*4,32);
	    file_buf_len=(block_len-1)*(60+_flag*4);
	    fs.read(reinterpret_cast<char *>(&file_buf[8]),file_buf_len/8);
	  }
	  else {
	    return eof;
	  }
	}
	++num_blocks;
	new_block=true;
	long long sum;
	if (checksum(file_buf.get(),block_len,60+_flag*4,sum) != 0) {
	  std::cerr << "Warning: checksum error on block number " << num_blocks << std::endl;
	}
	file_buf_pos=60+_flag*4;
    }
    ++num_read;
    int rptlen;
    bits::get(file_buf.get(),rptlen,file_buf_pos,12);
    rptlen*=(60+_flag*4);
    file_buf_pos+=rptlen;
    return lroundf(static_cast<float>(rptlen)/8);
  }
  bool is_open() const { return (fs.is_open() || icosstream != nullptr); }
  int peek()
  {
    if (file_buf_pos == file_buf_len) {
	if (icosstream != nullptr) {
	  auto status=icosstream->read(file_buf.get(),8000);
	  if (status == eof || status == craystream::eod) {
	    return status;
	  }
	  bits::get(file_buf.get(),_flag,0,4);
	  bits::get(file_buf.get(),block_len,28+_flag*4,32);
	  if (block_len <= 0 || block_len > 1000) {
	    return error;
	  }
	  file_buf_len=(block_len-1)*(60+_flag*4);
	}
	else {
	  if (!fs.is_open()) {
	    std::cerr << "Error: no irstream has been opened" << std::endl;
	    exit(1);
	  }
	  fs.read(reinterpret_cast<char *>(file_buf.get()),8);
	  if (fs.gcount() > 0) {
	    bits::get(file_buf.get(),_flag,0,4);
	    if (_flag != 1) {
		return error;
	    }
	    bits::get(file_buf.get(),block_len,28+_flag*4,32);
	    if (block_len > 8000) {
		return error;
	    }
	    file_buf_len=(block_len-1)*8;
	    fs.read(reinterpret_cast<char *>(&file_buf[8]),file_buf_len);
	    file_buf_len=(block_len-1)*(60+_flag*4);
	  }
	  else {
	    return eof;
	  }
	}
	++num_blocks;
	new_block=true;
	word_count+=block_len;
	long long sum;
	if (checksum(file_buf.get(),block_len,60+_flag*4,sum) != 0) {
	  std::cerr << "Warning: checksum error on block number " << num_blocks << std::endl;
	}
	file_buf_pos=60+_flag*4;
    }
    else {
	new_block=false;
    }
    int rptlen;
    bits::get(file_buf.get(),rptlen,file_buf_pos,12);
    rptlen*=(60+_flag*4);
    return lroundf(static_cast<float>(rptlen)/8);
  }
  int read(unsigned char *buffer,size_t buffer_length)
  {
    auto rptlen=peek();
    if (rptlen == eof || rptlen == error || rptlen == craystream::eod) {
	return rptlen;
    }
    ++num_read;
    if (rptlen <= static_cast<int>(buffer_length)) {
	bits::get(file_buf.get(),buffer,file_buf_pos,8,0,rptlen);
	int irptlen;
	bits::get(file_buf.get(),irptlen,file_buf_pos,12);
	file_buf_pos+=(irptlen*(60+_flag*4));
	return rptlen;
    }
    else {
	if (buffer_length > 0) {
	  std::copy(&file_buf[file_buf_pos],&file_buf[file_buf_pos+buffer_length],buffer);
	}
	file_buf_pos+=rptlen;
	return buffer_length;
    }
  }
  void rewind()
  {
    if (icosstream != nullptr) {
	icosstream->rewind();
    }
    else {
	fs.clear();
	fs.seekg(std::ios_base::beg);
    }
    file_buf_len=file_buf_pos=0;
    num_read=num_blocks=0;
  }

protected:
  std::unique_ptr<icstream> icosstream;

private:
  unsigned char _flag;
};

std::string myerror="";
std::string mywarning="";

int main(int argc,char **argv)
{
  if (argc != 2) {
    std::cerr << "usage: readlmr6 <file>" << std::endl;
    exit(1);
  }
  irstream istream(argv[1]);
  if (!istream.is_open()) {
    std::cerr << "Error opening " << argv[1] << " for input" << std::endl;
    exit(1);
  }
  std::cout << "Rpt_#,RPTID,Date,Time,B10,Latitude,Longitude,Deck,Source_ID,Platform,Wind_dir,SLP,Air_temp" << std::endl;
  std::unique_ptr<unsigned char[]> buffer(new unsigned char[8000]);
  int num_bytes;
  auto cnt=0;
  while ( (num_bytes=istream.read(buffer.get(),8000)) > 0) {
    ++cnt;
    short id;
// decode RPTID - skip 12 bits from the beginning of the record, unpack 4 bits
    bits::get(buffer.get(),id,12,4);
    short b10;
// decode B10 - skip 16 bits from the beginning of the record, decode 10 bits
    bits::get(buffer.get(),b10,16,10);
    short yr,mo,dy,hr;
    bits::get(buffer.get(),yr,26,8);
    bits::get(buffer.get(),mo,34,4);
    bits::get(buffer.get(),dy,38,5);
    bits::get(buffer.get(),hr,43,12);
// subtract the base of -1 from HR
    --hr;
    short min=(hr % 100)*0.6;
    hr=hr/100;
    DateTime dt((yr+1769)*10000000000+mo*100000000+dy*1000000+hr*10000+min*100);
    int lat,lon;
    bits::get(buffer.get(),lon,59,16);
    bits::get(buffer.get(),lat,75,15);
    short deck;
    bits::get(buffer.get(),deck,94,10);
// subtract the base of -1 from DCK
    --deck;
    short sid;
    bits::get(buffer.get(),sid,104,8);
    --sid;
    short platform;
    bits::get(buffer.get(),platform,112,5);
    --platform;
    int dd;
    bits::get(buffer.get(),dd,137,9);
    if (dd == 0) {
	dd=-999;
    }
    short slp_p;
    bits::get(buffer.get(),slp_p,181,11);
    auto slp=-9999.9;
    if (slp_p > 0) {
	slp=(slp_p+8699)/10.;
    }
    short tair_p;
    bits::get(buffer.get(),tair_p,196,11);
    auto tair=-999.9;
    if (tair_p > 0) {
	tair=(tair_p-1000)/10.;
    }
    std::cout << cnt << "," << id << "," << dt.to_string("%Y-%m-%d") << "," << dt.to_string("%H:%MM:%SS") << "," << b10 << "," << (lat-9001)/100. << "," << (lon-1)/100. << "," << deck << "," << sid << "," << platform << "," << dd << "," << slp << "," << tair << std::endl;
  }
}
