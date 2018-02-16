#include <iostream>
#include <fstream>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>
#include <signal.h>
#include <string>
#include <sstream>
#include <forward_list>
#include <regex>
#include <thread>
#include <MySQL.hpp>
#include <mymap.hpp>
#include <netcdf.hpp>
#include <grid.hpp>
#include <myerror.hpp>
#include <buffer.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bits.hpp>
#include <metadata.hpp>
#include <bitmap.hpp>
#include <web/web.hpp>

metautils::Directives directives;
metautils::Args args;
std::string myerror="";
std::string mywarning="";
struct LocalArgs {
  LocalArgs() : startdate(),enddate(),parameters(),formats(),product(),grid_definition(),level(),tindex(),ofmt(),nlat(0.),slat(0.),wlon(0.),elon(0.),ladiff(0.),lodiff(0.),lat_s(),lon_s(),ststep(false) {}

  std::string startdate,enddate;
  std::list<std::string> parameters,formats;
  std::string product,grid_definition,level;
  std::string tindex,ofmt;
  float nlat,slat,wlon,elon;
  float ladiff,lodiff;
  std::string lat_s,lon_s;
  bool ststep;
} local_args;
struct Entry {
  Entry() : key(),sdum(),sdum2() {}

  std::string key;
  std::string sdum,sdum2;
};
struct NewDimEntry {
  NewDimEntry() : key(),new_dim_id(0) {}

  size_t key;
  size_t new_dim_id;
};
class SpatialBitmap {
public:
  SpatialBitmap() : buffer(nullptr), len(0) {}
  int length() const { return len; }
  unsigned char& operator[](int index) {
    if (index >= len) {
	std::cerr << "Error: index out of bounds" << std::endl;
	exit(1);
    }
    return buffer[index];
  }
  void resize(int length) {
    if (length <= len) {
	return;
    }
    len=length;
    buffer.reset(new unsigned char[len]);
  }

private:
  std::unique_ptr<unsigned char[]> buffer;
  int len;
};
std::string webhome,dsnum2,rqst_index,download_directory,compression;
struct Times {
  struct timespec start,end;
};
class TimingData
{
public:
  TimingData() : thread(0.),db(0.),read(0.),write(0.),grib2u(0.),grib2c(0.),nc(0.),read_bytes(0),num_reads(0) { }
  void add(const TimingData& source) {
    thread+=source.thread;
    db+=source.db;
    read+=source.read;
    write+=source.write;
    grib2u+=source.grib2u;
    grib2c+=source.grib2c;
    nc+=source.nc;
    read_bytes+=source.read_bytes;
    num_reads+=source.num_reads;
  }
  void reset() {
    thread=db=read=write=grib2u=grib2c=nc=0.;
    read_bytes=num_reads=0;
  }

  double thread,db,read,write,grib2u,grib2c,nc;
  long long read_bytes;
  int num_reads;
};
int MAX_NUM_THREADS=6;
const size_t OBUF_LEN=2000000;
struct ThreadStruct {
  ThreadStruct() : webID_code(),webID(),format(),fmt(),filename(),uConditions(),uConditions_no_dates(),parameters(),wget_filenames(),insert_filenames(),multi_table(),parameterTable(),includeParameterTable(nullptr),disp_order(0),f_attach(),size_input(0),parameter_mapper(nullptr),tattr(),tid(0),timing_data(),write_bytes(0),obuffer(nullptr) {}

  std::string webID_code,webID,format,fmt;
  std::string filename,uConditions,uConditions_no_dates;
  std::list<std::string> parameters,wget_filenames,insert_filenames;
  my::map<Entry> multi_table,parameterTable;
  std::shared_ptr<my::map<gributils::StringEntry>> includeParameterTable;
  size_t disp_order;
  std::string f_attach;
  long long size_input;
  std::shared_ptr<xmlutils::ParameterMapper> parameter_mapper;
  pthread_attr_t tattr;
  pthread_t tid;
  TimingData timing_data;
  long long write_bytes;
  std::unique_ptr<unsigned char[]> obuffer;
};
short topt_mo[13];
std::string csv_parameter,csv_level;
std::stringstream csv_header;
bool get_timings,is_test=false;
bool determined_temporal_subsetting=false;

extern "C" void cleanUp()
{
  print_myerror();
}

double elapsed_time(const Times& times)
{
  double elapsed_time;

  elapsed_time=(times.end.tv_sec+times.end.tv_nsec/1000000000.)-(times.start.tv_sec+times.start.tv_nsec/1000000000.);
  return elapsed_time;
}

bool is_selected_parameter(std::string format,Grid *grid,my::map<Entry>& parameterTable)
{
  std::stringstream ss;
  Entry e;

  e.key=format+"<!>";
  if (format == "WMO_GRIB2") {
    ss << grid->source() << "-" << (reinterpret_cast<GRIBGrid *>(grid))->sub_center_id() << "." << (reinterpret_cast<GRIBGrid *>(grid))->parameter_table_code() << "-" << (reinterpret_cast<GRIB2Grid *>(grid))->local_table_code() << ":" << (reinterpret_cast<GRIB2Grid *>(grid))->discipline() << "." << (reinterpret_cast<GRIB2Grid *>(grid))->parameter_category() << "." << grid->parameter();
  }
  else if (format == "WMO_GRIB1") {
    ss << grid->source() << "-" << (reinterpret_cast<GRIBGrid *>(grid))->sub_center_id() << "." << (reinterpret_cast<GRIBGrid *>(grid))->parameter_table_code() << ":" << grid->parameter();
  }
  e.key+=ss.str();
  return parameterTable.found(e.key,e);
}

void get_record(std::ifstream& ifs,off_t offset,int num_bytes,unsigned char **buffer,int& BUF_LEN,TimingData& timing_data)
{
  Times times;

  if (num_bytes > BUF_LEN) {
    if (*buffer != NULL) {
	delete[] *buffer;
    }
    BUF_LEN=num_bytes;
// set the buffer to at least 1 KB, otherwise there have been problems with
//   freeing too small of a buffer - sometimes core dumps
    if (BUF_LEN < 1024) {
	BUF_LEN=1024;
    }
    *buffer=new unsigned char[BUF_LEN];
  }
  if (get_timings) {
    clock_gettime(CLOCK_MONOTONIC,&times.start);
  }
  ifs.seekg(offset,std::ios_base::beg);
  ifs.read(reinterpret_cast<char *>(*buffer),num_bytes);
  if (get_timings) {
    clock_gettime(CLOCK_MONOTONIC,&times.end);
    timing_data.read+=elapsed_time(times);
    timing_data.read_bytes+=num_bytes;
    timing_data.num_reads++;
  }
}

void write_netcdf_subset_header(std::string input_file,OutputNetCDFStream& onc,std::list<std::string>& parameters,netCDFStream::Time& nc_time,SpatialBitmap& spatial_bitmap,int& num_values_in_subset)
{
  InputNetCDFStream inc;
  if (!inc.open(input_file)) {
    std::cerr << "Error opening " << input_file << " for input" << std::endl;
    exit(1);
  }
  auto attrs=inc.global_attributes();
  for (size_t n=0; n < attrs.size(); ++n) {
    onc.add_global_attribute(attrs[n].name,attrs[n].nc_type,attrs[n].num_values,attrs[n].values);
  }
  onc.add_global_attribute("Creation date and time",current_date_time().to_string());
  onc.add_global_attribute("Creator","NCAR - CISL RDA (dattore); data request #"+rqst_index);
  auto dims=inc.dimensions();
  num_values_in_subset=-1;
  for (size_t n=0; n < dims.size(); ++n) {
    onc.add_dimension(dims[n].name,dims[n].length);
    if (strutils::to_lower(dims[n].name) == "lat" || strutils::to_lower(dims[n].name) == "latitude" || strutils::to_lower(dims[n].name) == "lon" || strutils::to_lower(dims[n].name) == "longitude") {
	if (num_values_in_subset < 0) {
	  num_values_in_subset=1;
	}
	num_values_in_subset*=dims[n].length;
    }
  }
  auto vars=inc.variables();
  netCDFStream::VariableData sub_lat_data,sub_lon_data;
  my::map<NewDimEntry> new_dim_table;
  my::map<Entry> new_coord_table;
  if (local_args.nlat < 99.) {
    spatial_bitmap.resize(num_values_in_subset);
    num_values_in_subset=1;
    NewDimEntry nde;
    nde.new_dim_id=dims.size();
    netCDFStream::VariableData lat_data,lon_data;
    bool found_lon_gap=false;
    for (size_t n=0; n < vars.size(); ++n) {
	if (vars[n].is_coord) {
	  if (strutils::to_lower(vars[n].name) == "lat" || strutils::to_lower(vars[n].name) == "latitude") {
	    inc.variable_data(vars[n].name,lat_data);
	    auto dim_len=0;
	    sub_lat_data.resize(lat_data.size(),vars[n].nc_type);
	    for (size_t l=0; l < lat_data.size(); ++l) {
		auto lat_val=lat_data[l];
		if (lat_val >= local_args.slat && lat_val <= local_args.nlat) {
		  sub_lat_data.set(dim_len,lat_val);
		  ++dim_len;
		}
	    }
	    if (dim_len == 0) {
		std::cerr << "Error: no latitudes match the request - can't continue" << std::endl;
		exit(1);
	    }
	    Entry e;
	    e.key=vars[n].name;
	    e.sdum=e.key+"0";
	    new_coord_table.insert(e);
	    onc.add_dimension(e.sdum,dim_len);
	    nde.key=vars[n].dimids[0];
	    new_dim_table.insert(nde);
	    ++nde.new_dim_id;
	    num_values_in_subset*=dim_len;
	  }
	  else if (strutils::to_lower(vars[n].name) == "lon" || strutils::to_lower(vars[n].name) == "longitude") {
	    inc.variable_data(vars[n].name,lon_data);
	    auto dim_len=0;
	    sub_lon_data.resize(lon_data.size(),vars[n].nc_type);
	    size_t last_lon=0;
	    for (size_t l=0; l < lon_data.size(); ++l) {
		auto lon_val=lon_data[l];
		if ((lon_val >= local_args.wlon && lon_val <= local_args.elon) || (lon_val >= (local_args.wlon+360.) && lon_val <= (local_args.elon+360.))) {
		  sub_lon_data.set(dim_len,lon_val);
		  ++dim_len;
		  if (last_lon == 0) {
		    last_lon=l;
		  }
		  else if (l > 0 && l != (last_lon+1)) {
		    found_lon_gap=true;
		  }
		  last_lon=l;
		}
	    }
	    if (found_lon_gap) {
		std::cerr << "Error: found a gap in the longitudes - can't continue" << std::endl;
		exit(1);
	    }
	    if (dim_len == 0) {
		std::cerr << "Error: no longitudes match the request - can't continue" << std::endl;
		exit(1);
	    }
	    Entry e;
	    e.key=vars[n].name;
	    e.sdum=e.key+"0";
	    new_coord_table.insert(e);
	    onc.add_dimension(e.sdum,dim_len);
	    nde.key=vars[n].dimids[0];
	    new_dim_table.insert(nde);
	    ++nde.new_dim_id;
	    num_values_in_subset*=dim_len;
	  }
	}
    }
    auto l=0;
    for (size_t n=0; n < lat_data.size(); ++n) {
	for (size_t m=0; m < static_cast<size_t>(lon_data.size()); m++) {
	  if (lat_data[n] >= local_args.slat && lat_data[n] <= local_args.nlat && ((lon_data[m] >= local_args.wlon && lon_data[m] <= (local_args.elon)) || (lon_data[m] >= local_args.wlon+360. && lon_data[m] <= (local_args.elon+360.)))) {
	    spatial_bitmap[l++]=1;
	  }
	  else {
	    spatial_bitmap[l++]=0;
	  }
	}
    }
    if (l == 0) {
	std::cerr << "Error: no spatial bitmap locations were set - can't continue" << std::endl;
	exit(1);
    }
  }
  my::map<Entry> unique_variable_table;
  for (const auto& parameter : parameters) {
    Entry e;
    e.key=parameter;
    size_t idx;
    if ( (idx=e.key.find(":")) != std::string::npos) {
	e.key=e.key.substr(idx+1);
    }
    if (!unique_variable_table.found(e.key,e)) {
	unique_variable_table.insert(e);
    }
  }
  for (size_t n=0; n < vars.size(); ++n) {
    Entry e;
    if (vars[n].is_coord || unique_variable_table.found(vars[n].name,e)) {
	std::unique_ptr<size_t[]> dimension_ids(new size_t[vars[n].dimids.size()]);
	for (size_t m=0; m < vars[n].dimids.size(); m++) {
	  NewDimEntry nde;
	  if (new_dim_table.found(vars[n].dimids[m],nde)) {
	    dimension_ids[m]=nde.new_dim_id;
	  }
	  else {
	    dimension_ids[m]=vars[n].dimids[m];
	  }
	}
	if (!new_coord_table.found(vars[n].name,e)) {
	  e.sdum=vars[n].name;
	}
	onc.add_variable(e.sdum,vars[n].nc_type,vars[n].dimids.size(),dimension_ids.get());
	for (size_t m=0; m < vars[n].attrs.size(); ++m) {
	  onc.add_variable_attribute(e.sdum,vars[n].attrs[m].name,vars[n].attrs[m].nc_type,vars[n].attrs[m].num_values,vars[n].attrs[m].values);
	  if (strutils::to_lower(vars[n].name) == "time" && vars[n].attrs[m].name == "units") {
	    auto uparts=strutils::split(*(reinterpret_cast<std::string *>(vars[n].attrs[m].values)));
	    if (uparts.size() > 2 && uparts[1] == "since") {
		nc_time.units=uparts[0];
		auto s=uparts[2];
		for (size_t n=3; n < uparts.size(); ++n) {
		  s+=uparts[n];
		}
		strutils::replace_all(s,"-","");
		strutils::replace_all(s,":","");
		nc_time.base.set(std::stoll(s));
		nc_time.nc_type=vars[n].nc_type;
	    }
	    if (nc_time.units.empty()) {
		std::cerr << "Error: unable to locate time units" << std::endl;
		exit(1);
	    }
	  }
	}
    }
  }
  onc.write_header();
  for (size_t n=0; n < vars.size(); ++n) {
    Entry e;
    if (vars[n].is_coord && !vars[n].is_rec && !new_coord_table.found(vars[n].name,e)) {
	netCDFStream::VariableData var_data;
	inc.variable_data(vars[n].name,var_data);
	onc.write_non_record_data(vars[n].name,var_data.get());
    }
  }
  if (sub_lat_data.size() > 0) {
    onc.write_non_record_data("lat0",sub_lat_data.get());
  }
  if (sub_lon_data.size() > 0) {
    onc.write_non_record_data("lon0",sub_lon_data.get());
  }
  inc.close();
}

void clear_wfrqst()
{
  MySQL::Server server_d;
  metautils::connect_to_rdadb_server(server_d);
  if (!server_d) {
    std::cerr << "Error: unable to connect to RDADB (clear_wfrqst)" << std::endl;
    exit(1);
  }
  server_d._delete("wfrqst","rindex = "+rqst_index);
  server_d.disconnect();
}

void insert_into_wfrqst(std::string fname,std::string fmt,size_t disp_order)
{
  MySQL::Server server_d;
  metautils::connect_to_rdadb_server(server_d);
  if (!server_d) {
    std::cerr << "Error: unable to connect to RDADB (insert_into_wfrqst)" << std::endl;
    exit(1);
  }
  auto dorder=strutils::itos(disp_order);
  if (server_d.insert("wfrqst","rindex,disp_order,data_format,file_format,wfile,status",rqst_index+","+dorder+",'"+fmt+"','','"+fname+"','O'","update disp_order="+dorder+",data_format='"+fmt+"'") < 0) {
    std::cerr << "Insert error: " << server_d.error() << std::endl;
    exit(1);
  }
  server_d.disconnect();
}

void set_fcount(size_t fcount)
{
  MySQL::Server server_d;
  metautils::connect_to_rdadb_server(server_d);
  if (!server_d) {
    std::cerr << "Error: unable to connect to RDADB (set_fcount)" << std::endl;
    exit(1);
  }
  if (server_d.update("dsrqst","fcount = "+strutils::itos(fcount),"rindex = "+rqst_index) < 0) {
    std::cerr << "Error (update): " << server_d.error() << std::endl;
    exit(1);
  }
  server_d.disconnect();
}

void *build_file(void *ts)
{
  ThreadStruct *t=reinterpret_cast<ThreadStruct *>(ts);
  std::ifstream ifs;
  OutputNetCDFStream onc;
  std::ofstream ofs;
  MySQL::LocalQuery query,query_no_dates,query_count_files;
  MySQL::Row row;
  int BUF_LEN=0;
  unsigned char *buffer=NULL;
  struct stat buf;
  std::string last_valid_date;
  std::unique_ptr<my::map<Entry>> nts_table;
  Times times,db_times,grib2u_times,grib2c_times,write_times,nc_times;
  netCDFStream::Time nc_time;
  SpatialBitmap spatial_bitmap;
  int num_values_in_subset=0;
  netCDFStream::VariableData time_data,var_data;
  union {
    int i;
    float f;
  } b4_data;

  GridToNetCDF::GridData grid_data;
  grid_data.wrote_header=false;
  grid_data.parameter_mapper=t->parameter_mapper;
  t->f_attach="";
  t->insert_filenames.clear();
  t->wget_filenames.clear();
  if (!local_args.ofmt.empty()) {
    t->fmt=local_args.ofmt;
  }
  else {
    t->fmt=t->format;
  }
  Entry e;
  auto is_multi=t->multi_table.found(t->webID_code,e);
  auto already_exists=false;
  if (!is_test) {
    if (get_timings) {
	clock_gettime(CLOCK_MONOTONIC,&times.start);
    }
    t->write_bytes=0;
    if (local_args.ststep) {
	nts_table.reset(new my::map<Entry>(99999));
    }
    if ((strutils::to_lower(local_args.ofmt) == "netcdf" && !is_multi) || (strutils::to_lower(t->format) == "netcdf" && local_args.ofmt.empty())) {
	if (!strutils::has_ending(t->filename,".nc")) {
	  t->filename+=".nc";
	}
	if (stat((download_directory+t->filename).c_str(),&buf) == 0 || stat((download_directory+t->filename+compression).c_str(),&buf) == 0) {
	  already_exists=true;
	  t->write_bytes+=buf.st_size;
	}
	else {
	  if (strutils::to_lower(local_args.ofmt) == "netcdf" && !onc.open(download_directory+t->filename)) {
	    std::cerr << "Error opening " << download_directory << t->filename << " for output" << std::endl;
	    exit(1);
	  }
	}
    }
    else {
	if (strutils::to_lower(local_args.ofmt) == "netcdf") {
	  if (stat((download_directory+t->filename+".nc").c_str(),&buf) == 0 || stat((download_directory+t->filename+".nc"+compression).c_str(),&buf) == 0) {
	    already_exists=true;
	    t->write_bytes+=buf.st_size;
	    t->insert_filenames.emplace_back((t->filename).substr(1)+".nc");
	    t->wget_filenames.emplace_back((t->filename).substr(1)+".nc"+compression);
	  }
	  else if (stat((download_directory+t->filename).c_str(),&buf) == 0 || stat((download_directory+t->filename+compression).c_str(),&buf) == 0) {
	    already_exists=true;
	    t->write_bytes+=buf.st_size;
	    if (is_multi) {
		t->f_attach=t->filename+"<!>"+t->format;
	    }
	    t->insert_filenames.emplace_back((t->filename).substr(1));
	    t->wget_filenames.emplace_back((t->filename).substr(1)+compression);
	  }
	}
	else {
	  if (strutils::to_lower(local_args.ofmt) == "csv") {
	    t->filename+=".csv";
	  }
	  if (stat((download_directory+t->filename).c_str(),&buf) == 0 || stat((download_directory+t->filename+compression).c_str(),&buf) == 0) {
	    already_exists=true;
	    t->write_bytes+=buf.st_size;
	  }
	}
	if (!already_exists) {
	  if (!local_args.ststep) {
	    ofs.open((download_directory+t->filename).c_str());
	    if (!ofs.is_open()) {
		std::cerr << "Error opening " << download_directory << t->filename << " for output" << std::endl;
		exit(1);
	    }
	  }
	  if (is_multi) {
	    t->f_attach=t->filename+"<!>"+t->format;
	}
    }
    if (!local_args.ststep && !is_multi && (topt_mo[0] == 0 || already_exists)) {
	t->insert_filenames.emplace_back((t->filename).substr(1));
	t->wget_filenames.emplace_back((t->filename).substr(1)+compression);
    }
  }
  else {
    already_exists=false;
  }
  std::string union_query="";
  std::string union_query_no_dates="";
  auto do_sort=false;
  for (const auto& parameter : t->parameters) {
    if (!union_query.empty()) {
	union_query+=" union ";
    }
    if (!union_query_no_dates.empty()) {
	union_query_no_dates+=" union ";
    }
    if (local_args.ststep || topt_mo[0] == 1 || strutils::to_lower(local_args.ofmt) == "csv") {
	union_query+="select byte_offset,byte_length,valid_date from IGrML.`ds"+dsnum2+"_inventory_"+parameter+"` where webID_code = "+t->webID_code+" and "+t->uConditions;
	union_query_no_dates+="select byte_offset,byte_length,valid_date from IGrML.`ds"+dsnum2+"_inventory_"+parameter+"` where webID_code = "+t->webID_code;
	if (!t->uConditions_no_dates.empty()) {
	  union_query_no_dates+=" and "+t->uConditions_no_dates;
	}
	if (parameter == t->parameters.back()) {
	  query.set("select distinct byte_offset,byte_length,valid_date from ("+union_query+") as u order by valid_date,byte_offset");
	  query_no_dates.set("select distinct byte_offset,byte_length,valid_date from ("+union_query_no_dates+") as u order by valid_date,byte_offset");
	}
    }
    else if (strutils::to_lower(t->format) == "netcdf" && local_args.ofmt.empty()) {
	union_query+="select byte_offset,byte_length,valid_date,process from IGrML.`ds"+dsnum2+"_inventory_"+parameter+"` where webID_code = "+t->webID_code+" and "+t->uConditions;
	union_query_no_dates+="select byte_offset,byte_length,valid_date,process from IGrML.`ds"+dsnum2+"_inventory_"+parameter+"` where webID_code = "+t->webID_code;
	if (!t->uConditions_no_dates.empty()) {
	  union_query_no_dates+=" and "+t->uConditions_no_dates;
	}
	if (parameter == t->parameters.back()) {
	  query.set("select distinct byte_offset,byte_length,valid_date,process from ("+union_query+") as u order by valid_date");
	  query_no_dates.set("select distinct byte_offset,byte_length,valid_date,process from ("+union_query_no_dates+") as u order by valid_date");
	  if (local_args.nlat > 99.) {
	    query_count_files.set("select count(byte_offset) from ("+union_query_no_dates+") as u");
	  }
	}
    }
    else {
	union_query+="select byte_offset,byte_length from IGrML.`ds"+dsnum2+"_inventory_"+parameter+"` where webID_code = "+t->webID_code+" and "+t->uConditions;
	union_query_no_dates+="select byte_offset,byte_length from IGrML.`ds"+dsnum2+"_inventory_"+parameter+"` where webID_code = "+t->webID_code;
	if (!t->uConditions_no_dates.empty()) {
	 union_query_no_dates+=" and "+t->uConditions_no_dates;
	}
	if (parameter == t->parameters.back()) {
	  query.set("select distinct byte_offset,byte_length from ("+union_query+") as u");
	  query_no_dates.set("select distinct byte_offset,byte_length from ("+union_query_no_dates+") as u");
	  do_sort=true;
	}
    }
  }
  MySQL::Server server;
  size_t num_tries=0;
  while (num_tries < 3) {
    metautils::connect_to_metadata_server(server);
    if (server) {
	break;
    }
    ++num_tries;
    sleep(pow(2.,num_tries)*5);
  }
  if (num_tries == 3) {
    std::cerr << "Error: unable to connect to metadata server - 3" << std::endl;
    exit(1);
  }
  if (query.submit(server) < 0) {
    std::cerr << "Error: " << query.error() << std::endl;
    std::cerr << "Query: " << query.show() << std::endl;
    exit(1);
  }
  if (!is_test && !determined_temporal_subsetting) {
    if (query_no_dates.submit(server) < 0) {
	std::cerr << "Error: " << query_no_dates.error() << std::endl;
	std::cerr << "Query: " << query_no_dates.show() << std::endl;
	exit(1);
    }
    if (query.num_rows() < query_no_dates.num_rows())
	determined_temporal_subsetting=true;
  }
  if (!already_exists) {
    auto linked_to_full_file=false;
    if (!is_test) {
	if (query_count_files) {
	  if (query_count_files.submit(server) < 0) {
	    std::cerr << "Error: " << query_count_files.error() << std::endl;
	    std::cerr << "Query: " << query_count_files.error() << std::endl;
	    exit(1);
	  }
	  if (!query_count_files.fetch_row(row) || row[0].empty()) {
	    std::cerr << "Error getting file count" << std::endl;
	    exit(1);
	  }
	  if (std::stoi(row[0]) == static_cast<int>(query.num_rows())) {
std::cerr << "**linked '" << webhome+"/"+t->webID << "' to '" << download_directory+t->filename << "'" << std::endl;
//	    mysystem2("/bin/cp "+webhome+"/"+t->webID+" "+download_directory+t->filename,oss,ess);
std::stringstream oss,ess;
mysystem2("/bin/ln -s "+webhome+"/"+t->webID+" "+download_directory+t->filename,oss,ess);
	    if (!ess.str().empty()) {
		std::cerr << "Error: " << ess.str() << std::endl;
		exit(1);
	    }
	    linked_to_full_file=true;
	  }
	}
	if (!linked_to_full_file) {
	  if (strutils::to_lower(t->format) == "netcdf" && local_args.ofmt.empty()) {
	    if (!onc.open(download_directory+t->filename)) {
		std::cerr << "Error opening " << download_directory << t->filename << " for output" << std::endl;
		exit(1);
	    }
	    write_netcdf_subset_header(webhome+"/"+t->webID,onc,t->parameters,nc_time,spatial_bitmap,num_values_in_subset);
	  }
	  ifs.open((webhome+"/"+t->webID).c_str());
	  if (!ifs.is_open()) {
	    std::cerr << "Error opening " << webhome << "/" << t->webID << " for input" << std::endl;
	    exit(1);
	  }
	}
    }
    if (!linked_to_full_file) {
	grid_data.subset_definition.south_latitude=local_args.slat;
	grid_data.subset_definition.north_latitude=local_args.nlat;
	grid_data.subset_definition.west_longitude=local_args.wlon;
	grid_data.subset_definition.east_longitude=local_args.elon;
	if (get_timings) {
	  clock_gettime(CLOCK_MONOTONIC,&db_times.start);
	}
	if (is_test) {
	  t->timing_data.num_reads=query.num_rows();
	  while ( (query.fetch_row(row))) {
	    t->timing_data.read_bytes+=std::stoi(row[1]);
	  }
	  return NULL;
	}
	if (get_timings) {
	  clock_gettime(CLOCK_MONOTONIC,&db_times.end);
	  t->timing_data.db+=elapsed_time(db_times);
	}
	my::map<Grid::GLatEntry> *glats=nullptr;
	while ( (query.fetch_row(row))) {
	  bool okay_to_continue;
	  if (topt_mo[0] == 0) {
	    okay_to_continue=true;
	  }
	  else {
	    if (topt_mo[std::stoi(row[2].substr(4,2))] == 1) {
		okay_to_continue=true;
	    }
	    else {
		okay_to_continue=false;
	    }
	  }
	  if (okay_to_continue) {
	    if (!local_args.ofmt.empty()) {
		if (strutils::to_lower(local_args.ofmt) == "netcdf") {
		  if (is_multi) {
		    auto num_bytes=std::stoi(row[1]);
		    get_record(ifs,std::stoll(row[0]),num_bytes,&buffer,BUF_LEN,t->timing_data);
		    if (get_timings) {
			clock_gettime(CLOCK_MONOTONIC,&write_times.start);
		    }
		    ofs.write(reinterpret_cast<char *>(buffer),num_bytes);
		    t->write_bytes+=num_bytes;
		    if (get_timings) {
			clock_gettime(CLOCK_MONOTONIC,&write_times.end);
			t->timing_data.write+=elapsed_time(write_times);
		    }
		  }
		  else {
		    if (topt_mo[0] == 1) {
			t->insert_filenames.emplace_back((t->filename).substr(1));
			t->wget_filenames.emplace_back((t->filename).substr(1)+compression);
		    }
		    get_record(ifs,std::stoll(row[0]),std::stoi(row[1]),&buffer,BUF_LEN,t->timing_data);
		    if (t->format == "WMO_GRIB2") {
			if (get_timings) {
			  clock_gettime(CLOCK_MONOTONIC,&grib2u_times.start);
			}
			GRIB2Message msg2;
			msg2.fill(buffer,false);
			if (get_timings) {
			  clock_gettime(CLOCK_MONOTONIC,&grib2u_times.end);
			  t->timing_data.grib2u+=elapsed_time(grib2u_times);
			}
			for (size_t n=0; n < msg2.number_of_grids(); ++n) {
			  auto grid=msg2.grid(n);
			  if (is_selected_parameter(t->format,grid,t->parameterTable)) {
			    if (get_timings) {
				clock_gettime(CLOCK_MONOTONIC,&nc_times.start);
			    }
			    convert_grid_to_netcdf(grid,Grid::grib2Format,&onc,grid_data);
			    if (get_timings) {
				clock_gettime(CLOCK_MONOTONIC,&nc_times.end);
				t->timing_data.nc+=elapsed_time(nc_times);
			    }
			  }
			}
		    }
		    else {
			std::cerr << "Error: unable to convert from '" << t->format << "'" << std::endl;
			exit(1);
		    }
		  }
		}
		else if (strutils::to_lower(local_args.ofmt) == "csv") {
		  get_record(ifs,std::stoll(row[0]),std::stoi(row[1]),&buffer,BUF_LEN,t->timing_data);
		  if (t->format == "WMO_GRIB2") {
		    GRIB2Message msg2;
		    msg2.fill(buffer,false);
		    for (size_t n=0; n < msg2.number_of_grids(); ++n) {
			auto grid=msg2.grid(n);
			if (is_selected_parameter(t->format,grid,t->parameterTable)) {
			  if (csv_header.str().empty()) {
			    csv_header << "\"Date\",\"Time\",\"" << csv_parameter << "@" << csv_level << "\"";
			  }
			  if (grid->definition().type == Grid::gaussianLatitudeLongitudeType) {
			    if (glats == nullptr) {
				glats=new my::map<Grid::GLatEntry>;
				fill_gaussian_latitudes(*glats,grid->definition().num_circles,true);
			    }
			    ofs << grid->valid_date_time().to_string("%Y%m%d%H%MM,") << grid->valid_date_time().to_string("%mm/%dd/%Y,%HH:%MM") << "," << grid->gridpoint(grid->longitude_index_of(local_args.wlon),(reinterpret_cast<GRIBGrid *>(grid))->latitude_index_of(local_args.nlat,glats)) << std::endl;
			  }
			  else {
			    ofs << grid->valid_date_time().to_string("%Y%m%d%H%MM,") << grid->valid_date_time().to_string("%mm/%dd/%Y,%HH:%MM") << "," << grid->gridpoint_at(local_args.nlat,local_args.wlon) << std::endl;
			  }
			}
		    }
		  }
		}
		else {
		  std::cerr << "Error: unable to convert to '" << local_args.ofmt << "'" << std::endl;
		  exit(1);
		}
	    }
	    else {
		if (local_args.ststep) {
		  if (row[2] != last_valid_date && ofs.is_open()) {
		    ofs.close();
		  }
		  std::string nts_filename=t->filename.substr(0,1)+row[2]+"."+t->filename.substr(1);
		  if (!ofs.is_open()) {
		    if (stat((download_directory+nts_filename).c_str(),&buf) != 0) {
			if (!nts_table->found(nts_filename,e)) {
			  e.key=nts_filename;
			  nts_table->insert(e);
			  t->insert_filenames.emplace_back(nts_filename.substr(1));
			  t->wget_filenames.emplace_back(nts_filename.substr(1)+compression);
			}
			ofs.open((download_directory+nts_filename).c_str());
			if (!ofs.is_open()) {
			  std::cerr << "Error opening " << download_directory << nts_filename << " for output" << std::endl;
			  exit(1);
			}
		    }
		    else {
			if (!nts_table->found(nts_filename,e)) {
			  e.key=nts_filename;
			  nts_table->insert(e);
			  t->insert_filenames.emplace_back(nts_filename.substr(1));
			  t->wget_filenames.emplace_back(nts_filename.substr(1)+compression);
			}
		    }
		  }
		  last_valid_date=row[2];
		}
		if (ofs.is_open()) {
		  auto num_bytes=std::stoi(row[1]);
		  get_record(ifs,std::stoll(row[0]),std::stoi(row[1]),&buffer,BUF_LEN,t->timing_data);
		  auto is_spatial_subset=false;
		  if (local_args.nlat < 9999. && local_args.elon < 9999. && local_args.slat > -9999. && local_args.wlon > -9999.) {
		    if (t->format == "WMO_GRIB2") {
			if (get_timings) {
			  clock_gettime(CLOCK_MONOTONIC,&grib2u_times.start);
			}
			GRIB2Message msg2;
			msg2.fill(buffer,false);
			if (get_timings) {
			  clock_gettime(CLOCK_MONOTONIC,&grib2u_times.end);
			  t->timing_data.grib2u+=elapsed_time(grib2u_times);
			  clock_gettime(CLOCK_MONOTONIC,&grib2c_times.start);
			}
			GRIB2Message smsg2;
			smsg2.initialize(2,NULL,0,true,true);
			for (size_t n=0; n < msg2.number_of_grids(); ++n) {
			  auto grid=msg2.grid(n);
			  if (is_selected_parameter(t->format,grid,t->parameterTable)) {
			    GRIB2Grid sgrid2;
			    sgrid2=(reinterpret_cast<GRIB2Grid *>(grid))->create_subset(local_args.slat,local_args.nlat,1,local_args.wlon,local_args.elon,1);
			    smsg2.append_grid(&sgrid2);
			  }
			}
			num_bytes=smsg2.copy_to_buffer(t->obuffer.get(),OBUF_LEN);
			if (get_timings) {
			  clock_gettime(CLOCK_MONOTONIC,&grib2c_times.end);
			  t->timing_data.grib2c+=elapsed_time(grib2c_times);
			}
		    }
		    else if (t->format == "WMO_GRIB1") {
			GRIBMessage msg;
			msg.fill(buffer,false);
			auto grid=msg.grid(0);
			if (is_selected_parameter(t->format,grid,t->parameterTable)) {
			  GRIBMessage smsg;
			  smsg.initialize(1,NULL,0,true,true);
			  GRIBGrid sgrid;
			  sgrid=create_subset_grid(*(reinterpret_cast<GRIBGrid *>(grid)),local_args.slat,local_args.nlat,local_args.wlon,local_args.elon);
			  if (sgrid.is_filled()) {
			    smsg.append_grid(&sgrid);
			    num_bytes=smsg.copy_to_buffer(t->obuffer.get(),OBUF_LEN);
			  }
			  else {
			    num_bytes=0;
			  }
			}
		    }
		    else {
			std::cerr << "Error: unable to create subset for format '" << t->format << "'" << std::endl;
			exit(1);
		    }
		    is_spatial_subset=true;
		  }
		  if (get_timings) {
		    clock_gettime(CLOCK_MONOTONIC,&write_times.start);
		  }
		  if (is_spatial_subset) {
		    if (num_bytes > 0) {
			ofs.write(reinterpret_cast<char *>(t->obuffer.get()),num_bytes);
		    }
		  }
		  else {
		    ofs.write(reinterpret_cast<char *>(buffer),num_bytes);
		  }
		  t->write_bytes+=num_bytes;
		  if (get_timings) {
		    clock_gettime(CLOCK_MONOTONIC,&write_times.end);
		    t->timing_data.write+=elapsed_time(write_times);
		  }
		}
		else if (onc.is_open()) {
		  if (t->parameters.size() > 1) {
		    std::cerr << "Error: found more than one parameter - can't continue" << std::endl;
		    exit(1);
		  }
		  auto tval=DateTime(std::stoll(row[2])*100).seconds_since(nc_time.base);
		  if (nc_time.units == "hours") {
		    tval/=3600.;
		  }
		  else if (nc_time.units == "days") {
		    tval/=86400.;
		  }
		  else {
		    std::cerr << "Error: can't handle nc time units of '" << nc_time.units << "'" << std::endl;
		    exit(1);
		  }
		  if (time_data.size() == 0) {
		    time_data.resize(1,nc_time.nc_type);
		  }
		  time_data.set(0,tval);
		  onc.add_record_data(time_data);
		  get_record(ifs,std::stoll(row[0]),std::stoi(row[1]),&buffer,BUF_LEN,t->timing_data);
		  if (get_timings) {
		    clock_gettime(CLOCK_MONOTONIC,&nc_times.start);
		  }
		  if (!row[3].empty()) {
		    if (var_data.size() == 0) {
			var_data.resize(num_values_in_subset,static_cast<netCDFStream::NcType>(std::stoi(row[3])));
		    }
		    auto m=0;
		    for (int n=0; n < spatial_bitmap.length(); ++n) {
			if (spatial_bitmap[n] == 1) {
			  switch (static_cast<netCDFStream::NcType>(std::stoi(row[3]))) {
			    case netCDFStream::NcType::FLOAT:
			    {
				get_bits(&buffer[n*4],b4_data.i,0,32);
				var_data.set(m++,b4_data.f);
				break;
			    }
			    default:
			    {
				std::cerr << "Error: can't handle nc variable type " << row[3] << std::endl;
				exit(1);
			    }
			  }
			}
		    }
		    onc.add_record_data(var_data);
		    if (get_timings) {
			clock_gettime(CLOCK_MONOTONIC,&nc_times.end);
			t->timing_data.nc+=elapsed_time(nc_times);
		    }
		  }
		  else {
		    std::cerr << "Error: incomplete inventory information - can't continue" << std::endl;
		    exit(1);
		  }
		}
	    }
	  }
	}
	ifs.close();
	ifs.clear();
	if ((strutils::to_lower(local_args.ofmt) == "netcdf" && !is_multi) || (strutils::to_lower(t->format) == "netcdf" && local_args.ofmt.empty())) {
	  if (!onc.close()) {
	    std::cerr << "Error: " << myerror << " for file " << download_directory << t->filename << std::endl;
	    myerror="";
	  }
	  stat((download_directory+t->filename).c_str(),&buf);
	  if (buf.st_size == 8) {
	    system(("rm -f "+download_directory+t->filename).c_str());
	    if (t->insert_filenames.size() == 1) {
		t->insert_filenames.clear();
	    }
	    t->filename="";
	    t->f_attach="";
	  }
	  else
	    t->write_bytes+=buf.st_size;
	}
	else {
	  auto offset=ofs.tellp();
	  ofs.close();
	  if (offset == 0) {
	    system(("rm -f "+download_directory+t->filename).c_str());
	    if (t->insert_filenames.size() == 1) {
		t->insert_filenames.clear();
	    }
	    t->filename="";
	    t->f_attach="";
	  }
	}
	if (do_sort) {
	  if (std::regex_search(t->format,std::regex("grib2",std::regex::icase)) && std::regex_search(local_args.ofmt,std::regex("netcdf",std::regex::icase))) {
	    if (!std::regex_search(t->filename,std::regex("\\.nc$"))) {
		std::stringstream oss,ess;
		mysystem2("/bin/tcsh -c \"sortgrid -f grib2 -o nc "+download_directory+t->filename+" "+download_directory+t->filename+".sorted\"",oss,ess);
		if (ess.str().empty()) {
		  system(("mv "+download_directory+t->filename+".sorted "+download_directory+t->filename).c_str());
		}
		else {
		  std::cerr << "sort error: '" << ess.str() << "'" << std::endl;
		  exit(1);
		}
	    }
	  }
	  else if (!local_args.ofmt.empty()) {
	    std::cerr << "don't know how to sort input format '" << t->format << "' to output format '" << local_args.ofmt << "'" << std::endl;
	    exit(1);
	  }
	}
    }
  }
  server.disconnect();
  if (local_args.ststep) {
    nts_table=nullptr;
  }
  if (buffer != nullptr) {
    delete[] buffer;
  }
  if (get_timings) {
    clock_gettime(CLOCK_MONOTONIC,&times.end);
    t->timing_data.thread=elapsed_time(times);
  }
  return nullptr;
}

void *do_conversion(void *ts)
{
  ThreadStruct *t=reinterpret_cast<ThreadStruct *>(ts);
  Times times;
  if (get_timings) {
    clock_gettime(CLOCK_MONOTONIC,&times.start);
  }
  GridToNetCDF::GridData grid_data;
  grid_data.parameter_mapper=t->parameter_mapper;
  t->insert_filenames.clear();
  t->wget_filenames.clear();
  if (strutils::to_lower(local_args.ofmt) == "netcdf") {
    auto fileinfo=strutils::split(t->f_attach,"<!>");
    if (fileinfo[1] == "WMO_GRIB2") {
	InputGRIBStream istream;
	istream.open((download_directory+fileinfo[0]).c_str());
	OutputNetCDFStream onc;
	if (!onc.open(download_directory+fileinfo[0]+".nc")) {
	  std::cerr << "Error opening " << download_directory << fileinfo[0] << ".nc for output" << std::endl;
	  exit(1);
	}
	grid_data.subset_definition.south_latitude=local_args.slat;
	grid_data.subset_definition.north_latitude=local_args.nlat;
	grid_data.subset_definition.west_longitude=local_args.wlon;
	grid_data.subset_definition.east_longitude=local_args.elon;
	GridToNetCDF::HouseKeeping hk;
	hk.include_parameter_table=t->includeParameterTable;
	write_netcdf_header_from_grib_file(istream,onc,grid_data,hk);
	if (grid_data.record_flag < 0) {
	  Buffer buffer;
	  int len;
	  while ( (len=istream.peek()) > 0) {
	    buffer.allocate(len);
	    istream.read(&buffer[0],len);
	    GRIB2Message msg;
	    msg.fill(&buffer[0],false);
	    for (size_t n=0; n < msg.number_of_grids(); ++n) {
		auto grid=msg.grid(n);
		if (is_selected_parameter(t->format,grid,t->parameterTable)) {
		  convert_grid_to_netcdf(grid,Grid::grib2Format,&onc,grid_data);
		}
	    }
	  }
	}
	else {
	  convert_grib_file_to_netcdf(download_directory+fileinfo[0],onc,grid_data.ref_date_time,grid_data.cell_methods,grid_data.subset_definition,*(t->parameter_mapper),hk.unique_variable_table,grid_data.record_flag);
	}
	istream.close();
	if (!onc.close()) {
	  std::cerr << "Error: " << myerror << " for file " << download_directory << fileinfo[0] << ".nc" << std::endl;
	  myerror="";
	}
	t->fmt="netCDF";
	t->insert_filenames.emplace_back(fileinfo[0].substr(1)+".nc");
	t->wget_filenames.emplace_back(fileinfo[0].substr(1)+".nc"+compression);
	for (const auto& key : hk.unique_variable_table.keys()) {
	  netCDFStream::UniqueVariableEntry ve;
	  hk.unique_variable_table.found(key,ve);           
	  ve.free_memory();
	}
    }
    else {
	std::cerr << "Error: unable to convert from '" << fileinfo[1] << "'" << std::endl;
	exit(1);
    }
    system(("rm -f "+download_directory+fileinfo[0]).c_str());
  }
  if (get_timings) {
    clock_gettime(CLOCK_MONOTONIC,&times.end);
    t->timing_data.thread=elapsed_time(times);
  }
  return nullptr;
}

struct BitmapEntry {
  struct Data {
    Data() : ladiffs(),lodiffs() {}

    std::list<float> ladiffs,lodiffs;
  };
  BitmapEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};

void combine_csv_files(std::list<std::string>& file_list,std::list<std::string>& wget_list)
{
  std::stringstream oss,ess;

  std::ofstream ofs;
  auto csv_file="data.req"+rqst_index+"_"+local_args.lat_s+"_"+local_args.lon_s+".csv";
  ofs.open(download_directory+"/"+csv_file);
  std::forward_list<std::string> line_list;
  for (const auto& file : file_list) {
    std::deque<std::string> sp=strutils::split(file,"<!>");
    std::ifstream ifs;
    char line[32768];
    ifs.open(download_directory+"/"+sp[2]);
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	line_list.emplace_front(line);
	ifs.getline(line,32768);
    }
    ifs.close();
    ifs.clear();
    mysystem2("/bin/rm "+download_directory+"/"+sp[2],oss,ess);
  }
  line_list.sort(
  [](const std::string& left,const std::string& right) -> bool
  {
    long long l=std::stoll(left.substr(0,12));
    long long r=std::stoll(right.substr(0,12));
    if (l <= r) {
	return true;
    }
    else {
	return false;
    }
  });
  ofs << csv_header.str() << std::endl;
  for (const auto& line : line_list) {
    ofs << line.substr(13) << std::endl;
  }
  ofs.close();
set_fcount(1);
clear_wfrqst();
insert_into_wfrqst(csv_file,"csv",1);
  file_list.clear();
  file_list.emplace_back("1<!>csv<!>"+csv_file);
  wget_list.clear();
  wget_list.emplace_back(csv_file);
std::this_thread::sleep_for(std::chrono::seconds(15));
}

int main(int argc,char **argv)
{
  std::ifstream ifs;
  std::ofstream ofs;
  char line[256];
  MySQL::LocalQuery query,query2;
  MySQL::Row row,row2;
  std::string invConditions,fmtConditions,whereConditions,lvlConditions,paramConditions,allLvlConditions;
  std::string uConditions,uConditions_no_dates,union_query;
  int next=1,n,m,l;
  my::map<Entry> uniqueFormatsTable,uniqueParametersTable,levelTable,uniqueLevelTable;
  std::list<std::string> multi_file_list;
  Entry e;
  int num_threads,thread_index;
  size_t idx;
  ThreadStruct *t;
  int *t_idx;
  std::vector<std::shared_ptr<xmlutils::ParameterMapper>> p;
  xmlutils::LevelMapper level_mapper;
  GRIBMessage msg;
  GRIB2Grid grid;
  std::string rinfo,filename,sdum,f_attach,http_cookie;
  size_t disp_order;
  int num_input=0,num_topt_mo=0,num_parameters=0;
  long long size_input=0;
  my::map<Entry> multi_table,parameterTable;
  std::shared_ptr<my::map<gributils::StringEntry>> includeParameterTable;
  gributils::StringEntry se;
  std::list<std::string> query_results;
  my::map<BitmapEntry> gridDefinition_bitmaps;
  BitmapEntry be;
  std::list<float>::iterator it_lat,it_lon,end_it;
  Grid::GridDefinition grid_def;
  Grid::GridDimensions grid_dim;
  my::map<Grid::GLatEntry> gaus_lats;
  Grid::GLatEntry gle;
  std::vector<size_t> gridDefinition_values;
  my::map<Entry> rdafileHash(99999),formatHash;
  std::string format,note,rqsttype,duser,location;
  int subflag=0;
  std::string months[]={"","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
  std::list<std::string> wget_list,insert_list;
  std::stringstream oss,ess;
  Times times,db_times;
  TimingData timing_data;
  long long write_bytes=0;
  bool should_attach,ignore_volume=false,ignore_restrictions=false;

  if (argc < 3 || argc > 6) {
    std::cerr << "usage: subconv [-t] [-I] [-R] [-n num_threads] rindex directory" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "-I    ignore volume and allow large request to process" << std::endl;
    std::cerr << "-R    ignore user restrictions and process request" << std::endl;
    std::cerr << "-t    turn on internal timings" << std::endl;
    std::cerr << "\n-OR-" << std::endl;
    std::cerr << "usage: subconv -T <rinfo_string>" << std::endl;
    exit(1);
  }
  get_timings=false;
  while (strutils::has_beginning(argv[next],"-")) {
    if (std::string(argv[next]) == "-n") {
	MAX_NUM_THREADS=atoi(argv[++next]);
	next++;
    }
    else if (std::string(argv[next]) == "-I") {
	ignore_volume=true;
	next++;
    }
    else if (std::string(argv[next]) == "-R") {
	ignore_restrictions=true;
	next++;
    }
    else if (std::string(argv[next]) == "-t") {
	get_timings=true;
	clock_gettime(CLOCK_MONOTONIC,&times.start);
	next++;
    }
    else if (std::string(argv[next]) == "-T") {
	rinfo=argv[++next];
//	strutils::replace_all(rinfo,"\\!","!");
//	strutils::replace_all(rinfo,"%21","!");
	download_directory="x";
	is_test=true;
    }
  }
  metautils::read_config("subconv","","",false);
  if (!is_test) {
    rqst_index=argv[next++];
    clear_wfrqst();
    download_directory=argv[next++];
  }
  atexit(cleanUp);
  t=new ThreadStruct[MAX_NUM_THREADS];
  p.reserve(MAX_NUM_THREADS);
  for (n=0; n < MAX_NUM_THREADS; n++) {
    t[n].obuffer.reset(new unsigned char[OBUF_LEN]);
    pthread_attr_init(&t[n].tattr);
    pthread_attr_setstacksize(&t[n].tattr,10000000);
    p.emplace_back(new xmlutils::ParameterMapper);
  }
  t_idx=new int[MAX_NUM_THREADS];
  MySQL::Server server_d;
  metautils::connect_to_rdadb_server(server_d);
  if (!server_d) {
    std::cerr << "Error: unable to connect to RDADB (1)" << std::endl;
    exit(1);
  }
  if (!is_test) {
    if (get_timings) {
	clock_gettime(CLOCK_MONOTONIC,&db_times.start);
    }
    server_d.update("dsrqst","enotice='"+download_directory+"/.email_notice',fcount=-1","rindex = "+rqst_index);
    query.set("rinfo,file_format,email,location","dsrqst","rindex = "+rqst_index);
    if (query.submit(server_d) < 0) {
	std::cerr << "Error: " << query.error() << std::endl;
	std::cerr << "Query: " << query.show() << std::endl;
	exit(1);
    }
    if (!query.fetch_row(row)) {
	std::cerr << "Error: no entry in dsrqst for rindex = " << rqst_index << std::endl;
	exit(1);
    }
    rinfo=row[0];
    compression=strutils::to_lower(row[1]);
    if (!compression.empty()) {
	compression="."+compression;
    }
    duser=row[2];
    location=row[3];
  }
  MySQL::Server server_m;
  metautils::connect_to_metadata_server(server_m);
  if (!server_m) {
    std::cerr << "Error: unable to connect to metadata server - 1" << std::endl;
    exit(1);
  }
  if (get_timings) {
    clock_gettime(CLOCK_MONOTONIC,&db_times.end);
    timing_data.db+=elapsed_time(db_times);
  }
  local_args.nlat=local_args.elon=99999.;
  local_args.slat=local_args.wlon=-99999.;
  local_args.ladiff=local_args.lodiff=-99.;
  local_args.ststep=false;
  for (n=0; n < 13; topt_mo[n++]=0);
  includeParameterTable.reset(new my::map<gributils::StringEntry>);
  bool dates_are_init=false;
  auto info_parts=strutils::split(rinfo,";");
  for (auto part : info_parts) {
    strutils::trim(part);
    auto nvp=strutils::split(part,"=");
    if (nvp[0] == "dsnum") {
	args.dsnum=nvp[1];
// restrictions
	if (!is_test && !ignore_restrictions) {
	  ifs.open("/glade/u/home/dattore/util/subconv.conf");
	  if (ifs.is_open()) {
	    ifs.getline(line,256);
	    while (!ifs.eof()) {
		auto lparts=strutils::split(line);
		if (strutils::to_lower(lparts[0]) == strutils::to_lower(duser) && (lparts.size() == 1 || lparts[1] == args.dsnum || lparts[1].empty())) {
		  std::cerr << "Refuse to process data request " << rqst_index << " for " << duser << std::endl;
		  std::cerr << rinfo << std::endl;
		  mysystem2("/glade/u/home/rdadata/bin/dsrqst -sr -ri "+rqst_index+" -rs E",oss,ess);
		  exit(1);
		}
		ifs.getline(line,256);
	    }
	    ifs.close();
	  }
	  query.set("select sum(size_request) from dsrqst where email = '"+duser+"' and status = 'O'");
	  if (query.submit(server_d) == 0 && query.fetch_row(row) && !row[0].empty() && std::stoll(row[0]) > 1000000000000) {
	    if (is_test) {
		std::cout << "Error: quota exceeded - you will need to purge some of your existing requests first" << std::endl;
	    }
	    else {
		std::cerr << "User has exceeded quota" << std::endl;
	    }
	    mysystem2("/glade/u/home/rdadata/bin/dsrqst -sr -ri "+rqst_index+" -rs E",oss,ess);
	    exit(1);
	  }
	}
	dsnum2=strutils::substitute(args.dsnum,".","");
    }
    else if (nvp[0] == "startdate") {
	local_args.startdate=strutils::substitute(nvp[1],"-","");
	strutils::replace_all(local_args.startdate,":","");
	strutils::replace_all(local_args.startdate," ","");
	if (local_args.startdate.length() != 12) {
	  if (is_test) {
	    std::cout << "Error: start date not specified properly" << std::endl;
	    std::cout << "Your request:" << std::endl;
	    std::cout << rinfo << std::endl;
	  }
	  else {
	    std::cerr << "Error: start date not specified properly" << std::endl;
	  }
	  exit(1);
	}
    }
    else if (nvp[0] == "enddate") {
	local_args.enddate=strutils::substitute(nvp[1],"-","");
	strutils::replace_all(local_args.enddate,":","");
	strutils::replace_all(local_args.enddate," ","");
	if (local_args.enddate.length() != 12) {
	  if (is_test) {
	    std::cout << "Error: end date not specified properly" << std::endl;
	    std::cout << "Your request:" << std::endl;
	    std::cout << rinfo << std::endl;
	  }
	  else {
	    std::cerr << "Error: end date not specified properly" << std::endl;
	  }
	  exit(1);
	}
    }
    else if (nvp[0] == "dates" && nvp[1] == "init") {
	dates_are_init=true;
    }
    else if (nvp[0] == "parameters") {
	if (!nvp[1].empty()) {
	  auto params=strutils::split(nvp[1],",");
	  num_parameters=params.size();
	  for (const auto& param : params) {
	    auto pparts=strutils::split(param,"!");
	    if (is_test && pparts.size() != 2) {
		std::cout << "Error: parameter(s) not specified properly" << std::endl;
		std::cout << "Your request:" << std::endl;
		std::cout << rinfo << std::endl;
		exit(1);
	    }
	    if (!uniqueFormatsTable.found(pparts[0],e)) {
		e.key=pparts[0];
		query2.set("format","WGrML.formats","code = "+e.key);
		if (query2.submit(server_m) < 0) {
		  std::cerr << query2.error() << std::endl;
		}
		else if (query2.num_rows() == 1) {
		  query2.fetch_row(row);
		  e.sdum=row[0];
		}
		uniqueFormatsTable.insert(e);
		local_args.formats.push_back(e.key);
	    }
	    local_args.parameters.emplace_back(param);
	    e.key=e.sdum+"<!>"+pparts[1];
	    if (!parameterTable.found(e.key,e)) {
		parameterTable.insert(e);
	    }
	    pparts=strutils::split(param,":");
	    if (pparts.size() == 2) {
		if (!includeParameterTable->found(pparts[1],se)) {
		  se.key=pparts[1];
		  includeParameterTable->insert(se);
		}
	    }
	  }
	}
    }
    else if (nvp[0] == "product") {
	local_args.product=nvp[1];
    }
    else if (nvp[0] == "grid_definition") {
	local_args.grid_definition=nvp[1];
    }
    else if (nvp[0] == "level") {
	local_args.level=nvp[1];
    }
    else if (nvp[0] == "tindex") {
	local_args.tindex=nvp[1];
    }
    else if (nvp[0] == "ofmt") {
	local_args.ofmt=nvp[1];
    }
    else if (nvp[0] == "nlat") {
	if (!nvp[1].empty()) {
	  local_args.lat_s=nvp[1];
	  if (local_args.lat_s.front() == '-') {
	    local_args.lat_s=local_args.lat_s.substr(1)+"S";
	  }
	  else {
	    local_args.lat_s.push_back('N');
	  }
	  local_args.nlat=std::stof(nvp[1]);
	  if (subflag < 4) {
	    subflag+=4;
	  }
	  if (is_test && (local_args.nlat < -90. || local_args.nlat > 90.)) {
	    std::cout << "Error: north latitude (nlat) must be in the range of -90 to 90" << std::endl;
	    std::cout << "Your request:" << std::endl;
	    std::cout << rinfo << std::endl;
	    exit(1);
	  }
	}
    }
    else if (nvp[0] == "slat") {
	if (!nvp[1].empty()) {
	  local_args.slat=std::stof(nvp[1]);
	  if (is_test && (local_args.slat < -90. || local_args.slat > 90.)) {
	    std::cout << "Error: south latitude (slat) must be in the range of -90 to 90" << std::endl;
	    std::cout << "Your request:" << std::endl;
	    std::cout << rinfo << std::endl;
	    exit(1);
	  }
	}
    }
    else if (nvp[0] == "wlon") {
	if (!nvp[1].empty()) {
	  local_args.lon_s=nvp[1];
	  if (local_args.lon_s.front() == '-') {
	    local_args.lon_s=local_args.lon_s.substr(1)+"W";
	  }
	  else {
	    local_args.lon_s.push_back('E');
	  }
	  local_args.wlon=std::stof(nvp[1]);
	  if (is_test && (local_args.wlon < -180. || local_args.wlon > 180.)) {
	    std::cout << "Error: west longitude (wlon) must be in the range of -180 to 180" << std::endl;
	    std::cout << "Your request:" << std::endl;
	    std::cout << rinfo << std::endl;
	    exit(1);
	  }
	}
    }
    else if (nvp[0] == "elon") {
	if (!nvp[1].empty()) {
	  local_args.elon=std::stof(nvp[1]);
	  if (is_test && (local_args.elon < -180. || local_args.elon > 180.)) {
	    std::cout << "Error: east longitude (elon) must be in the range of -180 to 180" << std::endl;
	    std::cout << "Your request:" << std::endl;
	    std::cout << rinfo << std::endl;
	    exit(1);
	  }
	}
    }
    else if (nvp[0] == "ststep" && nvp[1] == "yes") {
	local_args.ststep=true;
    }
    else if (strutils::has_beginning(nvp[0],"topt_mo")) {
	topt_mo[std::stoi(strutils::substitute(nvp[0],"topt_mo",""))]=1;
	topt_mo[0]=1;
    }
  }
  server_d.disconnect();
// grids 62 and 64 were consolidated to 83 for ds093.0 and ds093.1
  if (std::regex_search(args.dsnum,std::regex("^093\\.[01]")) && (local_args.grid_definition == "62" || local_args.grid_definition == "64")) {
    local_args.grid_definition="83";
  }
  if (local_args.nlat < 99990. && local_args.nlat == local_args.slat && local_args.wlon == local_args.elon) {
    ignore_volume=true;
  }
  if (strutils::to_lower(local_args.ofmt) == "csv" && local_args.parameters.size() == 1 && strutils::occurs(local_args.level,",") == 0) {
    auto pparts=strutils::split(local_args.parameters.front(),"!");
    uniqueFormatsTable.found(pparts[0],e);
    csv_parameter=metadata::detailed_parameter(*(p[0]),e.sdum,pparts[1]);
    if ( (idx=csv_parameter.find(" <small")) != std::string::npos) {
	csv_parameter=csv_parameter.substr(0,idx);
    }
    uniqueFormatsTable.found(local_args.formats.front(),e);
    if (local_args.level.empty()) {
	query.set("levelType_codes","WGrML.ds"+dsnum2+"_agrids_cache","parameter = '"+pparts[1]+"'");
	if (query.submit(server_m) == 0 && query.fetch_row(row)) {
	  query.set("map,type,value","WGrML.levels","code = "+row[0].substr(0,row[0].find(":")));
	}
    }
    else {
	query.set("map,type,value","WGrML.levels","code = "+local_args.level);
    }
    if (query.submit(server_m) == 0 && query.num_rows() > 0) {
	query.fetch_row(row);
	csv_level=metadata::detailed_level(level_mapper,e.sdum,row[0],row[1],row[2],false);
	strutils::replace_all(csv_level,"<nobr>","");
	strutils::replace_all(csv_level,"</nobr>","");
    }
  }
  query2.set("select count(distinct parameter) from WGrML.ds"+dsnum2+"_agrids_cache");
  if (query2.submit(server_m) < 0) {
    std::cerr << query2.error() << std::endl;
  }
  else if (query2.num_rows() == 1) {
    query2.fetch_row(row);
    if (num_parameters < std::stoi(row[0])) {
	subflag+=1;
    }
  }
  if (myequalf(local_args.nlat,90.,0.001) && myequalf(local_args.slat,-90.,0.001) && myequalf(local_args.wlon,-180.,0.001) && myequalf(local_args.elon,180.,0.001)) {
    local_args.nlat=local_args.elon=99999.;
    local_args.slat=local_args.wlon=-99999.;
  }
  if (topt_mo[0] == 1) {
    for (n=1; n < 13; n++) {
	if (topt_mo[n] == 1)
	  num_topt_mo++;
    }
    if (num_topt_mo == 12)
	topt_mo[0]=0;
  }
  metautils::connect_to_rdadb_server(server_d);
  if (!server_d) {
    if (is_test) {
	std::cout << "Error: database connection error" << std::endl;
    }
    else {
	std::cerr << "Error: unable to connect to RDADB (2)" << std::endl;
    }
    exit(1);
  }
  query.set("webhome","dataset","dsid = 'ds"+args.dsnum+"'");
  if (query.submit(server_d) < 0) {
    if (is_test)
	std::cout << "Error: database error - '" << query.error() << "'" << std::endl;
    else {
	std::cerr << "Error: " << query.error() << std::endl;
	std::cerr << "Query: " << query.show() << std::endl;
    }
    exit(1);
  }
  if (query.fetch_row(row)) {
    webhome=row[0];
  }
  else {
    if (is_test) {
	std::cout << "Error: bad request" << std::endl;
	std::cout << "Your request:" << std::endl;
	std::cout << rinfo << std::endl;
    }
    else {
	std::cerr << "Error: unable to get webhome for " << args.dsnum << std::endl;
    }
    exit(1);
  }
strutils::replace_all(webhome,"/glade/data02/dsszone","/glade/p/rda/data");
  query.set("wfile,data_size,tindex","wfile","dsid = 'ds"+args.dsnum+"' and property = 'A' and type = 'D'");
  if (query.submit(server_d) < 0) {
    if (is_test) {
	std::cout << "Error: database error - '" << query.error() << "'" << std::endl;
    }
    else {
	std::cerr << "Error: " << query.error() << std::endl;
    }
    exit(1);
  }
  while (query.fetch_row(row)) {
    e.key=row[0];
    e.sdum=row[1];
    e.sdum2=row[2];
    rdafileHash.insert(e);
  }
  server_d.disconnect();
  if (local_args.nlat < 99. && local_args.slat > -99.)
    local_args.ladiff=local_args.nlat-local_args.slat;
  if (local_args.elon < 999. && local_args.wlon > -999.)
    local_args.lodiff=local_args.elon-local_args.wlon;
  if (args.dsnum.empty()) {
    if (is_test) {
	std::cout << "Error: bad request" << std::endl;
	std::cout << "Your request:" << std::endl;
	std::cout << rinfo << std::endl;
    }
    else {
	std::cerr << "Error: no dataset number given" << std::endl;
    }
    exit(1);
  }
  if (!local_args.startdate.empty()) {
    if (!invConditions.empty()) {
	invConditions+=" and ";
    }
    if (dates_are_init) {
	invConditions+="init_date";
    }
    else {
	invConditions+="valid_date";
    }
    invConditions+=" >= '"+local_args.startdate+"'";
  }
  if (!local_args.enddate.empty()) {
    if (!invConditions.empty()) {
	invConditions+=" and ";
    }
    if (dates_are_init) {
	invConditions+="init_date";
    }
    else {
	invConditions+="valid_date";
    }
    invConditions+=" <= '"+local_args.enddate+"'";
  }
  if (!local_args.product.empty()) {
    if (!invConditions.empty()) {
	invConditions+=" and ";
    }
    if (!uConditions_no_dates.empty()) {
	uConditions_no_dates+=" and ";
    }
    if (strutils::occurs(local_args.product,",") > 0) {
	invConditions+="(";
	uConditions_no_dates+="(";
	auto pparts=strutils::split(local_args.product,",");
	auto n=0;
	for (const auto& ppart : pparts) {
	  if (n++ > 0) {
	    invConditions+=" or ";
	    uConditions_no_dates+=" or ";
	  }
	  invConditions+="timeRange_code = "+ppart;
	  uConditions_no_dates+="timeRange_code = "+ppart;
	}
	invConditions+=")";
	uConditions_no_dates+=")";
    }
    else {
	invConditions+="timeRange_code = "+local_args.product;
	uConditions_no_dates+="timeRange_code = "+local_args.product;
    }
  }
  if (!local_args.grid_definition.empty()) {
    if (!invConditions.empty()) {
	invConditions+=" and ";
    }
    if (!uConditions_no_dates.empty()) {
	uConditions_no_dates+=" and ";
    }
    if (strutils::occurs(local_args.grid_definition,",") > 0) {
	invConditions+="(";
	uConditions_no_dates+="(";
	auto gparts=strutils::split(local_args.grid_definition,",");
	auto n=0;
	for (const auto& gpart : gparts) {
	  if (n++ > 0) {
	    invConditions+=" or ";
	    uConditions_no_dates+=" or ";
	  }
	  invConditions+="gridDefinition_code = "+gpart;
	  uConditions_no_dates+="gridDefinition_code = "+gpart;
	}
	invConditions+=")";
	uConditions_no_dates+=")";
    }
    else {
	invConditions+="gridDefinition_code = "+local_args.grid_definition;
	uConditions_no_dates+="gridDefinition_code = "+local_args.grid_definition;
    }
  }
  uConditions=invConditions;
  if (local_args.formats.size() > 0) {
    if (!fmtConditions.empty()) {
	fmtConditions+=" and ";
    }
    fmtConditions+="(";
    auto n=0;
    for (const auto& format : local_args.formats) {
	if (n > 0) {
	  fmtConditions+=" or ";
	}
	fmtConditions+="code = "+format;
	++n;
    }
    fmtConditions+=")";
  }
  if (!local_args.level.empty()) {
    if (strutils::occurs(local_args.level,",") > 0) {
	auto lparts=strutils::split(local_args.level,",");
	for (const auto& lpart : lparts) {
	  query.set("map","WGrML.levels","code = "+lpart);
	  if (query.submit(server_m) < 0) {
	    if (is_test) {
		std::cout << "Error: database error - '" << query.error() << "'" << std::endl;
	    }
	    else {
		std::cerr << "Error: " << query.error() << std::endl;
		std::cerr << "Query: " << query.show() << std::endl;
	    }
	    exit(1);
	  }
	  if (query.num_rows() == 0) {
	    if (is_test) {
		std::cout << "Error: bad request" << std::endl;
		std::cout << "Your request:" << std::endl;
		std::cout << rinfo << std::endl;
	    }
	    else {
		std::cerr << "Error: no entry in WGrML.levels for " << lpart << std::endl;
	    }
	    exit(1);
	  }
	  query.fetch_row(row);
	  e.key=row[0];
	  if (!levelTable.found(e.key,e)) {
	    e.sdum=lpart;
	    levelTable.insert(e);
	  }
	  else {
	    e.sdum+=","+lpart;
	    levelTable.replace(e);
	  }
	}
    }
    else {
	if (!uConditions.empty()) {
	  uConditions+=" and ";
	}
	if (!uConditions_no_dates.empty()) {
	  uConditions_no_dates+=" and ";
	}
	uConditions+="level_code = "+local_args.level;
	uConditions_no_dates+="level_code = "+local_args.level;
	if (!invConditions.empty()) {
	  invConditions+=" and ";
	}
	invConditions+="level_code = "+local_args.level;
    }
  }
  if (local_args.parameters.size() > 0) {
    for (const auto& parameter : local_args.parameters) {
	if (!union_query.empty()) {
	  union_query+=" union ";
	}
	union_query+="select distinct webID_code from IGrML.`ds"+dsnum2+"_inventory_"+parameter+"`";
	if (levelTable.size() > 0) {
	  lvlConditions="";
	  e.key=strutils::token(parameter,".",0);
	  if (strutils::contains(e.key,"!")) {
	    e.key=strutils::token(e.key,"!",1);
	  }
	  if (levelTable.found(e.key,e)) {
	    auto lcodes=strutils::split(e.sdum,",");
	    for (const auto& level_code : lcodes) {
		if (!lvlConditions.empty()) {
		  lvlConditions+=" or ";
		}
		lvlConditions+="level_code = "+level_code;
		if (!uniqueLevelTable.found(level_code,e)) {
		  if (!allLvlConditions.empty()) {
		    allLvlConditions+=" or ";
		  }
		  allLvlConditions+="level_code = "+level_code;
		  e.key=level_code;
		  uniqueLevelTable.insert(e);
		}
	    }
	    lvlConditions="("+lvlConditions+")";
	    if (!uConditions.empty()) {
		lvlConditions=" and "+lvlConditions;
	    }
	  }
	}
	if (!uConditions.empty() || !lvlConditions.empty()) {
	  union_query+=" where ";
	}
	if (!uConditions.empty()) {
	  union_query+=uConditions;
	}
	if (!lvlConditions.empty()) {
	  union_query+=lvlConditions;
	}
    }
    if (!allLvlConditions.empty()) {
	allLvlConditions="("+allLvlConditions+")";
	if (!uConditions.empty())
	  allLvlConditions=" and "+allLvlConditions;
    }
  }
  if (!fmtConditions.empty()) {
    query.set("select code,format from WGrML.formats where "+fmtConditions);
  }
  else {
    query.set("select code,format from WGrML.formats");
  }
  if (query.submit(server_m) < 0) {
    if (is_test) {
	std::cout << "Error: database error - '" << query.error() << "'" << std::endl;
    }
    else {
	std::cerr << "Error: " << query.error() << std::endl;
	std::cerr << "Query: " << query.show() << std::endl;
    }
    exit(1);
  }
  while (query.fetch_row(row)) {
    e.key=row[0];
    e.sdum=row[1];
    formatHash.insert(e);
  }
  if (local_args.ladiff > 0.09 || fabs(local_args.lodiff) > 0.09) {
    query.set("select distinct u.webID_code,webID,format_code,gridDefinition_codes from ("+union_query+") as u left join WGrML.ds"+dsnum2+"_webfiles as w on w.code = u.webID_code left join WGrML.ds"+dsnum2+"_agrids as a on a.webID_code = w.code");
  }
  else {
    query.set("select distinct u.webID_code,webID,format_code from ("+union_query+") as u left join WGrML.ds"+dsnum2+"_webfiles as w on w.code = u.webID_code");
  }
  if (query.submit(server_m) < 0) {
    if (is_test) {
	std::cout << "Error: no files match the request" << std::endl;
	std::cout << "Your request:" << std::endl;
	std::cout << rinfo << std::endl;
    }
    else {
	std::cerr << "Error: " << query.error() << std::endl;
	std::cerr << "Query: " << query.show() << std::endl;
    }
    exit(1);
  }
  if (query.num_rows() == 0) {
    if (is_test) {
	std::cout << "Error: no files match the request" << std::endl;
	std::cout << "Your request:" << std::endl;
	std::cout << rinfo << std::endl;
    }
    else {
	std::cerr << "Error: no files match the request" << std::endl;
	std::cerr << rinfo << std::endl;
    }
    exit(1);
  }
  disp_order=1;
  if (strutils::to_lower(local_args.ofmt) == "netcdf") {
// identify files that have multiple parameters in them
    while (query.fetch_row(row)) {
	if (formatHash.found(row[2],e)) {
	  union_query="";
	  for (const auto& parameter : local_args.parameters) {
	    if (!union_query.empty()) {
		union_query+=" union ";
	    }
	    union_query+="select '"+parameter+"' as p,level_code,timeRange_code from IGrML.`ds"+dsnum2+"_inventory_"+parameter+"` where webID_code = "+row[0]+" and "+invConditions;
	  }
	  query2.set("select distinct p,level_code,timeRange_code from ("+union_query+") as u");
	  if (query2.submit(server_m) < 0) {
	    if (is_test) {
		std::cout << "Error: database error - '" << query2.error() << "'" << std::endl;
	    }
	    else {
		std::cerr << "Error: " << query2.error() << std::endl;
		std::cerr << "Query: " << query2.show() << std::endl;
	    }
	    exit(1);
	  }
	  if (query2.num_rows() > 1) {
	    e.key=row[0];
	    multi_table.insert(e);
	  }
	}
    }
    query.rewind();
  }
  while (query.fetch_row(row)) {
    if (formatHash.found(row[2],e)) {
	format=e.sdum;
	should_attach=true;
	if (local_args.ladiff > 0.09 || fabs(local_args.lodiff) > 0.09) {
	  if (!gridDefinition_bitmaps.found(row[3],be)) {
	    be.key=row[3];
	    be.data.reset(new BitmapEntry::Data);
	    gridDefinition_bitmaps.insert(be);
	    bitmap::uncompress_values(row[3],gridDefinition_values);
	    for (const auto& gd_value : gridDefinition_values) {
		query2.set("definition,defParams","WGrML.gridDefinitions","code = "+strutils::itos(gd_value));
		if (query2.submit(server_m) < 0) {
		  if (is_test) {
		    std::cout << "Error: database error - '" << query2.error() << "'" << std::endl;
		  }
		  else {
		    std::cerr << "Error: " << query2.error() << std::endl;
		    std::cerr << "Query: " << query2.show() << std::endl;
		  }
		  exit(1);
		}
		if (query2.fetch_row(row2)) {
		  if (row2[0] == "latLon") {
		    auto gdef_parts=strutils::split(row2[1],":");
		    grid_dim.x=std::stoi(gdef_parts[0]);
		    grid_dim.y=std::stoi(gdef_parts[1]);
		    grid_def.slatitude=std::stof(gdef_parts[2].substr(0,gdef_parts[2].length()-1));
		    if (gdef_parts[2].back() == 'S') {
			grid_def.slatitude=-grid_def.slatitude;
		    }
		    grid_def.slongitude=std::stof(gdef_parts[3].substr(0,gdef_parts[3].length()-1));
		    if (gdef_parts[3].back() == 'W') {
			grid_def.slongitude=-grid_def.slongitude;
		    }
		    grid_def.elatitude=std::stof(gdef_parts[4].substr(0,gdef_parts[4].length()-1));
		    if (gdef_parts[4].back() == 'S') {
			grid_def.elatitude=-grid_def.elatitude;
		    }
		    grid_def.elongitude=std::stof(gdef_parts[5].substr(0,gdef_parts[5].length()-1));
		    if (gdef_parts[5].back() == 'W') {
			grid_def.elongitude=-grid_def.elongitude;
		    }
		    grid_def.loincrement=std::stof(gdef_parts[6]);
		    grid_def.laincrement=std::stof(gdef_parts[7]);
		    grid_def=fix_grid_definition(grid_def,grid_dim);
		    be.data->ladiffs.emplace_back(grid_def.laincrement);
		    be.data->lodiffs.emplace_back(grid_def.loincrement);
		  }
		  else if (row2[0] == "gaussLatLon") {
		    auto gdef_parts=strutils::split(row2[1],":");
		    grid_dim.x=std::stoi(gdef_parts[0]);
		    grid_dim.y=std::stoi(gdef_parts[1]);
		    grid_def.slatitude=std::stof(gdef_parts[2].substr(0,gdef_parts[2].length()-1));
		    if (gdef_parts[2].back() == 'S') {
			grid_def.slatitude=-grid_def.slatitude;
		    }
		    grid_def.slongitude=std::stof(gdef_parts[3].substr(0,gdef_parts[3].length()-1));
		    if (gdef_parts[3].back() == 'W') {
			grid_def.slongitude=-grid_def.slongitude;
		    }
		    grid_def.elatitude=std::stof(gdef_parts[4].substr(0,gdef_parts[4].length()-1));
		    if (gdef_parts[4].back() == 'S') {
			grid_def.elatitude=-grid_def.elatitude;
		    }
		    grid_def.elongitude=std::stof(gdef_parts[5].substr(0,gdef_parts[5].length()-1));
		    if (gdef_parts[5].back() == 'W') {
			grid_def.elongitude=-grid_def.elongitude;
		    }
		    grid_def.loincrement=std::stof(gdef_parts[6]);
		    grid_def.num_circles=std::stoi(gdef_parts[7]);
		    grid_def=fix_grid_definition(grid_def,grid_dim);
		    fill_gaussian_latitudes(gaus_lats,grid_def.num_circles,(grid_def.slatitude > grid_def.elatitude));
		    if (gaus_lats.found(grid_def.num_circles,gle)) {
			m=l=-1;
			for (n=0; n < static_cast<int>(grid_def.num_circles*2); n++) {
			    if (local_args.nlat <= gle.lats[n]) {
				m=n;
			    }
			    if (local_args.slat <= gle.lats[n]) {
				l=n;
			    }
			}
			if (m > l) {
			  be.data->ladiffs.emplace_back(0.);
			}
			else {
			  be.data->ladiffs.emplace_back(local_args.ladiff*2.);
			}
		    }
		    else {
			be.data->ladiffs.emplace_back(local_args.ladiff*2.);
		    }
		    be.data->lodiffs.emplace_back(grid_def.loincrement);
		  }
		  else {
		    if (is_test) {
			std::cout << "Error: bad request" << std::endl;
			std::cout << "Your request:" << std::endl;
			std::cout << rinfo << std::endl;
		    }
		    else {
			std::cerr << "Error: grid definition " << row2[0] << " not understood" << std::endl;
		    }
		    exit(1);
		  }
		}
	    }
	  }
	  should_attach=false;
	  for (it_lat=be.data->ladiffs.begin(),it_lon=be.data->lodiffs.begin(),end_it=be.data->ladiffs.end(); it_lat != end_it; ++it_lat,++it_lon) {
	    if ( (*it_lat-local_args.ladiff) < 0.0001 || (*it_lon-fabs(local_args.lodiff)) < 0.0001) {
		should_attach=true;
	    }
	  }
	}
	if (should_attach) {
	  rdafileHash.found(row[1],e);
	  if (local_args.tindex.empty() || e.sdum2 == local_args.tindex) {
	    query_results.emplace_back(row[0]+"<!>"+row[1]+"<!>"+e.sdum+"<!>"+format);
	  }
	}
    }
  }
  server_m.disconnect();
  rdafileHash.clear();
  if (query_results.empty()) {
    if (is_test) {
	std::cout << "Error: no data match the request" << std::endl;
	std::cout << "Your request:" << std::endl;
	std::cout << rinfo << std::endl;
    }
    else {
	std::cerr << "Error: no data match the request" << std::endl;
	std::cerr << rinfo << std::endl;
    }
    exit(1);
  }
  if (!allLvlConditions.empty()) {
    uConditions+=allLvlConditions;
  }
// build the files
  if (!is_test) {
    auto fcount=query_results.size();
    if (local_args.ofmt == "csv") {
	++fcount;
    }
    else if (topt_mo[0] == 1) {
	auto nmonths=0;
	for (size_t n=1; n < 13; ++n) {
	  if (topt_mo[n] == 1) {
	    ++nmonths;
	  }
	}
	fcount=fcount/12*nmonths;
    }
    set_fcount(fcount);
  }
  for (n=0; n < MAX_NUM_THREADS; ++n) {
    t_idx[n]=-1;
  }
  num_threads=0;
  thread_index=0;
  for (const auto& res : query_results) {
    while (num_threads == MAX_NUM_THREADS) {
	for (n=0; n < num_threads; ++n) {
	  if (pthread_kill(t[n].tid,0) != 0) {
	    pthread_join(t[n].tid,NULL);
	    for (const auto& fname : t[n].insert_filenames) {
		insert_list.emplace_back(strutils::itos(t[n].disp_order)+"<!>"+t[n].fmt+"<!>"+fname);
insert_into_wfrqst(fname,t[n].fmt,t[n].disp_order);
	    }
	    for (const auto& fname : t[n].wget_filenames) {
		wget_list.emplace_back(fname);
	    }
	    if (!t[n].f_attach.empty()) {
		multi_file_list.emplace_back(t[n].f_attach);
		++num_input;
		size_input+=t[n].size_input;
	    }
	    if (is_test || get_timings) {
		timing_data.add(t[n].timing_data);
	    }
	    if (is_test) {
		write_bytes+=t[n].timing_data.read_bytes;
	    }
	    else {
		write_bytes+=t[n].write_bytes;
	    }
	    if (!ignore_volume && write_bytes > 900000000000) {
		if (is_test) {
		  std::cout << "Error: requested volume is too large" << std::endl;
		}
		else {
		  std::cerr << "Error: request volume too large" << std::endl;
		}
		exit(1);
	    }
	    thread_index=n;
	    t_idx[n]=-1;
	    t[n].timing_data.reset();
	    num_threads--;
	    break;
	  }
	}
    }
    auto res_parts=strutils::split(res,"<!>");
    idx=res_parts[1].rfind("/");
    if (idx != std::string::npos) {
	filename=res_parts[1].substr(idx);
    }
    else {
	filename="/"+res_parts[1];
    }
    strutils::replace_all(filename,".tar","");
    if (res_parts[3] == "WMO_GRIB2") {
	if (!strutils::contains(filename,".grb2") && !strutils::contains(filename,".grib2"))
	  filename+=".grb2";
    }
    t[thread_index].webID_code=res_parts[0];
    t[thread_index].webID=res_parts[1];
    t[thread_index].format=res_parts[3];
    t[thread_index].filename=filename;
    t[thread_index].parameters=local_args.parameters;
    t[thread_index].uConditions=uConditions;
    t[thread_index].uConditions_no_dates=uConditions_no_dates;
    t[thread_index].size_input=std::stoll(res_parts[2]);
    t[thread_index].disp_order=disp_order;
    t[thread_index].f_attach=f_attach;
    t[thread_index].multi_table=multi_table;
    t[thread_index].parameterTable=parameterTable;
    t[thread_index].parameter_mapper=p[thread_index];
    pthread_create(&t[thread_index].tid,&t[thread_index].tattr,build_file,reinterpret_cast<void *>(&t[thread_index]));
    t_idx[thread_index]=0;
    size_input+=std::stoll(res_parts[2]);
    ++thread_index;
    ++num_threads;
    ++disp_order;
  }
  for (n=0; n < MAX_NUM_THREADS; ++n) {
    if (t_idx[n] == 0 && pthread_join(t[n].tid,NULL) == 0) {
	for (const auto& fname : t[n].insert_filenames) {
	  insert_list.emplace_back(strutils::itos(t[n].disp_order)+"<!>"+t[n].fmt+"<!>"+fname);
insert_into_wfrqst(fname,t[n].fmt,t[n].disp_order);
	}
	for (const auto& fname : t[n].wget_filenames) {
	  wget_list.emplace_back(fname);
	}
	if (!t[n].f_attach.empty()) {
	  multi_file_list.emplace_back(t[n].f_attach);
	  ++num_input;
	  size_input+=t[n].size_input;
	}
	if (is_test || get_timings) {
	  timing_data.add(t[n].timing_data);
	}
	if (is_test) {
	  write_bytes+=t[n].timing_data.read_bytes;
	}
	else {
	  write_bytes+=t[n].write_bytes;
	}
	if (!ignore_volume && write_bytes > 900000000000) {
	  if (is_test) {
	    std::cout << "Error: requested volume is too large" << std::endl;
	  }
	  else {
	    std::cerr << "Error: request volume too large" << std::endl;
	  }
	  exit(1);
	}
	t[n].timing_data.reset();
    }
  }
  if (is_test) {
    std::cout << "Success: " << timing_data.num_reads << " grids" << std::endl;
    exit(0);
  }
  for (n=0; n < MAX_NUM_THREADS; ++n) {
    t_idx[n]=-1;
    pthread_attr_destroy(&t[n].tattr);
    t[n].obuffer=nullptr;
  }
  num_threads=0;
  thread_index=0;
  for (const auto& mfile : multi_file_list) {
    while (num_threads == MAX_NUM_THREADS) {
	for (n=0; n < num_threads; ++n) {
	  if (pthread_kill(t[n].tid,0) != 0) {
	    pthread_join(t[n].tid,NULL);
	    for (const auto& fname : t[n].insert_filenames) {
		insert_list.emplace_back(strutils::itos(t[n].disp_order)+"<!>"+t[n].fmt+"<!>"+fname);
insert_into_wfrqst(fname,t[n].fmt,t[n].disp_order);
	    }
	    for (const auto& fname : t[n].wget_filenames) {
		wget_list.emplace_back(fname);
	    }
	    thread_index=n;
	    t_idx[n]=-1;
	    --num_threads;
	    break;
	  }
	}
    }
    t[thread_index].f_attach=mfile;
    t[thread_index].disp_order=disp_order;
    t[thread_index].parameterTable=parameterTable;
    t[thread_index].includeParameterTable=includeParameterTable;
    pthread_create(&t[thread_index].tid,&t[thread_index].tattr,do_conversion,reinterpret_cast<void *>(&t[thread_index]));
    t_idx[thread_index]=0;
    ++thread_index;
    ++num_threads;
    ++disp_order;
  }
  for (n=0; n < MAX_NUM_THREADS; n++) {
    if (t_idx[n] == 0) {
	pthread_join(t[n].tid,NULL);
	for (const auto& fname : t[n].insert_filenames) {
	  insert_list.emplace_back(strutils::itos(t[n].disp_order)+"<!>"+t[n].fmt+"<!>"+fname);
insert_into_wfrqst(fname,t[n].fmt,t[n].disp_order);
	}
	for (const auto& fname : t[n].wget_filenames) {
	  wget_list.emplace_back(fname);
	}
	if (is_test || get_timings) {
	  timing_data.add(t[n].timing_data);
	}
	t[n].timing_data.reset();
    }
  }
  metautils::connect_to_metadata_server(server_m);
  if (!server_m) {
    std::cerr << "Error: unable to connect to metadata server - 2" << std::endl;
    exit(1);
  }
  http_cookie="HTTP_COOKIE=duser="+duser;
  putenv(const_cast<char *>(http_cookie.c_str()));
  putenv(const_cast<char *>("SERVER_NAME=rda.ucar.edu"));
  sdum=download_directory;
  strutils::replace_all(sdum,"/glade/data02/dsstransfer","");
  strutils::replace_all(sdum,"/glade/p/rda/transfer","");
  if (local_args.ofmt == "csv") {
    combine_csv_files(insert_list,wget_list);
  }
  ofs.open((download_directory+"/wget."+rqst_index+".csh").c_str());
  create_wget_script(wget_list,sdum,"csh",&ofs);
  ofs.close();
  ofs.clear();
  ofs.open((download_directory+"/curl."+rqst_index+".csh").c_str());
  create_curl_script(wget_list,sdum,"csh",&ofs);
  ofs.close();
  ofs.clear();
  ofs.open((download_directory+"/.email_notice").c_str());
  ofs << "From: <SENDER>" << std::endl;
  ofs << "To: <RECEIVER>" << std::endl;
  ofs << "Cc: <CCD>" << std::endl;
  ofs << "Subject: Your <DSID> Data Request <RINDEX> is Ready" << std::endl;
  ofs << "The subset of <DSID> - '<DSTITLE>' that you requested is ready for you to download." << std::endl << std::endl;
  note+=std::string("Subset details:")+"\n";
  if (!local_args.startdate.empty() && !local_args.enddate.empty()) {
    note+="  Date range: "+local_args.startdate+" to "+local_args.enddate+"\n";
  }
  if (local_args.ststep) {
    note+="    *Each timestep in its own file\n";
  }
  if (topt_mo[0] == 1) {
    note+="  Include these months only:";
    for (n=1; n <= 12; n++) {
	if (topt_mo[n] == 1) {
	  note+=" "+months[n];
	}
    }
    note+="\n";
  }
  if (local_args.parameters.size() > 0) {
    note+="  Parameter(s): \n";
    for (const auto& parameter : local_args.parameters) {
	auto pparts=strutils::split(parameter,"!");
	uniqueFormatsTable.found(pparts[0],e);
	sdum=metadata::detailed_parameter(*(t[0].parameter_mapper),e.sdum,pparts[1]);
	if ( (idx=sdum.find(" <small")) != std::string::npos) {
	  sdum=sdum.substr(0,idx);
	}
	if (!uniqueParametersTable.found(sdum,e)) {
	  e.key=sdum;
	  uniqueParametersTable.insert(e);
	  note+="     "+sdum+"\n";
	}
    }
  }
  if (!local_args.level.empty()) {
    uniqueLevelTable.clear();
    note+="  Level(s):\n";
    if (local_args.formats.size() > 0) {
	uniqueFormatsTable.found(local_args.formats.front(),e);
	auto levels=strutils::split(local_args.level,",");
	for (const auto& level : levels) {
	  query.set("map,type,value","WGrML.levels","code = "+level);
	  if (query.submit(server_m) == 0 && query.num_rows() > 0) {
	    query.fetch_row(row);
	    sdum=metadata::detailed_level(level_mapper,e.sdum,row[0],row[1],row[2],false);
	    strutils::replace_all(sdum,"<nobr>","");
	    strutils::replace_all(sdum,"</nobr>","");
	    if (!uniqueLevelTable.found(sdum,e)) {
		e.key=sdum;
		uniqueLevelTable.insert(e);
		note+="     "+sdum+"\n";
	    }
	  }
	  else
	    note+="     "+level+"\n";
	}
    }
    else
	note+="     "+local_args.level+"\n";
  }
  if (!local_args.product.empty()) {
    note+="  Product(s):\n";
    auto pparts=strutils::split(local_args.product,",");
    for (const auto& part : pparts) {
	query.set("timeRange","WGrML.timeRanges","code = "+part);
	if (query.submit(server_m) == 0 && query.num_rows() > 0) {
	  query.fetch_row(row);
	  note+="    "+row[0]+"\n";
	}
	else
	  note+="    "+part+"\n";
    }
  }
  if (!local_args.grid_definition.empty()) {
    query.set("definition,defParams","WGrML.gridDefinitions","code = "+local_args.grid_definition);
    if (query.submit(server_m) == 0 && query.num_rows() > 0) {
	query.fetch_row(row);
	local_args.grid_definition=convert_grid_definition(row[0]+"<!>"+row[1]);
	strutils::replace_all(local_args.grid_definition,"&deg;","-deg");
	strutils::replace_all(local_args.grid_definition,"<small>","");
	strutils::replace_all(local_args.grid_definition,"</small>","");
    }
    note+="  Grid: "+local_args.grid_definition+"\n";
  }
  if (!local_args.ofmt.empty()) {
    note+="  Output format conversion: "+local_args.ofmt+"\n";
    rqsttype="T";
  }
  else
    rqsttype="S";
  if (local_args.nlat < 9999. && local_args.elon < 9999. && local_args.slat > -9999. && local_args.wlon > -9999.) {
    if (local_args.nlat == local_args.slat && local_args.wlon == local_args.elon) {
	note+="  Spatial subsetting (single gridpoint):\n";
	note+="    Latitude: "+strutils::ftos(local_args.nlat,8)+"\n";
	note+="    Longitude: "+strutils::ftos(local_args.wlon,8)+"\n";
    }
    else {
	note+="  Spatial subsetting (bounding box):\n";
	note+="    Latitudes (top/bottom): "+strutils::ftos(local_args.nlat,0)+" / "+strutils::ftos(local_args.slat,0)+"\n";
	note+="    Longitudes (left/right): "+strutils::ftos(local_args.wlon,0)+" / "+strutils::ftos(local_args.elon,0)+"\n";
    }
  }
  ofs << note << std::endl;
  if (get_timings) {
    clock_gettime(CLOCK_MONOTONIC,&db_times.start);
  }
  metautils::connect_to_rdadb_server(server_d);
  if (!server_d) {
    std::cerr << "Error: unable to connect to RDADB (3)" << std::endl;
    exit(1);
  }
  if (server_d.insert("wfrqst","rindex,disp_order,data_format,file_format,wfile,type,status",rqst_index+",-1,NULL,NULL,'wget."+rqst_index+".csh','S','O'","update disp_order=-1,data_format=NULL") < 0) {
    std::cerr << "Error (insert): " << server_d.error() << std::endl;
    exit(1);
  }
  if (server_d.insert("wfrqst","rindex,disp_order,data_format,file_format,wfile,type,status",rqst_index+",-1,NULL,NULL,'curl."+rqst_index+".csh','S','O'","update disp_order=-1,data_format=NULL") < 0) {
    std::cerr << "Error (insert): " << server_d.error() << std::endl;
    exit(1);
  }
  if (determined_temporal_subsetting) {
	subflag+=2;
  }
  if (server_d.update("dsrqst","fcount = "+strutils::itos(num_input)+", size_input = "+strutils::lltos(size_input)+", rqsttype = '"+rqsttype+"', note = '"+strutils::substitute(note,"'","\\'")+"', subflag = "+strutils::itos(subflag),"rindex = "+rqst_index) < 0) {
    std::cerr << "Update error: " << server_d.error() << std::endl;
    exit(1);
  }
/*
  for (const auto& item : insert_list) {
    auto parts=strutils::split(item,"<!>");
    if (server_d.insert("wfrqst","rindex,disp_order,data_format,file_format,wfile,status",rqst_index+","+parts[0]+",'"+parts[1]+"','','"+parts[2]+"','O'","update disp_order="+parts[0]+",data_format='"+parts[1]+"'") < 0) {
	std::cerr << "Insert error: " << server_d.error() << std::endl;
	exit(1);
    }
  }
*/
  server_d.disconnect();
  if (get_timings) {
    clock_gettime(CLOCK_MONOTONIC,&db_times.end);
    timing_data.db+=elapsed_time(db_times);
  }
  if (location.empty()) {
    ofs << "You will need to be signed in to the RDA web server at https://rda.ucar.edu/.  Then you will find your data at:  <DSSURL><WHOME>/<RQSTID>/" << std::endl;
    ofs << std::endl;
    ofs << "Your data will remain on our system for <DAYS> days.  If this is not sufficient time for you to retrieve your data, you can extend the time from the user dashboard (link appears at the top of our web pages when you are signed in). Expand the \"Customized Data Requests\" section to manage your data requests." << std::endl;
  }
  else {
    ofs << "You will find your data in:  <WHOME>/<RQSTID>/" << std::endl;
  }
  ofs << std::endl;
  ofs << "If you have any questions related to this data request, please let me know by replying to this email." << std::endl;
  ofs << std::endl;
  ofs << "Sincerely," << std::endl;
  ofs << "<SPECIALIST>" << std::endl;
  ofs << "NCAR/CISL RDA" << std::endl;
  ofs << "<PHONENO>" << std::endl;
  ofs << "<SENDER>" << std::endl;
  ofs.close();
  server_m.disconnect();
  if (get_timings) {
    clock_gettime(CLOCK_MONOTONIC,&times.end);
    std::cout << "Date/time: " << current_date_time().to_string("%Y-%m-%d %H:%MM:%SS %Z") << std::endl;
    std::cout << "Host: " << strutils::token(host_name(),".",0) << std::endl;
    std::cout << "# of threads: " << MAX_NUM_THREADS << std::endl;
    std::cout << "Total wallclock time: " << elapsed_time(times) << " seconds" << std::endl;
    std::cout << "Total database time: " << timing_data.db << " seconds" << std::endl;
    std::cout << "Total time in threads: " << timing_data.thread << " seconds" << std::endl;
    std::cout << "Total read time: " << timing_data.read << " seconds" << std::endl;
    std::cout << "  Total bytes read: " << timing_data.read_bytes << " bytes" << std::endl;
    std::cout << "  Read rate: " << timing_data.read_bytes/1000000./timing_data.read << " MB/sec" << std::endl;
    std::cout << "  Average record length: " << static_cast<double>(timing_data.read_bytes)/timing_data.num_reads << " bytes" << std::endl;
    std::cout << "Total write time: " << timing_data.write << " seconds" << std::endl;
    std::cout << "Total GRIB2 uncompress time: " << timing_data.grib2u << " seconds" << std::endl;
    std::cout << "Total GRIB2 compress time: " << timing_data.grib2c << " seconds" << std::endl;
    std::cout << "Total netCDF conversion time: " << timing_data.nc << " seconds" << std::endl;
  }
  if (!myerror.empty()) {
    exit(1);
  }
  else {
    exit(0);
  }
}
