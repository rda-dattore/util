#include <iomanip>
#include <fstream>
#include <unordered_map>
#include <algorithm>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <utime.h>
#include <errno.h>
#include <string>
#include <list>
#include <regex>
#include <thread>
#include <strutils.hpp>
#include <utils.hpp>
#include <datetime.hpp>

const size_t DEFAULT_NUM_JOBS=1;
const size_t MAX_NUM_JOBS=4;
struct Args {
  Args() : server(),resource(),local_name(),glob(),mtime(0),num_jobs(DEFAULT_NUM_JOBS),time_wait(1),num_to_retrieve(0xffffffff),quiet(false),verbose(false),use_remote_times(true),list_only(false),count_only(false),show_diffs(false),no_clobber(false),name_is_globbed(false) {}

  std::string server,resource,local_name;
  std::regex glob;
  long long mtime;
  size_t num_jobs,time_wait,num_to_retrieve;
  bool quiet,verbose,use_remote_times,list_only,count_only,show_diffs,no_clobber,name_is_globbed;
} args;
struct ThreadStruct {
  ThreadStruct() : status(-1),sock(),sock_server(),tid(),resource(),local_name(),size(0),modify(0),retrieved(false) {}

  int status,sock;
  struct sockaddr_in sock_server;
  pthread_t tid;
  std::string resource,local_name;
  off_t size;
  long long modify;
  bool retrieved;
};
int timeout=5;

extern "C" void *t_connect(void *t)
{
  ThreadStruct *ts=(ThreadStruct *)t;
  auto num_tries=0;
  while (num_tries < 3) {
    if ( (ts->status=connect(ts->sock,(struct sockaddr *)&(ts->sock_server),sizeof(ts->sock_server))) < 0) {
	switch errno {
	  case 106:
	  {
// Transport endpoint is already connected
	    ts->status=0;
	    break;
	  }
	}
    }
    if (ts->status < 0) {
	++num_tries;
	if (num_tries == 3) {
	  std::cerr << "Error: unable to connect to server in passive mode - errno: " << errno << " sockfd: " << ts->sock << " tid: " << ts->tid << std::endl;
	  exit(1);
	}
	else {
	  std::this_thread::sleep_for(std::chrono::seconds(num_tries*15));
	}
    }
    else {
	num_tries=3;
    }
  }
  return nullptr;
}

std::string clean(std::string string)
{
  strutils::replace_all(string,"\r\n","\n");
  strutils::replace_all(string,"\r","\\r");
  strutils::trim(string);
  return string;
}

void getMessages(int sock,std::vector<std::string>& messages,int flags = 0)
{
  const size_t BUF_LEN=32768;
  char buffer[BUF_LEN];
  std::string message;

  messages.clear();
  auto num_bytes=recv(sock,reinterpret_cast<void *>(buffer),BUF_LEN,flags);
  if (num_bytes < 0) {
    messages.emplace_back("Error");
  }
  else {
    while (num_bytes > 0) {
	message.assign(buffer,num_bytes);
	if (args.verbose) {
	  std::cout << clean(message) << std::endl;
	}
	messages.emplace_back(message);
	num_bytes=recv(sock,reinterpret_cast<void *>(buffer),BUF_LEN,MSG_DONTWAIT);
    }
    std::this_thread::sleep_for(std::chrono::seconds(args.time_wait));
    num_bytes=recv(sock,reinterpret_cast<void *>(buffer),BUF_LEN,MSG_DONTWAIT);
    while (num_bytes > 0) {
	message.assign(buffer,num_bytes);
	if (args.verbose) {
	  std::cout << clean(message) << std::endl;
	}
	messages.emplace_back(message);
	num_bytes=recv(sock,reinterpret_cast<void *>(buffer),BUF_LEN,MSG_DONTWAIT);
    }
  }
}

int passive(ThreadStruct ts,std::string& error)
{
  std::string request="PASV\r\n";
  if (args.verbose) {
    std::cout << "> " << request;
  }
  size_t ntries=0;
  const size_t MAX_TRIES=3;
  std::string port_message;
  while (ntries < MAX_TRIES) {
    send(ts.sock,request.c_str(),request.length(),0);
    std::vector<std::string> messages;
    getMessages(ts.sock,messages);
    for (const auto& msg : messages) {
	if (std::regex_search(msg,std::regex("^227 "))) {
// get passive port
	  port_message=msg;
	  ntries=MAX_TRIES;
	  break;
	}
    }
    ++ntries;
    if (ntries < MAX_TRIES) {
	std::this_thread::sleep_for(std::chrono::seconds(ntries*15));
    }
  }
  if (port_message.empty()) {
    error="unable to get passive port number";
    return -1;
  }
  auto mparts=strutils::split(port_message,",");
  ts.sock_server.sin_port=htons(std::stoi(mparts[4])*256+std::stoi(mparts[5]));
  memset(&ts.sock_server.sin_zero,0,8);
  if ( (ts.sock=socket(AF_INET,SOCK_STREAM,0)) < 0) {
    error="unable to enter passive mode";
    return -1;
  }
  int status;
  if ( (status=pthread_create(&ts.tid,nullptr,t_connect,reinterpret_cast<void *>(&ts))) != 0) {
    std::cerr << "Error creating connection thread: " << status << std::endl;
    exit(1);
  }
  auto tm1=time(nullptr);
  auto tm2=tm1;
  while ( (tm2-tm1) < timeout) {
    if (pthread_kill(ts.tid,0) != 0) {
	break;
    }
    tm2=time(nullptr);
  }
  if ( (tm2-tm1) >= timeout) {
    error="timeout while trying to connect to server in passive mode";
    return -1;
  }
  pthread_join(ts.tid,nullptr);
  return ts.sock;
}

void mod_time(int sock,std::string resource,struct utimbuf& times)
{
  std::string request="MDTM "+resource+"\r\n";
  if (args.verbose) {
    std::cout << "> " << request;
  }
  send(sock,request.c_str(),request.length(),0);
  std::vector<std::string> messages;
  getMessages(sock,messages);
  std::string mdtm_message;
  for (const auto& msg : messages) {
    if (std::regex_search(msg,std::regex("^213"))) {
	mdtm_message=msg;
	break;
    }
  }
  if (std::regex_search(mdtm_message,std::regex("^213"))) {
    auto mdtm_parts=strutils::split(mdtm_message);
    if (mdtm_parts.size() > 1) {
	times.actime=static_cast<time_t>(DateTime(std::stoll(mdtm_parts[1])).seconds_since(DateTime(1970,1,1,0,0)));
	times.modtime=times.actime;
    }
  }
  else {
    times.actime=0;
    times.modtime=times.actime;
  }
}

void list(const ThreadStruct& ts,std::deque<std::tuple<std::string,off_t,long long>> *filelist)
{
  std::string error;
  auto dsock=passive(ts,error);
  std::string request;
  if (args.list_only) {
    if (ts.resource.back() == '/') {
	request="LIST "+ts.resource+"\r\n";
    }
    else {
	request="NLST "+ts.resource+"\r\n";
    }
  }
  else {
    if (ts.resource.back() == '/') {
	request="MLSD "+ts.resource+"\r\n";
    }
    else {
	request="MLST "+ts.resource+"\r\n";
    }
  }
  if (args.verbose) {
    std::cout << "> " << request;
  }
  send(ts.sock,request.c_str(),request.length(),0);
  std::vector<std::string> messages;
  getMessages(ts.sock,messages);
  std::string directory_listing;
  if (std::regex_search(request,std::regex("^(LIST|MLSD|NLST)")) && std::regex_search(messages[0],std::regex("^150"))) {
    FILE *fp;
    if ( (fp=fdopen(dsock,"r")) == nullptr) {
	std::cerr << "Error opening data connection" << std::endl;
	exit(1);
    }
    const size_t BUF_LEN=32768;
    char buffer[BUF_LEN];
    int num_bytes;
    while ( (num_bytes=fread(buffer,1,BUF_LEN,fp)) > 0) {
	directory_listing+=std::string(buffer,num_bytes);
    }
    fclose(fp);
  }
  else if (std::regex_search(request,std::regex("^MLST")) && std::regex_search(messages[0],std::regex("^250"))) {
    directory_listing=messages[1];
  }
  if (directory_listing.length() > 0) {
    if (args.verbose || (args.list_only && !args.name_is_globbed)) {
	std::cout << directory_listing << std::endl;
    }
    if (!args.list_only || args.name_is_globbed) {
	strutils::replace_all(directory_listing,"\r\n","\n");
	strutils::trim(directory_listing);
	auto dlines=strutils::split(directory_listing,"\n");
	for (const auto& dline : dlines) {
	  if (args.list_only) {
	    auto dparts=strutils::split(dline);
	    if (std::regex_search(dparts.back(),args.glob)) {
		std::cout << dline << std::endl;
	    }
	  }
	  else {
	    if (std::regex_search(dline,std::regex("type=file;",std::regex::icase))) {
		auto facts=strutils::split(dline,";");
		std::string filename=facts.back();
		strutils::trim(filename);
		if (filename[0] == '/' && filename != args.resource) {
		  filename=filename.substr(filename.rfind("/")+1);
		}
		off_t size=0;
		long long modify=0;
		for (const auto& fact : facts) {
		  if (std::regex_search(fact,std::regex("^size=",std::regex::icase))) {
		    size=std::stoi(fact.substr(5));
		  }
		  else if (std::regex_search(fact,std::regex("^modify=",std::regex::icase))) {
		    modify=std::stoll(fact.substr(7));
		  }
		}
		if (modify >= args.mtime && (!args.name_is_globbed || std::regex_search(filename,args.glob))) {
		  if (args.resource.back() == '/') {
		    filelist->emplace_back(std::make_tuple(args.resource+filename,size,modify));
		  }
		  else if (filename == args.resource) {
		    filelist->emplace_back(std::make_tuple(args.resource,size,modify));
		  }
		  else {
		    filelist->emplace_back(std::make_tuple(args.resource+"/"+filename,size,modify));
		  }
		  if (args.show_diffs) {
		    struct stat buf;
		    if (stat(filename.c_str(),&buf) == 0) {
			if (buf.st_size != size) {
			  std::cout << filename << ": file sizes differ" << std::endl;
			}
		    }
		    else {
			std::cout << filename << ": No such local file" << std::endl;
		    }
		  }
		}
	    }
	  }
	}
	if (args.count_only && !args.list_only) {
	  if (filelist->size() > 0 || ts.resource.back() == '/') {
	    std::cout << filelist->size() << std::endl;
	    exit(1);
	  }
	}
    }
  }
  else {
    exit(1);
  }
}

bool retrieve(const ThreadStruct& ts)
{
  std::vector<std::string> messages;
  std::string request;
  struct stat buf;
  if (args.mtime > 0 && stat(ts.local_name.c_str(),&buf) == 0) {
    if (ts.size == buf.st_size) {
	return false;
    }
  }
  int dsock;
  std::string error;
  auto num_tries=0;
  while (num_tries < 3) {
    if ( (dsock=passive(ts,error)) < 0) {
	++num_tries;
	std::this_thread::sleep_for(std::chrono::seconds(num_tries*15));
    }
    else {
	break;
    }
  }
  if (dsock < 0) {
    std::cerr << "Error: " << error << std::endl;
    exit(1);
  }
/*
  struct utimbuf times;
  if (args.use_remote_times) {
    mod_time(ts.sock,ts.resource,times);
  }
*/
  request="RETR "+ts.resource+"\r\n";
  if (args.verbose) {
    std::cout << "> " << request;
  }
  send(ts.sock,request.c_str(),request.length(),0);
  getMessages(ts.sock,messages);
  std::string retrieve_message;
  for (const auto& msg : messages) {
    if (std::regex_search(msg,std::regex("^150 "))) {
	retrieve_message=msg;
	break;
    }
  }
  if (retrieve_message.length() > 0) {
    FILE *fp;
    if ( (fp=fdopen(dsock,"r")) == nullptr) {
	std::cerr << "Error opening data connection" << std::endl;
	exit(1);
    }
    std::ofstream ofs;
    ofs.open(ts.local_name.c_str());
    if (!ofs) {
	std::cerr << "Error opening output file " << ts.local_name << std::endl;
	exit(1);
    }
    int num_bytes;
    char buffer[32768];
    while ( (num_bytes=fread(buffer,1,32768,fp)) > 0) {
	ofs.write(buffer,num_bytes);
    }
    fclose(fp);
    ofs.close();
    getMessages(ts.sock,messages);
    if (args.use_remote_times) {
	if (ts.modify > 0) {
	  struct utimbuf times;
	  times.actime=static_cast<time_t>(DateTime(ts.modify).seconds_since(DateTime(1970,1,1,0,0)));
	  times.modtime=times.actime;
	  utime(ts.local_name.c_str(),&times);
	}
	else {
	  std::cout << "Warning: unable to set remote time for " << ts.local_name << std::endl;
	}
    }
    if (!args.quiet) {
	std::cout << "Saved " << ts.local_name << std::endl;
    }
    return true;
  }
  else {
    return false;
  }
}

extern "C" void *t_retrieve(void *t)
{
  ThreadStruct *ts=(ThreadStruct *)t;
  ts->retrieved=retrieve(*ts);
  return nullptr;
}

bool getSocket(ThreadStruct& ts,const struct hostent *hp,std::string& error)
{
// open communication with the server
  if ( (ts.sock=socket(AF_INET,SOCK_STREAM,0)) < 0) {
    error="unable to open communication with server";
    return false;
  }
  if (args.verbose) {
    std::cout << "Communications opened with server" << std::endl;
  }
  char **p=hp->h_addr_list;
  struct in_addr in;
//  memcpy(&in.s_addr,*p,sizeof(in.s_addr));
std::copy(&p[0][0],&p[0][sizeof(in.s_addr)],&(reinterpret_cast<unsigned char *>(&in.s_addr)[0]));
  ts.sock_server.sin_addr.s_addr=in.s_addr;
  ts.sock_server.sin_family=AF_INET;
  ts.sock_server.sin_port=htons(21);
  pthread_create(&ts.tid,nullptr,t_connect,reinterpret_cast<void *>(&ts));
  auto tm1=time(nullptr);
  auto tm2=tm1;
  while ( (tm2-tm1) < timeout) {
    if (pthread_kill(ts.tid,0) != 0) {
	break;
    }
    tm2=time(nullptr);
  }
  if ( (tm2-tm1) >= timeout) {
    error="unable to connect to server";
    return false;
  }
  else {
    pthread_join(ts.tid,nullptr);
  }
  if (ts.status < 0) {
    error="unable to connect to server";
    return false;
  }
  if (args.verbose) {
    std::cout << "Connected successfully" << std::endl;
  }
  std::vector<std::string> messages;
  getMessages(ts.sock,messages);
  std::string request="USER anonymous\r\n";
  if (args.verbose) {
    std::cout << "> " << request;
  }
  send(ts.sock,request.c_str(),request.length(),0);
  getMessages(ts.sock,messages);
   auto can_continue=false;
   auto requires_password=false;
   for (const auto& msg : messages) {
     if (std::regex_search(msg,std::regex("^(331|230)"))) {
	can_continue=true;
	if (std::regex_search(msg,std::regex("^331"))) {
	  requires_password=true;
	}
	break;
    }
  }
  if (!can_continue) {
    error="unable to login";
    return false;
  }
  if (requires_password) {
    std::string user=getenv("USER");
    if (user.empty()) {
	std::cerr << "Error: unable to determine email address to send as password - set your $USER environment variable" << std::endl;
	exit(1);
    }
    request="PASS "+user+"@ucar.edu\r\n";
    if (args.verbose) {
	std::cout << "> " << request;
    }
    send(ts.sock,request.c_str(),request.length(),0);
    getMessages(ts.sock,messages);
    can_continue=false;
    for (const auto& msg : messages) {
	if (std::regex_search(msg,std::regex("^230"))) {
	  can_continue=true;
	  break;
	}
    }
  }
  if (!can_continue) {
    error="login failed";
    return false;
  }
  if (args.verbose) {
    std::cout << "Login successful" << std::endl;
  }
  request="TYPE I\r\n";
  if (args.verbose) {
    std::cout << "> " << request;
  }
  send(ts.sock,request.c_str(),request.length(),0);
  getMessages(ts.sock,messages);
  ts.resource=args.resource;
  return true;
}

int main(int argc,char **argv)
{
  if (argc < 2) {
    std::cerr << "usage: " << argv[0] << " [options] ftp://<remote-server>/<remote-path>" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "-c               count only - don't download files" << std::endl;
    std::cerr << "--diff           compare remote and local resources" << std::endl;
    std::cerr << "-l <local_name>  download remote file to local_name (by default, " << argv[0] << " uses the" << std::endl;
    std::cerr << "                 remote name)" << std::endl;
    std::cerr << "-j <num>         number of jobs (threads) - default " << DEFAULT_NUM_JOBS << ", maximum " << MAX_NUM_JOBS << std::endl;
    std::cerr << "-L               listing only - don't download files" << std::endl;
    std::cerr << "-m/-M            save files with remote (default)/current modification time" << std::endl;
    std::cerr << "-N <num>         maximum number of files to retrieve" << std::endl;
    std::cerr << "-nc              no-clobber - don't download files that already exist" << std::endl;
    std::cerr << "-t <time>        only download files modified on or after <time>" << std::endl;
    std::cerr << "                 time (UTC) is specified as YYYY[MM[DD[hh[mm[ss]]]]], where year" << std::endl;
    std::cerr << "                 YYYY is required and all other fields are optional" << std::endl;
    std::cerr << "-T               time to wait for messages from ftp server (default 1) - set" << std::endl;
    std::cerr << "                 this higher for servers that take longer to respond" << std::endl;
    std::cerr << "-V               verbose output" << std::endl;
    exit(1);
  }
  auto arglist=strutils::split(unixutils::unix_args_string(argc,argv,'!'),"!");
  for (size_t n=0; n < arglist.size()-1; n++) {
    if (arglist[n] == "-c") {
	args.count_only=true;
    }
    else if (arglist[n] == "--diff") {
	args.list_only=args.show_diffs=true;
    }
    else if (arglist[n] == "-l") {
	args.local_name=arglist[++n];
    }
    else if (arglist[n] == "-j") {
	args.num_jobs=std::stoi(arglist[++n]);
	if (args.num_jobs > MAX_NUM_JOBS) {
	  args.num_jobs=MAX_NUM_JOBS;
	}
    }
    else if (arglist[n] == "-L") {
	args.list_only=true;
    }
    else if (arglist[n] == "-M") {
	args.use_remote_times=false;
    }
    else if (arglist[n] == "-N") {
	args.num_to_retrieve=std::stoi(arglist[++n]);
    }
    else if (arglist[n] == "-nc") {
	args.no_clobber=true;
    }
    else if (arglist[n] == "-t") {
	n++;
	if (arglist[n].length() < 4 || arglist[n].length() > 14 || (arglist[n].length() % 2) != 0 || !strutils::is_numeric(arglist[n])) {
	  std::cerr << "Error in time specification" << std::endl;
	  exit(1);
	}
	while (arglist[n].length() < 14) {
	  arglist[n]+="00";
	}
	args.mtime=std::stoll(arglist[n]);
    }
    else if (arglist[n] == "-T") {
	args.time_wait=std::stoi(arglist[++n]);
    }
    else if (arglist[n] == "-V") {
	args.verbose=true;
    }
  }
  args.server=arglist.back();
  if (!std::regex_search(args.server,std::regex("^ftp://"))) {
    std::cerr << "Error: invalid ftp specification - must begin with 'ftp://'" << std::endl;
    exit(1);
  }
  size_t idx;
  if ( (idx=args.server.substr(6).find("/")) != std::string::npos) {
    args.resource=args.server.substr(idx+6);
/*
    if (std::regex_search(args.resource,std::regex("/$"))) {
	strutils::chop(args.resource);
    }
*/
    args.server=args.server.substr(6,idx);
  }
  else {
    args.resource="/";
    args.server=args.server.substr(6);
  }
  if (std::regex_search(args.resource,std::regex("\\*"))) {
    auto idx=args.resource.rfind("/")+1;
    auto glob="^"+args.resource.substr(idx)+"$";
    strutils::replace_all(glob,".","\\.");
    strutils::replace_all(glob,"*","(.*)");
    args.glob.assign(glob);
    args.name_is_globbed=true;
    args.resource=args.resource.substr(0,idx);
  }
// look up server
  struct hostent *hp;
  if ( (hp=gethostbyname(args.server.c_str())) == nullptr) {
    std::cerr << "Error: lookup failed for server " << args.server << std::endl;
    exit(1);
  }
  if (args.verbose) {
    std::cout << "Found server " << args.server << std::endl;
  }
  std::unique_ptr<ThreadStruct[]> cts(new ThreadStruct[args.num_jobs]);
  std::string error;
  if (!getSocket(cts[0],hp,error)) {
    std::cerr << "Error: " << error << std::endl;
    exit(1);
  }
  if (args.list_only) {
    list(cts[0],nullptr);
  }
  else {
    std::deque<std::tuple<std::string,off_t,long long>> filelist;
    list(cts[0],&filelist);
    if (filelist.empty() && cts[0].resource.back() != '/') {
	cts[0].resource+="/";
	list(cts[0],&filelist);
    }
    size_t num_retrieved=0;
    if (args.num_jobs > 1) {
	for (size_t n=1; n < args.num_jobs; ++n) {
	  if (!getSocket(cts[n],hp,error)) {
	    args.num_jobs=n;
	    std::cout << "Warning: number of jobs adjusted to " << args.num_jobs << " because of server limitations" << std::endl;
	  }
	}
	if (args.num_to_retrieve <= args.num_jobs) {
	  args.num_to_retrieve=1;
	}
	else {
	  args.num_to_retrieve-=(args.num_jobs-1);
	}
	auto fileno=0;
	size_t num_jobs=0;
	auto tidx=0;
	while (!filelist.empty()) {
	  while (num_jobs == args.num_jobs) {
	    for (size_t n=0; n < args.num_jobs; ++n) {
		if (pthread_kill(cts[n].tid,0) != 0) {
		  pthread_join(cts[n].tid,nullptr);
		  if (cts[n].retrieved) {
		    ++num_retrieved;
		  }
		  cts[n].status=-1;
		  tidx=n;
		  --num_jobs;
		  break;
		}
	    }
	  }
	  if (num_retrieved == args.num_to_retrieve) {
	    break;
	  }
	  cts[tidx].resource=std::get<0>(filelist.front());
	  if (args.local_name.empty()) {
	    cts[tidx].local_name=cts[tidx].resource.substr(cts[tidx].resource.rfind("/")+1);
	  }
	  else {
	    cts[tidx].local_name=args.local_name+"."+strutils::itos(fileno);
	  }
	  struct stat buf;
	  if (args.no_clobber && stat(cts[tidx].local_name.c_str(),&buf) == 0) {
	    if (args.verbose) {
		std::cout << "Skipping " << cts[tidx].local_name << " - already exists" << std::endl;
	    }
	  }
	  else {
	    cts[tidx].size=std::get<1>(filelist.front());
	    cts[tidx].modify=std::get<2>(filelist.front());
	    if ( (cts[tidx].status=pthread_create(&cts[tidx].tid,nullptr,t_retrieve,reinterpret_cast<void *>(&cts[tidx]))) != 0) {
		std::cerr << "Error creating thread - tidx: " << tidx << " status: " << cts[tidx].status << std::endl;
		exit(1);
	    }
	    ++tidx;
	    ++num_jobs;
	    ++fileno;
	  }
	  filelist.pop_front();
	}
	for (size_t n=0; n < args.num_jobs; ++n) {
	  if (cts[n].status == 0) {
	    pthread_join(cts[n].tid,nullptr);
	  }
	}
    }
    else {
	auto n=0;
	for (const auto& facts : filelist) {
	  cts[0].resource=std::get<0>(facts);
	  if (args.local_name.empty()) {
	    cts[0].local_name=cts[0].resource.substr(cts[0].resource.rfind("/")+1);
	  }
	  else {
	    cts[0].local_name=args.local_name+"."+strutils::itos(n);
	  }
	  struct stat buf;
	  if (args.no_clobber && stat(cts[0].local_name.c_str(),&buf) == 0) {
	    std::cout << "Skipping " << cts[0].local_name << " - already exists" << std::endl;
	  }
	  else {
	    cts[0].size=std::get<1>(facts);
	    cts[0].modify=std::get<2>(facts);
	    if (retrieve(cts[0])) {
		++num_retrieved;
	    }
	    if (num_retrieved == args.num_to_retrieve) {
		exit(0);
	    }
	    ++n;
	  }
	}
    }
  }
/*
  if (may_be_directory) {
// may be a directory
    size_t num_retrieved=0;
    std::deque<std::pair<std::string,off_t>> filelist;
    if (args.num_jobs > 1) {
	for (size_t n=1; n < args.num_jobs; ++n) {
	  if (!getSocket(cts[n],hp,error)) {
	    args.num_jobs=n;
	    std::cout << "Warning: number of jobs adjusted to " << args.num_jobs << " because of server limitations" << std::endl;
	  }
	}
	if (args.num_to_retrieve <= args.num_jobs) {
	  args.num_to_retrieve=1;
	}
	else {
	  args.num_to_retrieve-=(args.num_jobs-1);
	}
	auto fileno=0;
	size_t num_jobs=0;
	auto tidx=0;
	while (!filelist.empty()) {
	  while (num_jobs == args.num_jobs) {
	    for (size_t n=0; n < args.num_jobs; ++n) {
		if (pthread_kill(cts[n].tid,0) != 0) {
		  pthread_join(cts[n].tid,nullptr);
		  if (cts[n].retrieved) {
		    ++num_retrieved;
		  }
		  cts[n].status=-1;
		  tidx=n;
		  --num_jobs;
		  break;
		}
	    }
	  }
	  if (num_retrieved == args.num_to_retrieve) {
	    break;
	  }
	  cts[tidx].resource=filelist.front().first;
	  if (args.local_name.empty()) {
	    cts[tidx].local_name=cts[tidx].resource.substr(cts[tidx].resource.rfind("/")+1);
	  }
	  else {
	    cts[tidx].local_name=args.local_name+"."+strutils::itos(fileno);
	  }
	  cts[tidx].size=filelist.front().second;
	  filelist.pop_front();
	  struct stat buf;
	  if (args.no_clobber && stat(cts[tidx].local_name.c_str(),&buf) == 0) {
	    if (args.verbose) {
		std::cout << "Skipping " << cts[tidx].local_name << " - already exists" << std::endl;
	    }
	  }
	  else {
	    if ( (cts[tidx].status=pthread_create(&cts[tidx].tid,nullptr,t_retrieve,reinterpret_cast<void *>(&cts[tidx]))) != 0) {
		std::cerr << "Error creating thread - tidx: " << tidx << " status: " << cts[tidx].status << std::endl;
		exit(1);
	    }
	    ++tidx;
	    ++num_jobs;
	    ++fileno;
	  }
	}
	for (size_t n=0; n < args.num_jobs; ++n) {
	  if (cts[n].status == 0) {
	    pthread_join(cts[n].tid,nullptr);
	  }
	}
    }
    else {
	auto n=0;
	for (const auto& pair : filelist) {
	  cts[0].resource=pair.first;
	  if (args.local_name.empty()) {
	    cts[0].local_name=pair.first.substr(pair.first.rfind("/")+1);
	  }
	  else {
	    cts[0].local_name=args.local_name+"."+strutils::itos(n);
	  }
	  cts[0].size=pair.second;
	  if (retrieve(cts[0])) {
	    ++num_retrieved;
	  }
	  if (num_retrieved == args.num_to_retrieve) {
	    exit(0);
	  }
	  ++n;
	}
    }
  }
  else {
    if (args.list_only) {
	list(cts[0],nullptr);
    }
    else {
	if (args.local_name.empty()) {
	  cts[0].local_name=args.resource.substr(args.resource.rfind("/")+1);
	}
	else {
	  cts[0].local_name=args.local_name;
	}
	retrieve(cts[0]);
    }
  }
*/
}
