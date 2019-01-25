#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <math.h>

template <class Type>
class LinkedList {
public:
  struct Node {
    Type data;
    Node *previous,*next;
  };

  LinkedList();
  LinkedList(const LinkedList &source);
  ~LinkedList();
  void operator =(const LinkedList& source);
  void operator +=(const LinkedList& source);
  void advance(size_t num=1);
  void append(const Type& entry);
  void attach(const Type& entry);
  void backwardFind(const Type& match);
  void backwardReplace(const Type& match,const Type& replace);
  void clear();
  void clearList();
  void clearMark() { mark=NULL; }
  void forwardFind(const Type& match);
  void forwardReplace(const Type& match,const Type& replace);
  Type &getCurrent() const;
  size_t getLength() const { return list_length; }
  void goEnd() { cursor=tail; }
  void goMark() { cursor=mark; }
  void goStart() { cursor=head; }
  void insert(const Type& entry);
  bool isAtStart() const { return (cursor == head); }
  bool isAtEnd() const { return (cursor == tail); }
  bool isCurrent() const { return (cursor != NULL); }
  bool isMarked() const { return (mark != NULL); }
  void removeCurrent();
  void remove(const Type& match);
  void retreat(size_t num=1);
  void setMark() { mark=cursor; }
  void sort(int (*compare)(Type& left,Type& right));

/*
#if defined(__SUNPRO_CC_COMPAT)
  template <class Type> friend LinkedList<Type> sublist(const LinkedList<Type>& source,size_t length);
#else
  friend LinkedList<Type> sublist<Type>(const LinkedList<Type>& source,size_t length);
#endif
*/
template <class Type1> friend LinkedList<Type> sublist(const LinkedList<Type>& source,size_t length);

private:
  size_t listClear(Node*& root);
  void listAttach(const Type& entry);
  void listForwardFind(const Type& match);
  void listBackwardFind(const Type& match);
  bool isNode(const Node *node) const { return (node != NULL); }

  Node *head,*tail,*cursor,*mark;
  size_t list_length;
};

// template begins

template <class Type>
LinkedList<Type>::LinkedList()
{
  head=tail=cursor=mark=NULL;
  list_length=0;
}

template <class Type>
LinkedList<Type>::LinkedList(const LinkedList &source)
{
  head=tail=cursor=mark=NULL;
  list_length=0;

  *this=source;
}

template <class Type>
LinkedList<Type>::~LinkedList()
{
  listClear(head);
  head=tail=cursor=mark=NULL;
}

template <class Type>
void LinkedList<Type>::advance(size_t num)
{
  size_t n=0;

  while (isCurrent() && n < num) {
    cursor=cursor->next;
    n++;
  }
}

template <class Type>
void LinkedList<Type>::retreat(size_t num)
{
  size_t n=0;

  while (isCurrent() && n < num) {
    cursor=cursor->previous;
    n++;
  }
}

template <class Type>
void LinkedList<Type>::sort(int (*compare)(Type& left,Type& right))
{
  Type *array=new Type[list_length];
  int n=0,m;

  goStart();
  while (isCurrent()) {
    array[n++]=getCurrent();
    advance();
  }
  clearList();
  binarySort(array,n,compare);
  for (m=0; m < n; m++)
    attach(array[m]);
  delete[] array;
}

template <class Type>
void LinkedList<Type>::append(const Type& entry)
{
  setMark();
  goEnd();
  attach(entry);
  goMark();
  clearMark();
}

template <class Type>
void LinkedList<Type>::attach(const Type& entry)
{
  listAttach(entry);

// set up any other necessary links 
  if (head == NULL && tail == NULL)
// attached to an empty list
    head=tail=cursor;
  else if (tail->next != NULL)
// attached to end of list
    tail=cursor;
  else if (cursor->previous == NULL) {
// attached to end of list, but cursor was off end of list
    cursor->previous=tail;
    tail->next=cursor;
    tail=cursor;
  }
}

template <class Type>
void LinkedList<Type>::insert(const Type& entry)
{
  Node *last_cursor=cursor;

// create and insert a new node to the linked list
  if (isCurrent())
    cursor=cursor->previous;
  listAttach(entry);

  if (head == NULL && tail == NULL)
// inserted into an empty list
    head=tail=cursor;
  else if (last_cursor == head) {
// inserted at head of list
    cursor->next=head;
    head->previous=cursor;
    head=cursor;
  }
  else if (last_cursor == NULL) {
// inserted at end of list, but cursor was off end of list
    cursor->previous=tail;
    tail->next=cursor;
    tail=cursor;
  }
}

template <class Type>
void LinkedList<Type>::removeCurrent()
{
  Node *remove=cursor;

  if (!isCurrent())
    return;

  cursor=cursor->next;

// removing head node
  if (remove == head) {
    head=cursor;
    if (isCurrent())
	cursor->previous=NULL;
    else
	tail=NULL;
  }
// removing tail node
  else if (remove == tail) {
    tail=remove->previous;
    (remove->previous)->next=NULL;
  }
// removing inner node
  else {
    (remove->previous)->next=cursor;
    cursor->previous=remove->previous;
  }

  delete remove;
  list_length--;
}

template <class Type>
void LinkedList<Type>::remove(const Type& match)
{
  Node *save=cursor;

  cursor=head;
  while (isCurrent() && cursor->data != match)
    cursor=cursor->next;
  if (isCurrent())
    LinkedList<Type>::removeCurrent();
  else
    cursor=save;
}

template <class Type>
void LinkedList<Type>::clear()
{
  list_length-=listClear(cursor);
  if (isCurrent())
    tail=cursor;
  else
    head=tail=cursor=mark=NULL;
}

template <class Type>
void LinkedList<Type>::clearList()
{
  list_length-=listClear(head);
  if (list_length != 0) {
    std::cerr << "error clearing list" << std::endl;
    exit(-1);
  }
  head=tail=cursor=mark=NULL;
}

template <class Type>
void LinkedList<Type>::forwardFind(const Type& match)
{
// if already at tail, can not go forward
  if (!isCurrent())
    return;

  listForwardFind(match);
}

template <class Type>
void LinkedList<Type>::forwardReplace(const Type& match,const Type& replace)
{
// if already at tail, can not go forward
  if (!isCurrent())
    return;

  listForwardFind(match);
  if (isCurrent())
    cursor->data=replace;
}

template <class Type>
void LinkedList<Type>::backwardFind(const Type& match)
{
// if already at head, can not go backward
  if (cursor == head) {
    cursor=NULL;
    return;
  }

  listBackwardFind(match);
}

template <class Type>
void LinkedList<Type>::backwardReplace(const Type& match,const Type& replace)
{
// if already at head, can not go backward
  if (cursor == head) {
    cursor=NULL;
    return;
  }

  listBackwardFind(match);
  if (isCurrent())
    cursor->data=replace;
}

template <class Type>
Type &LinkedList<Type>::getCurrent() const
{
  if (!isCurrent()) {
    std::cerr << "Error: off end of list " << this << std::endl;
    exit(1);
  }
  return cursor->data;
}

template <class Type>
void LinkedList<Type>::operator =(const LinkedList& source)
{
  Node *source_cursor,*current=NULL;

// deal with the self-assignment case
  if (this == &source)
    return;
  listClear(head);
  head=tail=cursor=mark=NULL;
  list_length=0;
  source_cursor=source.head;
  while (source_cursor != NULL) {
    attach(source_cursor->data);
    if (source_cursor == source.cursor) current=cursor;
    if (source_cursor == source.mark) mark=cursor;
    source_cursor=source_cursor->next;
  }
  cursor=current;
}

template <class Type>
void LinkedList<Type>::operator +=(const LinkedList& source)
{
  Node *source_cursor;

  source_cursor=source.head;
  while (source_cursor != NULL) {
    attach(source_cursor->data);
    source_cursor=source_cursor->next;
  }
}

template <class Type>
void LinkedList<Type>::listAttach(const Type& entry)
{
  Node *new_node;

// create new node
  if ( (new_node=new Node) == NULL) {
    std::cerr << "listAttach: dynamic memory allocation error" << std::endl;
    exit(-1);
  }
  list_length++;
// copy entry to new node
  new_node->data=entry;
// attach the new node after the current node in the list
  if (isCurrent()) {
    new_node->next=cursor->next;
    new_node->previous=cursor;
    if (isNode(cursor->next))
      (cursor->next)->previous=new_node;
    cursor->next=new_node;
  }
  else
    new_node->next=new_node->previous=NULL;
// make the new node the current node in the list
  cursor=new_node;
}

template <class Type>
size_t LinkedList<Type>::listClear(Node*& root)
{
  size_t num_cleared=0;
  Node *save,*remove;

  if (!isNode(root))
    return 0;
  save=root->previous;
  while (isNode(root)) {
    remove=root;
    root=root->next;
    delete remove;
    num_cleared++;
  }
  root=save;
  if (isNode(root))
    root->next=NULL;
  return num_cleared;
}

template <class Type>
void LinkedList<Type>::listForwardFind(const Type& match)
{
  while (isCurrent() && cursor->data != match)
    cursor=cursor->next;
}

template <class Type>
void LinkedList<Type>::listBackwardFind(const Type& match)
{
  while (isCurrent() && cursor->data != match)
    cursor=cursor->previous;
}

template <class Type>
LinkedList<Type> sublist(const LinkedList<Type>& source,size_t length)
{
  LinkedList<Type> sub_list;
  size_t n=0;
  typename LinkedList<Type>::Node *source_cursor;

  source_cursor=source.cursor;
  while (source_cursor != NULL && n < length) {
    sub_list.attach(source_cursor->data);
    source_cursor=source_cursor->next;
    n++;
  }

  return sub_list;
}

template <class Type>
bool operator ==(LinkedList<Type>& source1,LinkedList<Type>& source2)
{
  if (source1.getLength() != source2.getLength())
    return false;
  source1.goStart();
  source2.goStart();
  while (source1.isCurrent()) {
    if (source1.getCurrent() != source2.getCurrent())
	return false;
    source1.advance();
    source2.advance();
  }
  return true;
}

template <class Type>
bool operator !=(LinkedList<Type>& source1,LinkedList<Type>& source2)
{
  return !(source1 == source2);
}

class String
{
public:
  static const char start_of_header[2];
  static const char end_of_text[2];
  static const char tab[2];
  static const char newline[2];
  static const char carriage_return[2];

  static const size_t XMLTag;

  String();
  String(const char *source);
  String(const char *source,size_t num_chars);
  String(const String& source);
  ~String();
  bool beginsWith(const char *string) const;
  bool beginsWith(const String& source) const { return beginsWith(source.toChar()); }
  String capitalized() const;
  char charAt(size_t index) const;
  void chop();
  void chop(size_t num_chars);
  void chopWhiteSpace();
  bool contains(const char *source) const;
  bool contains(const String& source) const;
  void convertUnicode();
  bool endsWith(char character) const;
  bool endsWith(const char *string) const;
  void fill(const char *source);
  void fill(const char *source,size_t num_chars);
  void finalizeBuffer() { ptr_to_string[len]='\0'; }
  char *getAddressOf(size_t index) const;
  char *getBufferReference(const size_t length);
  size_t getLength() const { return len; }
  int getLengthAsInt() const { return len; }
  String getToken(const String separator,size_t token_number);
  bool hasNoLetters() const;
  bool hasNoVowels() const;
  int indexOf(const char *string,int start = 0) const;
  void insert(const char *source) { insertAt(0,source); }
  void insertAt(size_t index,const char *source);
  bool isAlpha() const;
  int isAt(const char *string) const;
  bool isNumeric() const;
  bool isUpperCase() const;
  bool lettersAreUpperCase() const;
  size_t occurs(const char *source) const;
  size_t occurs(const String& source) const;
  void operator =(const String& source);
  void operator =(const char *source);
  void operator +=(const String& source);
//  char *operator +=(const char *source);
void operator +=(const char *source);
  bool replace(const char *old_string,const char *new_string);
  bool replace(const String& old_string,const char *new_string) { return replace(old_string.toChar(),new_string); }
  bool replace(const String& old_string,const String& new_string) { return replace(old_string.toChar(),new_string.toChar()); }
  String soundex() const;
/*
  size_t split(const char *separator,String **array) const;
  size_t split(size_t StringType,String **array) const;
  size_t split(const String separator,String **array) const { return split(separator.toChar(),array); }
*/
  String substitute(const char *old_string,const char *new_string) const;
  String substitute(const String& old_string,const char *new_string) const { return substitute(old_string.toChar(),new_string); }
  String substitute(const String& old_string,const String& new_string) const { return substitute(old_string.toChar(),new_string.toChar()); }
  String substr(size_t start_index,size_t num_chars) const;
  String substr(size_t start_index) const { return substr(start_index,len+1); }
  String substr(const char *start_with,size_t num_chars) const;
  String substr(size_t start_index,const char *end_with) const;
  const char *toChar() const;
  String toLower() const;
  String toLower(size_t start_index,size_t num_chars) const;
  String toUpper() const;
  String toUpper(size_t start_index,size_t num_chars) const;
  void trim();
  void trimBack();
  void trimFront();
  bool truncate(const char from_here_to_end);

  friend std::ostream& operator <<(std::ostream& out_stream,const String& source);
  friend String operator +(const String& source1,const String& source2);
  friend bool operator ==(const String& source1,const String& source2);
  friend bool operator !=(const String& source1,const String& source2);
  friend bool operator >(const String& source1,const String& source2);
  friend bool operator >=(const String& source1,const String& source2);
  friend bool operator <(const String& source1,const String& source2);
  friend bool operator <=(const String& source1,const String& source2);

protected:
  void cat(const char *source);

  size_t len;
  char *ptr_to_string;
};

class StringBuffer : public String
{
public:
  static const size_t buffer_increment;

  StringBuffer(size_t initial_capacity = 0);
  StringBuffer(const char *source,size_t initial_capacity = 0);
  StringBuffer(const String& source,size_t initial_capacity = 0);
  void fill(const char *source);
  size_t getCapacity() const { return capacity; }
  void operator =(const String& source);
  void operator =(const char *source);
/*
  StringBuffer operator +=(const StringBuffer& source);
  StringBuffer operator +=(const String& source);
  char *operator +=(const char *source);
*/
void operator +=(const StringBuffer& source);
void operator +=(const String& source);
void operator +=(const char *source);

private:
  void cat(const char *source);

  size_t capacity;
};

class StringParts
{
public:
  StringParts();
  StringParts(const String& string,const char *separator = "");
  StringParts(const String& string,size_t StringType);
  ~StringParts();
  void fill(const String& string,const char *separator = "");
  void fill(const String& string,size_t StringType);
  size_t getLength() const { return num_parts; }
  size_t getLengthOfPart(size_t part_number) const;
  const String& getPart(size_t part_number) const;

private:
  size_t capacity,num_parts;
  String *parts,empty_part;
};

const char String::start_of_header[2]={0x1,0};
const char String::end_of_text[2]={0x3,0};
const char String::tab[2]={0x9,0};
const char String::newline[2]={0xa,0};
const char String::carriage_return[2]={0xd,0};
const size_t String::XMLTag=1;

String::String()
{
  len=0;
  ptr_to_string=NULL;
}

String::String(const char *source)
{
  len=0;
  ptr_to_string=NULL;
  fill(source);
}

String::String(const char *source,size_t num_chars)
{
  len=0;
  ptr_to_string=NULL;
  fill(source,num_chars);
}

String::String(const String& source)
{
  len=0;
  ptr_to_string=NULL;
  *this=source;
}

String::~String()
{
  if (ptr_to_string != NULL) {
    delete[] ptr_to_string;
    ptr_to_string=NULL;
  }
}

bool String::beginsWith(const char *string) const
{
  size_t cmp_len;

  if (len == 0 || string == NULL)
    return false;
  cmp_len=strlen(string);
  if (cmp_len > len)
    return false;
  if (strncmp(ptr_to_string,string,cmp_len) == 0)
    return true;
  return false;
}

String String::capitalized() const
{
  String c=*this;
  int index;

  if (this->len > 0) {
    if (c.ptr_to_string[0] >= 'a' && c.ptr_to_string[0] <= 'z')
	c.ptr_to_string[0]-=32;
    while (c.contains("_")) {
	index=c.isAt("_");
	if (index < 0)
	  break;
	c.ptr_to_string[index]=' ';
	if ( (size_t)(index+1) < c.len && c.ptr_to_string[index+1] >= 'a' && c.ptr_to_string[index+1] <= 'z')
	  c.ptr_to_string[index+1]-=32;
    }
  }
  return c;
}

char String::charAt(size_t index) const
{
  if (len == 0 || index >= len)
    return '\0';
  else
    return ptr_to_string[index];
}

void String::chop()
{
  if (len == 0)
    return;
  len--;
  ptr_to_string[len]='\0';
}

void String::chop(size_t num_chars)
{
  size_t n;

  for (n=0; n < num_chars; n++)
    chop();
}

void String::chopWhiteSpace()
{
  while (len > 0 && ptr_to_string[len-1] == ' ')
    chop();
}

bool String::contains(const char *source) const
{
  size_t cmp_len;
  int n,num_cmps;

  if (source == NULL)
    return false;
  cmp_len=strlen(source);
  if (len >= cmp_len) {
    num_cmps=len-cmp_len+1;
    for (n=0; n < num_cmps; n++) {
	if (strncmp(source,&ptr_to_string[n],cmp_len) == 0)
	  return true;
    }
    return false;
  }
  else
    return false;
}

bool String::contains(const String& source) const
{
  return this->contains(source.ptr_to_string);
}

void String::convertUnicode()
{
  size_t n;
  char *temp;

  for (n=0; n < len; n++) {
    if ((unsigned char)ptr_to_string[n] == 0xc2 || (unsigned char)ptr_to_string[n] == 0xc3) {
	temp=new char[len-n];
	strcpy(temp,&ptr_to_string[n+1]);
	switch ((unsigned char)ptr_to_string[n+1]) {
	  case 0x80:
	  case 0x81:
	  case 0x82:
	  case 0x83:
	  case 0x84:
	  case 0x85:
	    temp[0]='A';
	    break;
	  case 0x87:
	    temp[0]='C';
	    break;
	  case 0x88:
	  case 0x89:
	  case 0x8a:
	  case 0x8b:
	    temp[0]='E';
	    break;
	  case 0x8c:
	  case 0x8d:
	  case 0x8e:
	  case 0x8f:
	    temp[0]='I';
	    break;
	  case 0x91:
	    temp[0]='N';
	    break;
	  case 0x92:
	  case 0x93:
	  case 0x94:
	  case 0x95:
	  case 0x96:
	  case 0x98:
	    temp[0]='O';
	    break;
	  case 0x99:
	  case 0x9a:
	  case 0x9b:
	  case 0x9c:
	    temp[0]='U';
	    break;
	  case 0x9d:
	    temp[0]='Y';
	    break;
	  case 0xa0:
	  case 0xa1:
	  case 0xa2:
	  case 0xa3:
	  case 0xa4:
	  case 0xa5:
	    temp[0]='a';
	    break;
	  case 0xa7:
	    temp[0]='c';
	    break;
	  case 0xa8:
	  case 0xa9:
	  case 0xaa:
	  case 0xab:
	    temp[0]='e';
	    break;
	  case 0xac:
	  case 0xad:
	  case 0xae:
	  case 0xaf:
	    temp[0]='i';
	    break;
	  case 0xb1:
	    temp[0]='n';
	    break;
	  case 0xb2:
	  case 0xb3:
	  case 0xb4:
	  case 0xb5:
	  case 0xb6:
	  case 0xb8:
	    temp[0]='o';
	    break;
	  case 0xb9:
	  case 0xba:
	  case 0xbb:
	  case 0xbc:
	    temp[0]='u';
	    break;
	  case 0xbd:
	  case 0xbf:
	    temp[0]='y';
	    break;
	}
	strcpy(&ptr_to_string[n],temp);
	len=strlen(ptr_to_string);
    }
  }
}

bool String::endsWith(char character) const
{
  if (len == 0)
    return false;

  if (ptr_to_string[len-1] == character)
    return true;
  else
    return false;
}

bool String::endsWith(const char *string) const
{
  size_t cmp_len;

  if (len == 0 || string == NULL)
    return false;
  cmp_len=strlen(string);
  if (cmp_len > len)
    return false;
  if (strncmp(&ptr_to_string[len-cmp_len],string,cmp_len) == 0)
    return true;
  return false;
}

void String::fill(const char *source)
{
  size_t n;

  if (ptr_to_string != NULL)
    delete[] ptr_to_string;
  if (source == NULL) {
    len=0;
    ptr_to_string=NULL;
  }
  else {
    len=strlen(source);
    if (len > 0) {
	ptr_to_string=new char[len+1];
	for (n=0; n < len; n++)
	  ptr_to_string[n]=source[n];
	ptr_to_string[len]='\0';
    }
    else
	ptr_to_string=NULL;
  }
}

void String::fill(const char *source,size_t num_chars)
{
  size_t n;
  char *temp;

  if (ptr_to_string != NULL)
    delete[] ptr_to_string;
  if (source == NULL) {
    len=0;
    ptr_to_string=NULL;
  }
  else {
// need this because *source could be this string or a subset of it
    temp=new char[num_chars+1];
    for (n=0; n < num_chars; n++)
	temp[n]=source[n];
    temp[num_chars]='\0';
    len=strlen(temp);
    if (len > 0) {
	ptr_to_string=new char[len+1];
	for (n=0; n < len; n++)
	  ptr_to_string[n]=temp[n];
	ptr_to_string[len]='\0';
    }
    else
	ptr_to_string=NULL;
    delete[] temp;
  }
}

char *String::getAddressOf(size_t index) const
{
  if (index < len)
    return &(ptr_to_string[index]);
  else
    return NULL;
}

String String::getToken(const String separator,size_t token_number)
{
  String token;
  StringParts sp;

  if (len == 0)
    return token;
  if (separator.getLength() == 0)
    sp.fill(*this);
  else
    sp.fill(*this,separator.toChar());
  if (token_number < sp.getLength())
    token=sp.getPart(token_number);
  return token;
}

bool String::hasNoLetters() const
{
  size_t n;

  for (n=0; n < len; n++) {
    if ((ptr_to_string[n] >= 'A' && ptr_to_string[n] <= 'Z') || (ptr_to_string[n] >= 'a' && ptr_to_string[n] <= 'z'))
	return false;
  }
  return true;
}

bool String::hasNoVowels() const
{
  size_t n;
  char c;

  for (n=0; n < len; n++) {
    c=ptr_to_string[n];
    if (c == 'a' || c == 'A' || c == 'e' || c == 'E' || c == 'i' || c == 'I' || c == 'o' || c == 'O' || c == 'u' || c == 'U')
	return false;
    if (ptr_to_string[n] == 'y') {
    }
  }
  return true;
}

int String::indexOf(const char *string,int start) const
{
  size_t cmp_len=strlen(string);
  int n;

  if (start >= 0) {
    if ( (start+cmp_len) > len)
	return -1;
    for (n=start; n <= (int)(len-cmp_len); n++) {
	if (strncmp(string,&ptr_to_string[n],cmp_len) == 0)
	  return n;
    }
  }
  else {
    start=-start;
    if (start < 0)
	return -1;
    if (start >= (int)len)
	start=len-1;
    if ((size_t)start > (len+cmp_len-1))
	start=len-cmp_len+1;
    for (n=start; n >= 0; n--) {
	if (strncmp(string,&ptr_to_string[n],cmp_len) == 0)
	  return n;
    }
  }
  return -1;
}

void String::insertAt(size_t index,const char *source)
{
  char *new_string;

  new_string=new char[len+strlen(source)+1];
  strncpy(new_string,ptr_to_string,index);
  new_string[index]='\0';
  strcat(new_string,source);
  strcat(new_string,&ptr_to_string[index]);
  this->fill(new_string);
  delete[] new_string;
}

bool String::isAlpha() const
{
  size_t n;

  for (n=0; n < len; n++) {
    if ((ptr_to_string[n] < 'A' || ptr_to_string[n] > 'Z') && (ptr_to_string[n] < 'a' || ptr_to_string[n] > 'z'))
	return false;
  }
  return true;
}

int String::isAt(const char *string) const
{
  size_t cmp_len=strlen(string);
  int n;

  if (len < cmp_len)
    return -1;
  for (n=0; n <= (int)(len-cmp_len); n++) {
    if (strncmp(string,&ptr_to_string[n],cmp_len) == 0)
	return n;
  }
  return -1;
}

bool String::isNumeric() const
{
  size_t n;

  for (n=0; n < len; n++) {
    if (ptr_to_string[n] < '0' || ptr_to_string[n] > '9')
	return false;
  }
  return true;
}

bool String::isUpperCase() const
{
  size_t n;

  for (n=0; n < len; n++) {
    if (ptr_to_string[n] < 'A' || ptr_to_string[n] > 'Z')
	return false;
  }
  return true;
}

bool String::lettersAreUpperCase() const
{
  size_t n;
  bool foundLetter=false;

  for (n=0; n < len; n++) {
    if (ptr_to_string[n] >= 'a' && ptr_to_string[n] <= 'z')
	return false;
    else if (ptr_to_string[n] >= 'A' && ptr_to_string[n] <= 'Z')
	foundLetter=true;
  }
  return foundLetter;
}

size_t String::occurs(const char *source) const
{
  int sourceLen=strlen(source);
  int checkLen=len-sourceLen;
  size_t numOccurrences=0;
  int n;

  for (n=0; n <= checkLen; n++) {
    if (strncmp(&ptr_to_string[n],source,sourceLen) == 0) {
	numOccurrences++;
	n+=(sourceLen-1);
    }
  }
  return numOccurrences;
}

size_t String::occurs(const String& source) const
{
  return this->occurs(source.ptr_to_string);
}

void String::operator =(const String& source)
{
  if (this == &source)
    return;
  fill(source.toChar());
}

void String::operator =(const char *source)
{
  fill(source);
}


void String::operator +=(const String& source)
{
  cat(source.toChar());
}

/*
char *String::operator +=(const char *source)
{
  cat(source);
  return ptr_to_string;
}
*/
void String::operator +=(const char *source)
{
  cat(source);
}

char *String::getBufferReference(const size_t length)
{
  if (ptr_to_string != NULL)
    delete[] ptr_to_string;
  len=length;
  ptr_to_string=new char[len+1];
  return ptr_to_string;
}

String String::soundex() const
{
  String head,tail;
  int n;

  if (this->len == 0 || !this->isAlpha())
    return "";
  head=this->substr((size_t)0,1);
  head=head.toUpper();
  tail=this->substr(1);
  tail=tail.toUpper();
  tail.replace("A","");
  tail.replace("E","");
  tail.replace("I","");
  tail.replace("O","");
  tail.replace("U","");
  tail.replace("H","");
  tail.replace("W","");
  tail.replace("Y","");
  tail.replace("B","1");
  tail.replace("F","1");
  tail.replace("P","1");
  tail.replace("V","1");
  tail.replace("C","2");
  tail.replace("G","2");
  tail.replace("J","2");
  tail.replace("K","2");
  tail.replace("Q","2");
  tail.replace("S","2");
  tail.replace("X","2");
  tail.replace("Z","2");
  tail.replace("D","3");
  tail.replace("T","3");
  tail.replace("L","4");
  tail.replace("M","5");
  tail.replace("N","5");
  tail.replace("R","6");
  for (n=len-1; n > 0; n--) {
    if (tail.ptr_to_string[n] == tail.ptr_to_string[n-1])
	tail.ptr_to_string[n]='*';
  }
  tail.replace("*","");
  while (tail.len < 3)
    tail+="0";
  return head+tail;
}

/*
size_t String::split(const char *separator,String **array) const
{
  size_t num_parts;
  char **parts;
  size_t n;

  if (len == 0)
    return 0;
  num_parts=strsplit(ptr_to_string,separator,&parts);
  *array=new String[num_parts];
  for (n=0; n < num_parts; n++) {
    (*array)[n].fill(parts[n]);
    delete[] parts[n];
  }
  if (num_parts > 0)
    delete[] parts;
  return num_parts;
}

size_t String::split(size_t StringType,String **array) const
{
  int bindex,eindex;
  String temp(*this);
  LinkedList<String> parts;
  size_t n;

  if (len == 0)
    return 0;
  switch (StringType) {
    case XMLTag:
// the first tag should be at the beginning of the string
	bindex=temp.isAt("<");
	if (bindex != 0)
	  return 0;
	else {
	  while (bindex >= 0) {
	    if (bindex == 0) {
		eindex=temp.isAt(">");
		parts.attach(temp.substr((size_t)0,eindex+1));
		temp=temp.substr(eindex+1,32768);
	    }
	    else {
		parts.attach(temp.substr((size_t)0,bindex));
		temp=temp.substr(bindex,32768);
	    }
	    bindex=temp.isAt("<");
	  }
	}
	break;
  }
  if (parts.getLength() > 0) {
    *array=new String[parts.getLength()];
    parts.goStart();
    n=0;
    while (parts.isCurrent()) {
	(*array)[n++]=parts.getCurrent();
	parts.advance();
    }
  }
  return parts.getLength();
}
*/

String String::substr(size_t start_index,size_t num_chars) const
{
  String new_string;
  char *sub_string=new char[num_chars+1];

  if (num_chars > len)
    num_chars=len;
  strncpy(sub_string,&ptr_to_string[start_index],num_chars);
  sub_string[num_chars]='\0';
  new_string.fill(sub_string);
  delete[] sub_string;
  return new_string;
}

String String::substr(const char *start_with,size_t num_chars) const
{
  size_t cmp_len=strlen(start_with);
  int n,start_index=-1;

  for (n=0; n <= (int)(len-cmp_len); n++) {
    if (strncmp(start_with,&ptr_to_string[n],cmp_len) == 0) {
	start_index=n;
	n=len;
    }
  }
  if (start_index != -1)
    return substr(start_index,num_chars);
  else
    return String();
}

String String::substr(size_t start_index,const char *end_with) const
{
  size_t cmp_len=strlen(end_with);
  int n,num_chars=0;

  for (n=start_index; n <= (int)(len-cmp_len); n++) {
    if (strncmp(end_with,&ptr_to_string[n],cmp_len) == 0) {
	num_chars=n+1-start_index;
	n=len;
    }
  }
  if (num_chars > 0)
    return substr(start_index,num_chars);
  else
    return String();
}

void String::cat(const char *source)
{
  char *temp;
  size_t new_len;

  if (source == NULL)
    return;
  if (len > 0) {
    temp=new char[len+1];
    strcpy(temp,ptr_to_string);
    if (ptr_to_string != NULL)
	delete[] ptr_to_string;
    new_len=len+strlen(source);
    ptr_to_string=new char[new_len+1];
    strcpy(ptr_to_string,temp);
    strcat(ptr_to_string,source);
    len=new_len;
    delete[] temp;
  }
  else {
    len=strlen(source);
    ptr_to_string=new char[len+1];
    strcpy(ptr_to_string,source);
  }
}

const char *String::toChar() const
{
  if (len > 0)
    return ptr_to_string;
  else
    return NULL;
}

String String::toLower() const
{
  String new_string;

  new_string=toLower(0,len);
  return new_string;
}

String String::toLower(size_t start_index,size_t num_chars) const
{
  String new_string;
  size_t n;

  if (len == 0)
    return new_string;
  new_string=*this;
  for (n=0; n < num_chars; n++) {
    if (new_string.ptr_to_string[n+start_index] >= 'A' && new_string.ptr_to_string[n] <= 'Z')
	new_string.ptr_to_string[n]+=32;
  }
  return new_string;
}

String String::toUpper() const
{
  String new_string;

  new_string=toUpper(0,len);
  return new_string;
}

String String::toUpper(size_t start_index,size_t num_chars) const
{
  String new_string;
  size_t n;

  if (len == 0)
    return new_string;
  new_string=*this;
  for (n=0; n < num_chars; n++) {
    if (new_string.ptr_to_string[n+start_index] >= 'a' && new_string.ptr_to_string[n] <= 'z')
	new_string.ptr_to_string[n+start_index]-=32;
  }
  return new_string;
}

void String::trim()
{
  if (len == 0)
    return;

  trimFront();
  trimBack();
}

void String::trimBack()
{
  int n;

  if (len == 0)
    return;
  n=len-1;
  while (n >= 0 && (ptr_to_string[n] == ' ' || ptr_to_string[n] == 0x9 || ptr_to_string[n] == 0xa || ptr_to_string[n] == 0xd))
    n--;
  len=n+1;
  ptr_to_string[len]='\0';
}

void String::trimFront()
{
  char *temp;
  size_t n=0;

  while (n < len && (ptr_to_string[n] == ' ' || ptr_to_string[n] == 0x9 || ptr_to_string[n] == 0xa || ptr_to_string[n] == 0xd))
    n++;
  if (n > 0) {
    temp=new char[strlen(&ptr_to_string[n])+1];
    strcpy(temp,&ptr_to_string[n]);
    strcpy(ptr_to_string,temp);
    len=strlen(ptr_to_string);
    delete[] temp;
  }
}

bool String::truncate(const char from_here_to_end)
{
  int n;
  bool truncated=false;

  for (n=len-1; n >= 0; n--) {
    if (ptr_to_string[n] == from_here_to_end)
	break;
  }
  if (n >= 0) {
    ptr_to_string[n]='\0';
    len=n;
    truncated=true;
  }
  return truncated;
}

bool String::replace(const char *old_string,const char *new_string)
{
  int old_len=strlen(old_string);
  int new_len=strlen(new_string);
  int check_len=len-old_len;
  size_t numReplacements;
  size_t rlen;
  int n,m;
  char *temp=NULL,*temp2=NULL;

  if (strcmp(old_string,new_string) == 0)
    return false;
// find out how many replacements there are
  numReplacements=0;
  for (n=0; n <= check_len; n++) {
    if (strncmp(&ptr_to_string[n],old_string,old_len) == 0) {
	numReplacements++;
	n+=(old_len-1);
    }
  }
  if (numReplacements == 0)
    return false;
  rlen=len+(new_len-old_len)*numReplacements;
  if (rlen > len) {
    temp=new char[len+1];
    strcpy(temp,ptr_to_string);
    if (ptr_to_string != NULL)
	delete[] ptr_to_string;
    ptr_to_string=new char[rlen+1];
  }
  if (old_len >= new_len) {
    for (n=0; n <= check_len; n++) {
	if (strncmp(&ptr_to_string[n],old_string,old_len) == 0) {
	  if (new_len > 0)
	    strncpy(&ptr_to_string[n],new_string,new_len);
	  if (old_len > new_len) {
	    temp2=new char[strlen(&ptr_to_string[n+old_len])+1];
	    strcpy(temp2,&ptr_to_string[n+old_len]);
	    strcpy(&ptr_to_string[n+new_len],temp2);
	    delete[] temp2;
	  }
	  check_len-=(old_len-new_len);
	  n+=(new_len-1);
	}
    }
    len=rlen;
  }
  else {
    m=0;
    for (n=0; n <= check_len; n++) {
	if (strncmp(&temp[n],old_string,old_len) == 0) {
	  strncpy(&ptr_to_string[m],new_string,new_len);
	  m+=new_len;
	  n+=(old_len-1);
	}
	else {
	  ptr_to_string[m]=temp[n];
	  m++;
	}
    }
    for (; n < (int)len; n++) {
	ptr_to_string[m]=temp[n];
	m++;
    }
    len=rlen;
    ptr_to_string[rlen]='\0';
    if (temp != NULL)
	delete[] temp;
  }
  len=strlen(ptr_to_string);
  return true;
}

String String::substitute(const char *old_string,const char *new_string) const
{
  String newstr=*this;

  newstr.replace(old_string,new_string);
  return newstr;
}

std::ostream& operator <<(std::ostream& out_stream,const String& source)
{
  if (source.len > 0)
    out_stream << source.ptr_to_string;
  else
    out_stream << "";

  return out_stream;
}

String operator +(const String& source1,const String& source2)
{
  String new_string=source1;

  new_string.cat(source2.toChar());
  return new_string;
}

bool operator ==(const String& source1,const String& source2)
{
  if (source1.len == 0 && source2.len == 0)
    return true;
  else if (source1.len == 0 || source2.len == 0)
    return false;

  if (strcmp(source1.ptr_to_string,source2.ptr_to_string) == 0)
    return true;
  else
    return false;
}

bool operator !=(const String& source1,const String& source2)
{
  return !(source1 == source2);
}

bool operator <(const String& source1,const String& source2)
{
  size_t cmp_len= (source1.len < source2.len) ? source1.len : source2.len;
  size_t n;

  for (n=0; n < cmp_len; n++) {
    if (source1.ptr_to_string[n] != source2.ptr_to_string[n]) {
	if (source1.ptr_to_string[n] < source2.ptr_to_string[n])
	  return true;
	else
	  return false;
    }
  }
  if (source1.len < source2.len)
    return true;
  else
    return false;
}

bool operator <=(const String& source1,const String& source2)
{
  if (source1 < source2 || source1 == source2)
    return true;
  else
    return false;
}

bool operator >(const String& source1,const String& source2)
{
  size_t cmp_len= (source1.len < source2.len) ? source1.len : source2.len;
  size_t n;

  for (n=0; n < cmp_len; n++) {
    if (source1.ptr_to_string[n] != source2.ptr_to_string[n]) {
	if (source1.ptr_to_string[n] > source2.ptr_to_string[n])
	  return true;
	else
	  return false;
    }
  }
  if (source1.len > source2.len)
    return true;
  else
    return false;
}

bool operator >=(const String& source1,const String& source2)
{
  if (source1 > source2 || source1 == source2)
    return true;
  else
    return false;
}

const size_t StringBuffer::buffer_increment=500000;

StringBuffer::StringBuffer(size_t initial_capacity)
{
  if (initial_capacity == 0)
    capacity=buffer_increment;
  else
    capacity=initial_capacity;
}

StringBuffer::StringBuffer(const char *source,size_t initial_capacity)
{
  if (initial_capacity == 0)
    capacity=buffer_increment;
  else
    capacity=initial_capacity;
  fill(source);
}

StringBuffer::StringBuffer(const String& source,size_t initial_capacity)
{
  if (initial_capacity == 0)
    capacity=buffer_increment;
  else
    capacity=initial_capacity;
  fill(source.toChar());
}

void StringBuffer::fill(const char *source)
{
  if (source == NULL)
    return;
  if (strlen(source) > capacity) {
    if (ptr_to_string != NULL)
	delete[] ptr_to_string;
    capacity=strlen(source)+buffer_increment;
  }
  len=strlen(source);
  ptr_to_string=new char[capacity+1];
  strcpy(ptr_to_string,source);
}

void StringBuffer::operator =(const String& source)
{
  fill(source.toChar());
}

void StringBuffer::operator =(const char *source)
{
  fill(source);
}

void StringBuffer::cat(const char *source)
{
  char *temp;

  if (source == NULL)
    return;
  if (len > 0) {
    if (len+strlen(source) < capacity)
	strcat(ptr_to_string,source);
    else {
	temp=new char[len+1];
	strcpy(temp,ptr_to_string);
	if (ptr_to_string != NULL)
	  delete[] ptr_to_string;
	capacity=len+strlen(source)+buffer_increment;
	ptr_to_string=new char[capacity+1];
	strcpy(ptr_to_string,temp);
	strcat(ptr_to_string,source);
	delete[] temp;
    }
    len+=strlen(source);
  }
  else
{
    fill(source);
}
}

//StringBuffer StringBuffer::operator +=(const StringBuffer& source)
void StringBuffer::operator +=(const StringBuffer& source)
{
  cat(source.toChar());
//  return *this;
}

//StringBuffer StringBuffer::operator +=(const String& source)
void StringBuffer::operator +=(const String& source)
{
  cat(source.toChar());
//  return *this;
}

/*
char *StringBuffer::operator +=(const char *source)
{
  cat(source);
  return ptr_to_string;
}
*/
void StringBuffer::operator +=(const char *source)
{
  cat(source);
}

StringParts::StringParts()
{
  capacity=num_parts=0;
  parts=NULL;
}

StringParts::StringParts(const String& string,const char *separator)
{
  capacity=num_parts=0;
  parts=NULL;
  fill(string,separator);
}

StringParts::StringParts(const String& string,size_t StringType)
{
  capacity=num_parts=0;
  parts=NULL;
  fill(string,StringType);
}

StringParts::~StringParts()
{
  if (parts != NULL)
    delete[] parts;
}

void StringParts::fill(const String& string,const char *separator)
{
  size_t len_cmp=strlen(separator);
  size_t check_len=string.getLength()-len_cmp+1;
  size_t n,m;
  size_t start,end;
  bool in_white_space=false;

  if (string.getLength() == 0) {
    num_parts=0;
    return;
  }
  num_parts=1;
  if (len_cmp == 0) {
// split on whitespace
    for (n=0; n < check_len; n++) {
	if (string.charAt(n) == ' ')
	  in_white_space=true;
	else {
	  if (in_white_space)
	    num_parts++;
	  in_white_space=false;
	}
    }
  }
  else {
// split on a separator
    for (n=0; n < check_len; n++) {
	if (strncmp(&(string.toChar()[n]),separator,len_cmp) == 0)
	  num_parts++;
    }
  }
  if (num_parts > capacity) {
    if (parts != NULL)
	delete[] parts;
    capacity=num_parts;
    parts=new String[capacity];
  }
  m=0;
  start=end=0;
  in_white_space=false;
  while (end < check_len) {
    if (len_cmp == 0) {
	if (string.charAt(end) == ' ') {
	  if (!in_white_space)
	    parts[m++].fill(string.getAddressOf(start),end-start);
	  in_white_space=true;
	}
	else {
	  if (in_white_space)
	    start=end;
	  in_white_space=false;
	}
	end++;
    }
    else {
	if (strncmp(&(string.toChar()[end]),separator,len_cmp) == 0) {
	  parts[m++].fill(string.getAddressOf(start),end-start);
	  end+=len_cmp;
	  start=end;
	}
	else
	  end++;
    }
  }
  if (start < string.getLength())
    parts[m].fill(string.getAddressOf(start));
  else
    parts[m]="";
}

void StringParts::fill(const String& string,size_t StringType)
{
  int bindex,eindex;
  String s=string;
  LinkedList<String> plist;
  size_t n;

  switch (StringType) {
    case String::XMLTag:
// the first tag should be at the beginning of the string
	bindex=s.isAt("<");
	if (bindex != 0) {
	  num_parts=0;
	  return;
	}
	else {
	  while (bindex >= 0) {
	    if (bindex == 0) {
		eindex=s.isAt(">");
		plist.attach(s.substr((size_t)0,eindex+1));
		s=s.substr(eindex+1,32768);
	    }
	    else {
		plist.attach(s.substr((size_t)0,bindex));
		s=s.substr(bindex,32768);
	    }
	    bindex=s.isAt("<");
	  }
	}
	num_parts=plist.getLength();
	break;
    default:
	num_parts=0;
  }
  if (num_parts > 0) {
    if (num_parts > capacity) {
	if (parts != NULL)
	  delete[] parts;
	capacity=num_parts;
	parts=new String[capacity];
    }
    plist.goStart();
    n=0;
    while (plist.isCurrent()) {
	parts[n++]=plist.getCurrent();
	plist.advance();
    }
  }
}

size_t StringParts::getLengthOfPart(size_t part_number) const
{
  if (part_number >= num_parts)
    return 0;
  else
    return parts[part_number].getLength();
}

const String& StringParts::getPart(size_t part_number) const
{
  if (part_number >= num_parts)
    return empty_part;
  else
    return parts[part_number];
}

String operator +(const String& source1,const char *source2)
{
  String new_string(source1);

  new_string+=source2;
  return new_string;
}

String operator +(const char *source1,const String& source2)
{
  String new_string(source1);

  new_string+=source2;
  return new_string;
}

String chop(const String& source)
{
  String new_string;

  new_string=source;
  new_string.chop();
  return new_string;
}

bool isAVowel(char c)
{
  if (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u')
    return true;
  else
    return false;
}

// Templates
template <class MaskType>
inline void createMask(MaskType& mask,size_t size)
{
  size_t n;

  mask=1;
  for (n=1; n < size; n++)
    (mask<<=1)++;
}

template <class BufType,class LocType>
inline void extractBits(const BufType *buf,LocType *loc,size_t off,const size_t bits,size_t skip = 0,const size_t num = 1)
{
// create a mask to use when right-shifting (necessary because different
// compilers do different things when right-shifting a signed bit-field)
  BufType bmask;
  size_t loc_size=sizeof(LocType)*8,buf_size=sizeof(BufType)*8,wskip;
  int rshift;
  LocType lmask,temp;

  if (bits > loc_size) {
    std::cerr << "Error: trying to unpack " << bits << " bits into a " << loc_size << "-bit location" << std::endl;
    exit(1);
  }
  else {
    createMask(bmask,buf_size);
    skip+=bits;
    if (loc_size <= buf_size) {
	for (size_t n=0; n < num; n++) {
// skip to the word containing the packed field
	  wskip=off/buf_size;
// right shift the bits in the packed buffer word to eliminate unneeded bits
	  rshift=buf_size-(off % buf_size)-bits;
// check for a packed field spanning two words
	  if (rshift < 0) {
	    loc[n]=(buf[wskip]<<-rshift);
	    loc[n]+=(buf[++wskip]>>(buf_size+rshift))&~(bmask<<-rshift);
	  }
	  else
	    loc[n]=(buf[wskip]>>rshift);
// remove any unneeded leading bits
	  if (bits != buf_size) loc[n]&=~(bmask<<bits);
	  off+=skip;
	}
    }
    else {
	createMask(lmask,loc_size);
// get difference in bits between word size of packed buffer and word size of
// unpacked location
	for (size_t n=0; n < num; n++) {
// skip to the word containing the packed field
	  wskip=off/buf_size;
// right shift the bits in the packed buffer word to eliminate unneeded bits
	  rshift=buf_size-(off % buf_size)-bits;
// check for a packed field spanning multiple words
	  if (rshift < 0) {
	    loc[n]=0;
	    while (rshift < 0) {
		temp=buf[wskip++];
		loc[n]+=(temp<<-rshift);
		rshift+=buf_size;
	    }
	    if (rshift != 0)
		loc[n]+=(buf[wskip]>>rshift)&~(bmask<<(buf_size-rshift));
	    else
		loc[n]+=buf[wskip];
	  }
	  else
	    loc[n]=(buf[wskip]>>rshift);
// remove any unneeded leading bits
	  if (bits != loc_size) loc[n]&=~(lmask<<bits);
	  off+=skip;
	}
    }
  }
}

template <class BufType,class LocType>
void getBits(const BufType *buf,LocType *loc,const size_t off,const size_t bits,const size_t skip,const size_t num)
{
// no work to do
  if (bits == 0) return;

  extractBits(buf,loc,off,bits,skip,num);
}

template <class BufType,class LocType>
void getBits(const BufType *buf,LocType& loc,const size_t off,const size_t bits)
{
// no work to do
  if (bits == 0) return;

  extractBits(buf,&loc,off,bits);
}

template <class BufType,class SrcType>
inline void putBits(BufType *buf,const SrcType *src,size_t off,const size_t bits,size_t skip = 0,const size_t num = 1)
{
// create a mask to use when right-shifting (necessary because different
// compilers do different things when right-shifting a signed bit-field)
  size_t src_size=sizeof(SrcType)*8,buf_size=sizeof(BufType)*8;
  size_t n,wskip,bskip,lclear,rclear,more;
  BufType bmask,left,right;
  SrcType smask;

  if (bits > src_size) {
//    std::cerr << "Error: packing " << bits << " bits from a " << src_size << "-bit field" << std::endl;
    exit(1);
  }
  else {
    createMask(bmask,buf_size);
    createMask(smask,src_size);
    skip+=bits;
    for (n=0; n < num; n++) {
// get number of words and bits to skip before packing begins
	wskip=off/buf_size;
	bskip=off % buf_size;
	lclear=bskip+bits;
	rclear=buf_size-bskip;
	left= (rclear != buf_size) ? (buf[wskip]&(bmask<<rclear)) : 0;
	if (lclear <= buf_size) {
// all bits to be packed are in the current word
// clear the field to be packed
	  right=
          (lclear != buf_size) ? (buf[wskip]&~(bmask<<(buf_size-lclear))) : 0;
// fill the field to be packed
	  buf[wskip]= (src_size != bits) ? (BufType)(src[n]&~(smask<<bits)) : (BufType)src[n];
	  buf[wskip]=left|right|(buf[wskip]<<(rclear-bits));
	}
	else {
// bits to be packed cross a word boundary(ies)
// clear the bit field to be packed
	  more=bits-rclear;
//	  buf[wskip]= (buf_size > src_size) ? left|((src[n]>>more)&~(bmask<<(bits-more))) : left|(src[n]>>more);
	  buf[wskip]=(BufType)(left|((src[n]>>more)&~(smask<<(bits-more))));
// clear the next (or part of the next) word and pack those bits
	  while (more > buf_size) {
	    more-=buf_size;
	    buf[++wskip]=(BufType)((src[n]>>more)&~(smask<<(src_size-more)));
	  }
	  wskip++;
	  more=buf_size-more;
	  right= (more != buf_size) ? (buf[wskip]&~(bmask<<more)) : 0;
	  buf[wskip]= (buf_size > src_size) ? (BufType)(src[n]&~(bmask<<src_size)) : (BufType)src[n];
	  buf[wskip]=right|(buf[wskip]<<more);
	}
	off+=skip;
    }
  }
}

template <class BufType,class SrcType>
void setBits(BufType *buf,const SrcType *src,const size_t off,const size_t bits,const size_t skip = 0,const size_t num = 1)
{
// no work to do
  if (bits == 0) return;

  putBits(buf,src,off,bits,skip,num);
}

template <class BufType,class SrcType>
void setBits(BufType *buf,const SrcType src,const size_t off,const size_t bits)
{
// no work to do
  if (bits == 0) return;

  putBits(buf,&src,off,bits);
}

template <class Type>
void strget(const char *str,Type& numeric_val,int num_chars)
{
  String cval(str,num_chars);
  int n,decimal;
  char *chr=(char *)cval.toChar();
  double mult;
  bool neg_val=false,neg_exp=false;
  bool reading_exponent=false;
  int exp=0;

  cval.trim();
  num_chars=decimal=cval.getLength();
  numeric_val=0;
  for (n=0; n < num_chars; n++) {
    if (chr[n] == '.') {
	numeric_val*=(Type)0.1;
	decimal=n;
    }
    else if (chr[n] == 'E')
	reading_exponent=true;
    else {
	if (chr[n] == ' ' || chr[n] == '+' || chr[n] == '-') {
	  if (chr[n] == '-') {
	    if (!reading_exponent)
		neg_val=true;
	    else
		neg_exp=true;
	  }
	  chr[n]='0';
	}
	if (!reading_exponent)
	  numeric_val+=(Type)((chr[n]-48)*pow(10.,num_chars-n-1));
	else
	  exp+=(int)((chr[n]-48)*pow(10.,num_chars-n-1));
    }
  }
  if (neg_val) numeric_val=-numeric_val;
  if (decimal != num_chars) {
    mult=pow(0.1,num_chars-decimal-1);
    numeric_val*=(Type)mult;
  }
  if (reading_exponent) {
    if (neg_exp) exp=-exp;
    numeric_val*=(Type)pow(10.,exp);
  }
}

template <class Type>
void swap(Type& a,Type& b)
{
  Type temp;

  temp=a;
  a=b;
  b=temp;
}

/*
template <class Item>
void merge(Item *left_array,size_t left_length,Item *right_array,size_t right_length,int (*compare)(Item& left,Item& right))
{
  Item *temp;
  size_t left=0,right=0,temp_count=0;
  bool unsorted=false;

  temp=new Item[left_length+right_length];

  while (left < left_length && right < right_length) {
    if (compare(left_array[left],right_array[right]) <= 0) {
	temp[temp_count]=left_array[left];
	left++;
    }
    else {
	temp[temp_count]=right_array[right];
	right++;
	unsorted=true;
    }
    temp_count++;
  }
  if (left < left_length) {
    while (left < left_length) {
	temp[temp_count]=left_array[left];
	left++;
	temp_count++;
    }
  }
  else {
    while (right < right_length) {
	temp[temp_count]=right_array[right];
	right++;
	temp_count++;
    }
  }

// copy the merged temporary array into the two sub-arrays
  if (unsorted) {
    temp_count=0;
    left=0;
    while (left < left_length) {
	left_array[left]=temp[temp_count];
	left++;
	temp_count++;
    }
    right=0;
    while (right < right_length) {
	right_array[right]=temp[temp_count];
	right++;
	temp_count++;
    }
  }

  delete[] temp;
}

template <class Item>
void merge(Item *left_array,size_t left_length,Item *right_array,size_t right_length,int (*compare)(Item& left,Item& right),void (*checkPrint)(Item& left,Item& right))
{
  Item *temp;
  size_t left=0,right=0,temp_count=0;
  bool unsorted=false;

  temp=new Item[left_length+right_length];

  while (left < left_length && right < right_length) {
    if (compare(left_array[left],right_array[right]) <= 0) {
	left++;
    }
    else {
	checkPrint(left_array[left],right_array[right]);
	temp[temp_count]=right_array[right];
	right++;
	unsorted=true;
    }
    temp_count++;
  }

  delete[] temp;
}

template <class Item>
void binarySort(Item *array,size_t num,int (*compare)(Item& left,Item& right))
{
  size_t left_length=num/2,right_length=num-left_length;

  if (left_length > 1)
    binarySort(array,left_length,compare);
  if (right_length > 1)
    binarySort(array+left_length,right_length,compare);
  merge(array,left_length,array+left_length,right_length,compare);
}

template <class Item>
void binarySort(Item *array,size_t num,int (*compare)(Item& left,Item& right),void (*checkPrint)(Item& left,Item& right))
{
  size_t left_length=num/2,right_length=num-left_length;

  if (left_length > 1)
    binarySort(array,left_length,compare,checkPrint);
  if (right_length > 1)
    binarySort(array+left_length,right_length,compare,checkPrint);
  merge(array,left_length,array+left_length,right_length,compare,checkPrint);
}
*/

void error(String message)
{
  std::cerr << "Error: " << message << std::endl;
  exit(1);
}

class bfstream {
public:
  static const int error,eof;

  virtual void close()=0;
  size_t getBlockCount() const { return num_blocks; }
  size_t getMaximumBlockSize() const { return max_block_len; }
  virtual bool isOpen() const { return (fp != NULL); }
  virtual void rewind()=0;

protected:
  bfstream();
  virtual ~bfstream();

  FILE *fp;
  unsigned char *file_buf;
  size_t file_buf_len,file_buf_pos;
  size_t num_blocks,max_block_len;
  char *file;
};

class ibfstream : virtual public bfstream
{
public:
  size_t getNumberRead() const { return num_read; }
  virtual int ignore()=0;
  virtual bool open(const char *filename);
  virtual int peek()=0;
  virtual int read(unsigned char *buffer,size_t buffer_length)=0;

protected:
  size_t num_read;
};

class obfstream : virtual public bfstream
{
public:
  size_t getNumberWritten() const { return num_written; }
  virtual bool open(const char *filename);
  virtual int write(const unsigned char *buffer,size_t num_bytes)=0;

protected:
  size_t num_written;
};

class craystream : virtual public bfstream
{
public:
  static const int eod;

protected:
  craystream();

  static const size_t cray_block_size,cray_word_size;
  static const short cw_bcw,cw_eor,cw_eof,cw_eod;
  short cw_type;
};

class icstream : public ibfstream, virtual public craystream
{
public:
  icstream() { at_eod=false; }
  icstream(const char *filename) { open(filename); at_eod=false; }
  icstream(const icstream& source) { *this=source; }
  void operator =(const icstream& source);
  void close();
  int ignore();
  bool open(const char *filename);
  int peek();
  int read(unsigned char *buffer,size_t buffer_length);
  void rewind();

private:
  int readFromDisk();

  bool at_eod;
};

class ocstream : public obfstream, virtual public craystream
{
public:
  ocstream() {}
  ocstream(const char *filename) { open(filename); }
  virtual ~ocstream() { if (isOpen()) close(); }
  void close();
  bool open(const char *filename);
  void rewind();
  int write(const unsigned char *buffer,size_t num_bytes);
  void writeEOF();

private:
  int writeToDisk();

  struct {
    size_t block_space,blocks_full,blocks_back;
    bool wrote_eof;
  } oc;
};

class iocstream : public icstream, public ocstream
{
public:
  iocstream() {}
  iocstream(const char *filename,const char *mode) { open(filename,mode); }
  virtual ~iocstream() { if (isOpen()) close(); }
  void close();
  int ignore();
  bool isInput() { return (stream_type == 0); }
  bool isOutput() { return (stream_type == 1); }
  bool open(const char *filename);
  bool open(const char *filename,const char *mode);
  int peek();
  int read(unsigned char *buffer,size_t buffer_length);
  void rewind();
  int write(const unsigned char *buffer,size_t num_bytes);
  void writeEOF();

private:
  short stream_type;
};

class rptoutstream : virtual public bfstream
{
public:
  size_t getBlockLength() const { return block_length; }
  bool isNewBlock() const { return is_new_block; }

protected:
  rptoutstream() { file_buf=new unsigned char[8000]; }

  size_t block_length,word_count;
  bool is_new_block;
};

class irstream : public ibfstream, virtual public rptoutstream
{
public:
  irstream() { icosstream=NULL; }
  irstream(const char *filename) { icosstream=NULL; open(filename); }
  irstream(const irstream& source) { *this=source; }
  virtual ~irstream() { if (isOpen()) close(); }
  void operator =(const irstream& source);
  void close();
  int getFlag() const { return (int)flag; }
  size_t getWordsRead() const { return word_count; }
  int ignore();
  bool isOpen() const { return (fp != NULL || icosstream != NULL); }
  bool open(const char *filename);
  int peek();
  int read(unsigned char *buffer,size_t buffer_length);
  void rewind();

protected:
  icstream *icosstream;

private:
  unsigned char flag;
};

class orstream : public obfstream, public rptoutstream
{
public:
  orstream() {}
  orstream(const char *filename) { open(filename); }
  virtual ~orstream() { if (isOpen()) close(); }
  void close();
  int flush();
  bool open(const char *filename);
  void rewind();
  int write(const unsigned char *buffer,size_t num_bytes);
};

class ocrstream : public obfstream, virtual public rptoutstream
{
public:
  ocrstream() { ocosstream=NULL; }
  ocrstream(const char *filename) { ocosstream=NULL; open(filename); }
  virtual ~ocrstream() { if (isOpen()) close(); }
  void close();
  int flush();
  size_t getWordsWritten() const { return word_count; }
  bool isOpen() const { return (ocosstream != NULL); }
  bool open(const char *filename);
  void rewind();
  int write(const unsigned char *buffer,size_t num_bytes);
  void writeEOF();

protected:
  ocstream *ocosstream;
};

class iocrstream : public irstream, public ocrstream
{
public:
  iocrstream() { icosstream=NULL; ocosstream=NULL; }
  iocrstream(const char *filename,const char *mode) { open(filename,mode); }
  virtual ~iocrstream() { if (isOpen()) close(); }
  void close();
  int flush();
  size_t getBlockLength() const;
  int ignore();
  bool isInput() { return (stream_type == 0); }
  bool isNewBlock() const;
  bool isOpen() const { return (fp != NULL || icosstream != NULL || ocosstream != NULL); }
  bool isOutput() { return (stream_type == 1); }
  bool open(const char *filename);
  bool open(const char *filename,const char *mode);
  int peek();
  int read(unsigned char *buffer,size_t buffer_length);
  void rewind();
  int write(const unsigned char *buffer,size_t num_bytes);
  void writeEOF();

private:
  short stream_type;
};

class vbsstream
{
public:
protected:
  vbsstream() { capacity=0; }

  size_t capacity,lrec_len;
};

class ivbsstream : public ibfstream, public vbsstream
{
public:
  ivbsstream() {}
  ivbsstream(const char *filename) { open(filename); }
  ivbsstream(const ivbsstream& source) { *this=source; }
  void operator =(const ivbsstream& source);
  void close();
  int ignore();
  int peek();
  int read(unsigned char *buffer,size_t buffer_length);
  void rewind();

private:
  int readFromDisk();
};

class ovbsstream : public obfstream, public vbsstream
{
public:
  ovbsstream() {}
  ovbsstream(const char *filename) { open(filename); }
  ovbsstream(const ovbsstream& source) { *this=source; }
  virtual ~ovbsstream() { if (isOpen()) close(); }
  void operator =(const ovbsstream& source);
  void close();
  void rewind();
  int write(const unsigned char *buffer,size_t num_bytes);

private:
  void writeToDisk();

  size_t block_space,blocks_full,blocks_back;
};

class if77stream : public ibfstream
{
public:
  if77stream() {}
  if77stream(const char *filename) { open(filename); }
  if77stream(const if77stream& source) { *this=source; }
  void operator =(const if77stream& source);
  void close();
  int ignore();
  int peek();
  int read(unsigned char *buffer,size_t buffer_length);
  void rewind() { ::rewind(fp); }
};

class of77stream : public obfstream
{
public:
  of77stream() {}
  of77stream(const char *filename) { open(filename); }
  of77stream(const of77stream& source) { *this=source; }
  virtual ~of77stream() { if (isOpen()) close(); }
  void operator =(const of77stream& source);
  void close();
  void rewind();
  int write(const unsigned char *buffer,size_t num_bytes);
};

const int bfstream::error=-1;
const int bfstream::eof=-2;

bfstream::bfstream()
{
  file=NULL;
  fp=NULL;
  file_buf=NULL;
  file_buf_len=file_buf_pos=0;
  num_blocks=0;
  max_block_len=0;
}

bfstream::~bfstream()
{
  if (file != NULL) {
    delete[] file;
    file=NULL;
  }
  if (file_buf != NULL) {
    delete[] file_buf;
    file_buf=NULL;
  }
  if (fp != NULL) {
    fclose(fp);
    fp=NULL;
  }
}

bool ibfstream::open(const char *filename)
{
// opening a stream while another is open is a fatal error
  if (isOpen()) {
    std::cerr << "Error: an open stream already exists" << std::endl;
    exit(1);
  }

#if defined(LINUX) || defined(MacOS)
  if ( (fp=fopen(filename,"r")) == NULL)
#else
  if ( (fp=fopen64(filename,"r")) == NULL)
#endif
    return false;

  file=new char[strlen(filename)+1];
  strcpy(file,filename);
  num_read=num_blocks=max_block_len=0;
  return true;
}

bool obfstream::open(const char *filename)
{
// opening a stream while another is open is a fatal error
  if (isOpen()) {
    std::cerr << "Error: an open stream already exists" << std::endl;
    exit(1);
  }

#if defined(LINUX) || defined(MacOS)
  if ( (fp=fopen(filename,"w")) == NULL)
#else
  if ( (fp=fopen64(filename,"w")) == NULL)
#endif
    return false;

  file=new char[strlen(filename)+1];
  strcpy(file,filename);
  num_written=num_blocks=max_block_len=0;
  return true;
}

const int craystream::eod=-5;
const size_t craystream::cray_block_size=4096;
const size_t craystream::cray_word_size=8;
const short craystream::cw_bcw=0;
const short craystream::cw_eor=0x8;
const short craystream::cw_eof=0xe;
const short craystream::cw_eod=0xf;

craystream::craystream()
{
  file_buf_len=cray_block_size;
  file_buf=new unsigned char[file_buf_len];
}

inline int icstream::readFromDisk()
{
  size_t bytes_read;
  size_t unused[2],previous_block;

  if (at_eod) {
    cw_type=cw_eod;
    return eod;
  }
  bytes_read=fread(file_buf,1,file_buf_len,fp);
  if (num_blocks == 0 && bytes_read < file_buf_len) {
    cw_type=-1;
    return error;
  }
  file_buf_pos=0;
  getBits(file_buf,cw_type,0,4);
  if (cw_type == cw_eod)
    at_eod=true;
  if (num_blocks == 0) {
    getBits(file_buf,unused[0],4,7);
    getBits(file_buf,unused[1],12,19);
    getBits(file_buf,previous_block,31,24);
    if (cw_type != cw_bcw || unused[0] != 0 || unused[1] != 0 || previous_block != 0)
	cw_type=cw_eod+1;
  }
  num_blocks++;

  return 0;
}

void icstream::operator =(const icstream& source)
{
  if (this == &source)
    return;

  if (isOpen())
    close();
  if (source.fp != NULL) {
    file=new char[strlen(source.file)+1];
    strcpy(file,source.file);
#if defined(LINUX) || defined(MacOS)
    fp=fopen(file,"r");
#else
    fp=fopen64(file,"r");
#endif
    fseek(fp,ftell(source.fp),SEEK_SET);
  }
  memcpy(file_buf,source.file_buf,file_buf_len);
  file_buf_pos=source.file_buf_pos;

  at_eod=source.at_eod;
  cw_type=source.cw_type;
  num_read=source.num_read;
  num_blocks=source.num_blocks;
}

void icstream::close()
{
  if (!isOpen())
    return;

  fclose(fp);
  fp=NULL;
  if (file != NULL) {
    delete[] file;
    file=NULL;
  }
}

int icstream::ignore()
{
  size_t len,ub;
  int num_copied=0;

// if the current position in the file buffer is at the end of the buffer, read
// a new block of data from the disk file
  if (file_buf_pos == file_buf_len) {
    if (readFromDisk() == error)
	return error;
    if (cw_type == cw_eof)
	return eof;
  }

  switch (cw_type) {
    case cw_bcw:
    case cw_eor:
// get the length of the current block of data
	getBits(&file_buf[file_buf_pos],len,55,9);

// move the buffer pointer to the next control word
	file_buf_pos+=(len+1)*cray_word_size;

// if the file buffer position is inside the file buffer, get the number of
// unused bits in the end of the current block of data
	if (file_buf_pos < file_buf_len) {
	  getBits(&file_buf[file_buf_pos],ub,4,6);
	  ub/=cray_word_size;
	}
	else
	  ub=0;
	num_copied=len*cray_word_size-ub;
	if (file_buf_pos < file_buf_len)
// if the file buffer position is inside the file buffer, get the control word
	  getBits(&file_buf[file_buf_pos],cw_type,0,4);
	else
// otherwise, must be at the end of a Cray block, so next control word assumed
// to be a BCW
	  cw_type=cw_bcw;
	break;
    case cw_eof:
	file_buf_pos+=cray_word_size;
	if (file_buf_pos < file_buf_len)
	  getBits(&file_buf[file_buf_pos],cw_type,0,4);
	else
	  cw_type=cw_bcw;
	break;
  }

  switch (cw_type) {
    case cw_bcw:
	num_copied+=ignore();
	return num_copied;
    case cw_eor:
	num_read++;
	return num_copied;
    case cw_eof:
	return eof;
    case cw_eod:
	return eod;
    default:
	return error;
  }
}

bool icstream::open(const char *filename)
{
  if (!ibfstream::open(filename))
    return false;

  file_buf_pos=file_buf_len;

  return true;
}

int icstream::peek()
{
  int rec_len;
  short cwt=cw_type;
  size_t loc=ftell(fp),fbp=file_buf_pos,nr=num_read,nb=num_blocks;
  unsigned char *fb=new unsigned char[file_buf_len];

  memcpy(fb,file_buf,file_buf_len);
  rec_len=ignore();
  fseek(fp,loc,SEEK_SET);

  memcpy(file_buf,fb,file_buf_len);
  delete[] fb;
  file_buf_pos=fbp;
  cw_type=cwt;
  num_read=nr;
  num_blocks=nb;

  return rec_len;
}

int icstream::read(unsigned char *buffer,size_t buffer_length)
{
  size_t len,ub,copy_start;
  int num_copied=0;
  static int total_len=0,iterator=0;

  iterator++;

// if the current position in the file buffer is at the end of the buffer, read
// a new block of data from the disk file
  if (file_buf_pos == file_buf_len) {
    readFromDisk();
    if (cw_type == cw_eof)
	return eof;
  }

  switch (cw_type) {
    case cw_bcw:
    case cw_eor:
    case cw_eof:
// get the length of the current block of data
	getBits(&file_buf[file_buf_pos],len,55,9);

// move the buffer pointer to the next control word
	copy_start=file_buf_pos+cray_word_size;
	file_buf_pos+=(len+1)*cray_word_size;

	if (file_buf_pos < file_buf_len) {
// if the file buffer position is inside the file buffer, get the number of
// unused bits in the end of the current block of data
	  getBits(&file_buf[file_buf_pos],ub,4,6);
	  ub/=8;
	}
	else
	  ub=0;

// copy the data to the user-specified buffer
	num_copied=len*cray_word_size-ub;
	total_len+=num_copied;
	if (buffer_length > 0) {
	  if (num_copied > (int)buffer_length)
	    num_copied=buffer_length;
	  if (num_copied > 0)
	    memcpy(buffer,&file_buf[copy_start],num_copied);
	}
	else
	  num_copied=0;

	if (file_buf_pos < file_buf_len) {
// if the file buffer position is inside the file buffer, get the control word
	  getBits(&file_buf[file_buf_pos],cw_type,0,4);
	  if (cw_type == cw_bcw)
	    return craystream::error;
	}
	else
// otherwise, must be at the end of a Cray block, so next control word assumed
// to be a BCW
	  cw_type=cw_bcw;
	break;
  }

  switch (cw_type) {
    case cw_bcw:
	if (buffer != NULL)
	  num_copied+=read(buffer+num_copied,buffer_length-num_copied);
	else
	  num_copied+=read(NULL,buffer_length-num_copied);
	iterator--;
	if (iterator == 0) {
	  if (num_copied > total_len)
	    num_copied=total_len;
	  total_len=0;
	}
	return num_copied;
    case cw_eor:
	num_read++;
	iterator--;
	if (iterator == 0) {
	  if (num_copied > total_len)
	    num_copied=total_len;
	  total_len=0;
	}
	return num_copied;
    case cw_eof:
	return eof;
    case cw_eod:
	return craystream::eod;
    default:
	return error;
  }
}

void icstream::rewind()
{
  ::rewind(fp);
  num_read=num_blocks=0;
  readFromDisk();
}

inline int ocstream::writeToDisk()
{
  size_t n,num_out;

  num_out=fwrite(file_buf,1,file_buf_len,fp);
  if (num_out != file_buf_len)
    return -1;
  file_buf_pos=0;
  for (n=0; n < cray_word_size; n++)
    file_buf[n]=0;
  oc.block_space=511;
  num_blocks++;

  return num_out;
}

void ocstream::close()
{
  size_t n;

  if (!isOpen())
    return;

// write the EOF
  if (!oc.wrote_eof)
    writeEOF();

// write the EOD
  if (oc.block_space == 0) {
    writeToDisk();
    oc.blocks_full++;
    oc.blocks_back++;
    setBits(&file_buf[file_buf_pos],oc.blocks_full,31,24);
    setBits(&file_buf[file_buf_pos],0,55,9);
  }
  file_buf_pos+=cray_word_size;
  for (n=file_buf_pos; n < file_buf_pos+8; n++)
    file_buf[n]=0;
  setBits(&file_buf[file_buf_pos],0xf,0,4);
  for (n=file_buf_pos+8; n < file_buf_len; n++)
    file_buf[n]=0;
  writeToDisk();

  fclose(fp);
  fp=NULL;
  if (file != NULL) {
    delete[] file;
    file=NULL;
  }
}

bool ocstream::open(const char *filename)
{
  size_t n;

  if (!obfstream::open(filename))
    return false;

  file_buf_pos=0;
  for (n=0; n < 8; n++)
    file_buf[n]=0;
  oc.block_space=511;
  oc.blocks_full=oc.blocks_back=0;
  oc.wrote_eof=false;

  return true;
}

void ocstream::rewind()
{

}

int ocstream::write(const unsigned char *buffer,size_t num_bytes)
{
  size_t num_words=(num_bytes+7)/8;
  size_t unused_bits=(num_words*8-num_bytes)*8;
  size_t n,new_front;

  if (num_words > oc.block_space) {
    setBits(&file_buf[file_buf_pos],oc.block_space,55,9);
    file_buf_pos+=cray_word_size;
    new_front=oc.block_space*cray_word_size;
    memcpy(&file_buf[file_buf_pos],buffer,new_front);
    if (writeToDisk() == error)
	return error;
    oc.blocks_full++;
    oc.blocks_back++;
    setBits(&file_buf[file_buf_pos],oc.blocks_full,31,24);
    write(&buffer[new_front],num_bytes-new_front);
  }
  else {
    setBits(&file_buf[file_buf_pos],num_words,55,9);
    file_buf_pos+=cray_word_size;
    memcpy(&file_buf[file_buf_pos],buffer,num_bytes);
    oc.block_space-=num_words;

// filled the block to the end
    if (oc.block_space == 0) {
	if (writeToDisk() == error)
	  return error;
	oc.blocks_full++;
	oc.blocks_back++;
	setBits(&file_buf[file_buf_pos],oc.blocks_full,31,24);
	file_buf_pos+=cray_word_size;
    }
    else
      file_buf_pos+=num_words*cray_word_size;
    for (n=file_buf_pos; n < file_buf_pos+cray_word_size; n++)
	file_buf[n]=0;
    setBits(&file_buf[file_buf_pos],0x8,0,4);
    setBits(&file_buf[file_buf_pos],unused_bits,4,6);
    setBits(&file_buf[file_buf_pos],oc.blocks_full,20,20);
    setBits(&file_buf[file_buf_pos],oc.blocks_back,40,15);
    oc.blocks_back=0;
    oc.block_space--;
    num_written++;
  }

  return num_bytes;
}

void ocstream::writeEOF()
{
  size_t n;

  if (oc.block_space == 0) {
    writeToDisk();
    oc.blocks_full++;
    oc.blocks_back++;
    setBits(&file_buf[file_buf_pos],oc.blocks_full,31,24);
    setBits(&file_buf[file_buf_pos],0,55,9);
  }
  file_buf_pos+=cray_word_size;
  for (n=file_buf_pos; n < file_buf_pos+8; n++)
    file_buf[n]=0;
  setBits(&file_buf[file_buf_pos],0xe,0,4);
  setBits(&file_buf[file_buf_pos],oc.blocks_full,20,20);
  oc.block_space--;
  oc.wrote_eof=true;
}

void iocstream::close()
{
  switch (stream_type) {
    case 0:
	icstream::close();
	break;
    case 1:
	ocstream::close();
	break;
  }
}

int iocstream::ignore()
{
  switch (stream_type) {
    case 0:
	return icstream::ignore();
    case 1:
	std::cerr << "Warning: ignore should not be called on a stream opened for writing" << std::endl;
  }

  return bfstream::error;
}

bool iocstream::open(const char *filename)
{
  std::cerr << "Error: no mode given" << std::endl;
  return false;
}

bool iocstream::open(const char *filename,const char *mode)
{
  if (strcmp(mode,"r") == 0) {
    stream_type=0;
    return icstream::open(filename);
  }
  else if (strcmp(mode,"w") == 0) {
    stream_type=1;
    return ocstream::open(filename);
  }
  else {
    std::cerr << "Error: bad mode " << mode << std::endl;
    return false;
  }
}

int iocstream::peek()
{
  switch (stream_type) {
    case 0:
	return icstream::peek();
    case 1:
	std::cerr << "Warning: peek should not be called on a stream opened for writing" << std::endl;
  }

  return bfstream::error;
}

int iocstream::read(unsigned char *buffer,size_t buffer_length)
{
  switch (stream_type) {
    case 0:
	return icstream::read(buffer,buffer_length);
    case 1:
	std::cerr << "Warning: read should not be called on a stream opened for writing" << std::endl;
  }

  return bfstream::error;
}

void iocstream::rewind()
{
  switch (stream_type) {
    case 0:
	icstream::rewind();
	break;
    case 1:
	ocstream::rewind();
	break;
  }
}

int iocstream::write(const unsigned char *buffer,size_t num_bytes)
{
  switch (stream_type) {
    case 0:
	std::cerr << "Warning: write should not be called on a stream opened for reading" << std::endl;
    case 1:
	return ocstream::write(buffer,num_bytes);
  }

  return bfstream::error;
}

void iocstream::writeEOF()
{
  switch (stream_type) {
    case 0:
	std::cerr << "Warning: writeEOF should not be called on a stream opened for reading" << std::endl;
    case 1:
	ocstream::writeEOF();
	break;
  }
}

String getUnixArgsString(int argc,char **argv,char separator = ':')
{
  String unix_args(argv[1]);
  int n;
  char s[2];

  s[0]=separator;
  s[1]='\0';
  for (n=2; n < argc; n++)
    unix_args+=(String(s)+argv[n]);

  return unix_args;
}

long long hex2long(char *hex_string,int num_chars)
{
  static long long ival=0;
  static int length=num_chars;
  long long add=0;

  if (num_chars > 1)
    hex2long(hex_string,num_chars-1);

  switch (hex_string[num_chars-1]) {
    case 'a':
    case 'A':
	add=10*(long long)pow(16.,length-num_chars);
	break;
    case 'b':
    case 'B':
	add=11*(long long)pow(16.,length-num_chars);
	break;
    case 'c':
    case 'C':
	add=12*(long long)pow(16.,length-num_chars);
	break;
    case 'd':
    case 'D':
	add=13*(long long)pow(16.,length-num_chars);
	break;
    case 'e':
    case 'E':
	add=14*(long long)pow(16.,length-num_chars);
	break;
    case 'f':
    case 'F':
	add=15*(long long)pow(16.,length-num_chars);
	break;
    default:
	add=(hex_string[num_chars-1]-48)*(long long)pow(16.,length-num_chars);
  }
  ival+=add;

  return ival;
}

long long htoi(char *hex_string)
{
  return hex2long(hex_string,strlen(hex_string));
}

char *lltoh(long long lval)
{
  int n,num_chars=0;
  long long power,ch;
  size_t cnt;
  char *string;

  while (lval >= pow(16.,num_chars))
    num_chars++;
  string=new char[num_chars+3];
  string[0]='0';
  string[1]='x';
  string[num_chars+2]='\0';

  cnt=2;
  for (n=num_chars-1; n >= 0; n--) {
    power=(long long)pow(16.,n);
    ch=lval/power;
    switch (ch) {
	case 10:
	  string[cnt]='a';
	  break;
	case 11:
	  string[cnt]='b';
	  break;
	case 12:
	  string[cnt]='c';
	  break;
	case 13:
	  string[cnt]='d';
	  break;
	case 14:
	  string[cnt]='e';
	  break;
	case 15:
	  string[cnt]='f';
	  break;
	default:
//	  string[cnt]=(int)ch+48;
string[cnt]=ch+'0';
    }
    lval-=ch*power;
    cnt++;
  }
  return string;
}

char *itoh(int ival)
{
  return lltoh(ival);
}

long long itoo(int ival)
{
  int n;
  long long oval=0;
  int max_power=0;
  size_t power,i;

  while (ival >= pow(8.,max_power))
    max_power++;
  for (n=max_power-1; n >= 0; n--) {
    power=(long long)pow(8.,n);
    i=ival/power;
    oval+=(i*(long long)pow(10.,n));
    ival-=i*power;
  }
  return oval;
}

long long ltoo(long long lval)
{
  long long oval=0,l;
  int n;
  int max_power=0;
  size_t power;

  while (lval >= pow(8.,max_power))
    max_power++;

  for (n=max_power-1; n >= 0; n--) {
    power=(long long)pow(8.,n);
    l=lval/power;
    oval+=(l*(long long)pow(10.,n));
    lval-=l*power;
  }

  return oval;
}

String itos(int val)
{
  char dum[24];

  sprintf(dum,"%d",val);

  return String(dum);
}

String ftos(float val,size_t max_d)
{
  String string;
  char dum[24];

  if (max_d > 0)
    sprintf(dum,("%."+itos(max_d)+"f").toChar(),val);
  else
    sprintf(dum,"%f",val);
  string.fill(dum);
  while (string.contains(".") && string.endsWith("0"))
    string.chop();
  if (string.endsWith("."))
    string.chop();

  return string;
}

String ftos(float val,size_t w,size_t d,char fill)
{
  String string;
  char dum[24];

  sprintf(dum,("%"+itos(w)+"."+itos(d)+"f").toChar(),val);
  string.fill(dum);
  string.replace(" ",String(&fill,1).toChar());

  return string;
}

String ltos(long long val,int width,char fill)
{
  String string;
  char dum[24];

  if (width > 0) {
    sprintf(dum,("%"+itos(width)+".0f").toChar(),(double)val);
    string.fill(dum);
    string.replace(" ",String(&fill,1).toChar());
  }
  else {
    sprintf(dum,"%lld",val);
    string.fill(dum);
  }
  return string;
}

void ebc_to_ascii(char *dest,char *src,size_t num)
{
  static char map[]={0x00,0x01,0x02,0x03,0x9c,0x09,0x86,0x7f,0x97,0x8d,0x8e,
                     0x0b,0x0c,0x0d,0x0e,0x0f,0x10,0x11,0x12,0x13,0x9d,0x85,
                     0x08,0x87,0x18,0x19,0x92,0x8f,0x1c,0x1d,0x1e,0x1f,0x80,
                     0x81,0x82,0x83,0x84,0x0a,0x17,0x1b,0x88,0x89,0x8a,0x8b,
                     0x8c,0x05,0x06,0x07,0x90,0x91,0x16,0x93,0x94,0x95,0x96,
                     0x04,0x98,0x99,0x9a,0x9b,0x14,0x15,0x9e,0x1a,0x20,0xa0,
                     0xa1,0xa2,0xa3,0xa4,0xa5,0xa6,0xa7,0xa8,0xd5,0x2e,0x3c,
                     0x28,0x2b,0x7c,0x26,0xa9,0xaa,0xab,0xac,0xad,0xae,0xaf,
                     0xb0,0xb1,0x21,0x24,0x2a,0x29,0x3b,0x5e,0x2d,0x2f,0xb2,
                     0xb3,0xb4,0xb5,0xb6,0xb7,0xb8,0xb9,0xe5,0x2c,0x25,0x5f,
                     0x3e,0x3f,0xba,0xbb,0xbc,0xbd,0xbe,0xbf,0xc0,0xc1,0xc2,
                     0x60,0x3a,0x23,0x40,0x27,0x3d,0x22,0xc3,0x61,0x62,0x63,
                     0x64,0x65,0x66,0x67,0x68,0x69,0xc4,0xc5,0xc6,0xc7,0xc8,
                     0xc9,0xca,0x6a,0x6b,0x6c,0x6d,0x6e,0x6f,0x70,0x71,0x72,
                     0xcb,0xcc,0xcd,0xce,0xcf,0xd0,0xd1,0x7e,0x73,0x74,0x75,
                     0x76,0x77,0x78,0x79,0x7a,0xd2,0xd3,0xd4,0x5b,0xd6,0xd7,
                     0xd8,0xd9,0xda,0xdb,0xdc,0xdd,0xde,0xdf,0xe0,0xe1,0xe2,
                     0xe3,0xe4,0x5d,0xe6,0xe7,0x7b,0x41,0x42,0x43,0x44,0x45,
                     0x46,0x47,0x48,0x49,0xe8,0xe9,0xea,0xeb,0xec,0xed,0x7d,
                     0x4a,0x4b,0x4c,0x4d,0x4e,0x4f,0x50,0x51,0x52,0xee,0xef,
                     0xf0,0xf1,0xf2,0xf3,0x5c,0x9f,0x53,0x54,0x55,0x56,0x57,
                     0x58,0x59,0x5a,0xf4,0xf5,0xf6,0xf7,0xf8,0xf9,0x30,0x31,
                     0x32,0x33,0x34,0x35,0x36,0x37,0x38,0x39,0xfa,0xfb,0xfc,
                     0xfd,0xfe,0xff};
  size_t n;
  unsigned char *s=(unsigned char *)src;
  unsigned char *d=(unsigned char *)dest;

  for (n=0; n < num; n++)
    d[n]=map[(int)s[n]];
}

char *ebcnasc(char *dest,char *src,size_t num)
{
  ebc_to_ascii(dest,src,num);

  return dest;
}

char *ebcasc(char *dest,char *src)
{
  size_t num=strlen(src);

  ebc_to_ascii(dest,src,num);
  dest[num]='\0';

  return dest;
}

char *ibcdnasc(char *dest,char *src,size_t num)
{
  static char map[]={0x30,0x31,0x32,0x33,0x34,0x35,0x36,0x37,0x38,0x39,0x3a,
                     0x3d,0x22,0x40,0x25,0x5b,0x2b,0x41,0x42,0x43,0x44,0x45,
                     0x46,0x47,0x48,0x49,0x3c,0x2e,0x29,0x5c,0x5e,0x3b,0x2d,
                     0x4a,0x4b,0x4c,0x4d,0x4e,0x4f,0x50,0x51,0x52,0x21,0x24,
                     0x2a,0x27,0x3f,0x3e,0x20,0x2f,0x53,0x54,0x55,0x56,0x57,
                     0x58,0x59,0x5a,0x5d,0x2c,0x28,0x5f,0x23,0x26};
  size_t n;
  unsigned char *s=(unsigned char *)src;
  unsigned char *d=(unsigned char *)dest;

  for (n=0; n < num; n++)
    d[n]=map[(int)s[n]];

  return dest;
}

char *ebcdnasc(char *dest,char *src,size_t num)
{
  static char map[]={0x3a,0x31,0x32,0x33,0x34,0x35,0x36,0x37,0x38,0x39,0x30,
                     0x3d,0x22,0x40,0x25,0x5b,0x20,0x2f,0x53,0x54,0x55,0x56,
                     0x57,0x58,0x59,0x5a,0x5d,0x2c,0x28,0x5f,0x23,0x26,0x2d,
                     0x4a,0x4b,0x4c,0x4d,0x4e,0x4f,0x50,0x51,0x52,0x21,0x24,
                     0x2a,0x27,0x3f,0x3e,0x2b,0x41,0x42,0x43,0x44,0x45,0x46,
                     0x47,0x48,0x49,0x3c,0x2e,0x29,0x5c,0x5e,0x3b};
  size_t n;
  unsigned char *s=(unsigned char *)src;
  unsigned char *d=(unsigned char *)dest;

  for (n=0; n < num; n++)
    d[n]=map[(int)s[n]];

  return dest;
}

char *dpcnasc(char *dest,char *src,size_t num)
{
  static char map[]={0x3a,0x41,0x42,0x43,0x44,0x45,0x46,0x47,0x48,0x49,0x4a,
                     0x4b,0x4c,0x4d,0x4e,0x4f,0x50,0x51,0x52,0x53,0x54,0x55,
                     0x56,0x57,0x58,0x59,0x5a,0x30,0x31,0x32,0x33,0x34,0x35,
                     0x36,0x37,0x38,0x39,0x2b,0x2d,0x2a,0x2f,0x28,0x29,0x24,
                     0x3d,0x20,0x2c,0x2e,0x23,0x5b,0x5d,0x25,0x22,0x5f,0x21,
                     0x26,0x27,0x3f,0x3c,0x3e,0x40,0x5c,0x5e,0x3b};
  size_t n;
  unsigned char *s=(unsigned char *)src;
  unsigned char *d=(unsigned char *)dest;

  for (n=0; n < num; n++)
    d[n]=map[(int)s[n]];
  return dest;
}

char *ascndpc(char *dest,char *src,size_t num)
{
  static char map[]={0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,
                     0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,
                     0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x2d,
                     0x3a,0x3a,0x3a,0x2b,0x3a,0x3a,0x3a,0x29,0x2a,0x27,0x25,
                     0x2e,0x26,0x2f,0x28,0x1b,0x1c,0x1d,0x1e,0x1f,0x20,0x21,
                     0x22,0x23,0x24,0x3a,0x3a,0x3a,0x2c,0x3a,0x3a,0x3a,0x1,
                     0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc,
                     0xd, 0xe, 0xf, 0x10,0x11,0x12,0x13,0x14,0x15,0x16,0x17,
                     0x18,0x19,0x1a,0x3a,0x3a,0x3a,0x3a,0x3a,0x3a,0x1, 0x2,
                     0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd,
                     0xe, 0xf, 0x10,0x11,0x12,0x13,0x14,0x15,0x16,0x17,0x18,
                     0x19,0x1a,0x3a,0x3a,0x3a,0x3a,0x3a};
  size_t n;
  unsigned char *s=(unsigned char *)src;
  unsigned char *d=(unsigned char *)dest;

  for (n=0; n < num; n++)
    d[n]=map[(int)s[n]];
  return dest;
}

struct Args {
  String ofile;
  short block_type;
  size_t trk,num_files,num_blocks;
  size_t num_errors;
  bool bcdconv;
  bool dpcconv;
  bool ebcconv;
  bool rewind;
  String device_name;
} args;

int errno;
ocstream ocs;

void parseArgs(int argc,char **argv)
{
  String argstring;
  StringParts sp;
  size_t n;

  args.block_type=0;
  args.bcdconv=false;
  args.dpcconv=false;
  args.ebcconv=false;
  args.rewind=true;
  args.num_files=0x7fffffff;
  args.num_blocks=0x7fffffff;
  args.num_errors=5;
  args.trk=0;

  argstring=getUnixArgsString(argc,argv);
  sp.fill(argstring,":");
  args.device_name=sp.getPart(0);
  args.trk=atoi(sp.getPart(1).toChar());
  for (n=2; n < sp.getLength(); n++) {
    if (sp.getPart(n).beginsWith("-blocke")) {
	n++;
	if (sp.getPart(n) == "none")
	  args.block_type=0;
	else if (sp.getPart(n) == "binary")
	  args.block_type=1;
	else if (sp.getPart(n) == "character")
	  args.block_type=2;
	else
	  error("blocking type "+sp.getPart(n)+" not recognized");
    }
    else if (sp.getPart(n).beginsWith("-tr")) {
	n++;
	if (sp.getPart(n) == "none") {
	  args.bcdconv=false;
	  args.dpcconv=false;
	}
	else if (sp.getPart(n) == "bcd")
	  args.bcdconv=true;
	else if (sp.getPart(n) == "dpc")
	  args.dpcconv=true;
	else if (sp.getPart(n) == "ebc")
	  args.ebcconv=true;
	else
	  error("translation type "+sp.getPart(n)+" not recognized");
    }
    else if (sp.getPart(n) == "-norew")
	args.rewind=false;
    else if (sp.getPart(n) == "-numb") {
	n++;
	args.num_blocks=atoi(sp.getPart(n).toChar());
    }
    else if (sp.getPart(n) == "-numf") {
	n++;
	args.num_files=atoi(sp.getPart(n).toChar());
    }
    else if (sp.getPart(n) == "-numerr") {
	n++;
	args.num_errors=atoi(sp.getPart(n).toChar());
    }
    else
	args.ofile=sp.getPart(n);
  }
  if (args.trk == 0)
    error(String("Error: drive type (trk) not specified"));
}

void read7trk()
{
  FILE *os;
  const size_t BUF_LEN=800000;
  unsigned char buffer[BUF_LEN],buf2[BUF_LEN],*p;
  FILE *device;
  int num_bytes;
  size_t nb=0,num_blocks=0,num_files=0;
  size_t n,orecs=0;
  size_t num_bad=0,bad_in_a_row=0;
  size_t eod=0;

  if (args.ofile.getLength() > 0) {
    if (args.block_type > 0) {
	if (!ocs.open(args.ofile.toChar()))
	  error("unable to open output file "+args.ofile);
    }
    else {
	if ( (os=fopen(args.ofile.toChar(),"w")) == NULL)
	  error("unable to open output file "+args.ofile);
    }
  }

  if ( (device=fopen(args.device_name.toChar(),"r")) == NULL)
    error(args.device_name+" not available");
  while (1) {
    while ( (num_bytes=read(device->_fileno,buffer,BUF_LEN)) > 0 || errno == EIO) {
	eod=0;
	nb++;
	num_blocks++;
	if (errno == EIO) {
	  num_bad++;
	  bad_in_a_row++;
	  std::cerr << "Error reading block " << nb << " in file " << num_files+1 << std::endl;
	  if (bad_in_a_row == args.num_errors)
	    break;
	  errno=0;
	}
	else {
	  bad_in_a_row=0;
	  if (args.bcdconv) {
	    ebcdnasc((char *)buffer,(char *)buffer,num_bytes);
	    p=buffer;
	  }
	  else if (args.dpcconv) {
	    dpcnasc((char *)buffer,(char *)buffer,num_bytes);
	    p=buffer;
	  }
	  else {
	    for (n=0; n < num_bytes; buf2[n++]=0);
	    setBits(buf2,buffer,0,6,0,num_bytes);
	    num_bytes=(num_bytes*6+7)/8;
	    p=buf2;
	  }
	  if (args.ofile.getLength() > 0) {
	    if (args.block_type == 0)
		fwrite(p,1,num_bytes,os);
	    else if (args.block_type == 1)
		ocs.write(p,num_bytes);
	    else
		ocs.write(p,num_bytes-1);
	  }
	  else
	    std::cout.write((char *)p,num_bytes);
	  orecs++;
	}
	if (args.num_blocks != 0x7fffffff && num_blocks == args.num_blocks)
	  break;
    }
    if (args.block_type > 0)
	ocs.writeEOF();
    if (bad_in_a_row == args.num_errors)
	break;
    eod++;
    num_files++;
    if (args.num_blocks != 0x7fffffff && num_blocks == args.num_blocks)
      break;
    if (args.num_files != 0x7fffffff && num_files == args.num_files)
	break;
    nb=0;
    if (eod > 1)
	break;
  }
  fclose(device);

  std::cout << "Tape records read: " << num_blocks << std::endl;
  std::cout << "Tape files read: " << num_files << std::endl;
  std::cout << "Block read errors: " << num_bad << std::endl;
  if (args.block_type == 0) {
    fclose(os);
    std::cout << "Output records written: " << orecs << std::endl;
  }
  else {
    ocs.close();
    std::cout << "COS-blocked records written: " << ocs.getNumberWritten() << std::endl;
  }
}

void read9trk()
{
  FILE *os=NULL;
  const size_t BUF_LEN=80000;
  unsigned char buffer[BUF_LEN];
  FILE *device;
  int num_bytes;
  size_t nb=0,num_blocks=0,num_files=0;
  size_t orecs=0;
  size_t num_bad=0,bad_in_a_row=0;
  size_t eod=0;
  String filename;

  if (args.device_name.getLength() == 0)
    error(String("don't know which 9-track device to use"));
  if (args.ofile.getLength() > 0) {
    if (args.block_type > 0) {
	if (!ocs.open(args.ofile.toChar()))
	  error("unable to open output file "+args.ofile);
    }
    else {
	filename=args.ofile+"."+ftos(num_files+1,5,0,'0');
	if ( (os=fopen(filename.toChar(),"w")) == NULL)
	  error("unable to open output file "+filename);
    }
  }

  while (1) {
    if (args.block_type == 0 && args.ofile.getLength() > 0 && os == NULL) {
	filename=args.ofile+"."+ftos(num_files+1,5,0,'0');
	if ( (os=fopen(filename.toChar(),"w")) == NULL)
	  error("unable to open output file "+filename);
    }
    if ( (device=fopen(args.device_name.toChar(),"r")) == NULL)
	error(args.device_name+" not available");
    while ( (num_bytes=read(device->_fileno,buffer,BUF_LEN)) > 0 || errno == EIO) {
	eod=0;
	nb++;
	num_blocks++;
	if (args.num_blocks != 0x7fffffff && num_blocks > args.num_blocks)
	  break;
	if (errno == EIO) {
	  num_bad++;
	  bad_in_a_row++;
	  if (bad_in_a_row == args.num_errors)
	    break;
	  else {
	    std::cerr << "Error reading block " << nb << " in file " << num_files+1 << std::endl;
	    errno=0;
	  }
	}
	else {
	  bad_in_a_row=0;
	  if (args.ebcconv)
	    ebcnasc((char *)buffer,(char *)buffer,num_bytes);
	  if (args.ofile.getLength() > 0) {
	    if (args.block_type == 0)
		fwrite(buffer,1,num_bytes,os);
	    else if (args.block_type == 1)
		ocs.write(buffer,num_bytes);
	    else
		ocs.write(buffer,num_bytes-1);
	  }
	  else
	    std::cout.write((char *)buffer,num_bytes);
	  orecs++;
	}
	if (args.num_blocks != 0x7fffffff && num_blocks == args.num_blocks)
	  break;
    }
    fclose(device);
    if (args.block_type > 0)
	ocs.writeEOF();
    if (bad_in_a_row == args.num_errors)
	break;
    eod++;
    num_files++;
    if (args.block_type == 0 && args.ofile.getLength() > 0) {
	fclose(os);
	os=NULL;
    }
    if (args.num_blocks != 0x7fffffff && num_blocks == args.num_blocks)
      break;
    if (args.num_files != 0x7fffffff && num_files == args.num_files)
	break;
    nb=0;
    if (eod > 1)
	break;
  }
  fclose(device);

  std::cout << "Tape records read: " << num_blocks << std::endl;
  std::cout << "Tape files read: " << num_files << std::endl;
  std::cout << "Block read errors: " << num_bad << std::endl;
  if (args.block_type == 0) {
    fclose(os);
    std::cout << "Output records written: " << orecs << std::endl;
  }
  else {
    ocs.close();
    std::cout << "COS-blocked records written: " << ocs.getNumberWritten() << std::endl;
  }
}

extern "C" {
void handler(int)
{
  if (args.block_type > 0) {
    ocs.writeEOF();
    ocs.close();
  }
}
}

int main(int argc,char **argv)
{
  String rew_string("/bin/mt -f ");

  signal(SIGHUP,handler);
  signal(SIGINT,handler);
  signal(SIGKILL,handler);
  signal(SIGPIPE,handler);
  signal(SIGTERM,handler);

  errno=0;
  if (argc == 1) {
    std::cerr << "usage: " << argv[0] << " <device> 7|9 [options] [output_file]" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "<device>    the device name (e.g. /dev/rmt/1mbn)" << std::endl;
    std::cerr << "7|9         indicates the number of tracks on the device" << std::endl;
    std::cerr << "            use 9 for anything other than a 7-track drive" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "-blocke[d] (none | binary | character)    specifies that the output file is a" << std::endl;
    std::cerr << "                                          stream of bytes, or a COS-blocked" << std::endl;
    std::cerr << "                                          binary or character dataset" << std::endl << std::endl;
    std::cerr << "-norew                                    the tape will not rewind before the" << std::endl;
    std::cerr << "                                          program exits (default is to rewind)" << std::endl << std::endl;
    std::cerr << "-numb <num>                               read <num> blocks from tape and quit" << std::endl << std::endl;
    std::cerr << "-numf <num>                               read <num> files from tape and quit" << std::endl << std::endl;
    std::cerr << "-numerr <num>                             quit after <num> consecutive block read" << std::endl;
    std::cerr << "                                          errors (the default is 5)" << std::endl << std::endl;
    std::cerr << "-tr[anslate] (none|bcd|dpc|ebc)           specifies character translation," << std::endl;
    std::cerr << "                                          none, BCD->ASCII, DPC->ASCII" << std::endl;
    std::cerr << "                                          or EBCDIC->ASCII" << std::endl;
  }
  else {
    parseArgs(argc,argv);
    if (args.trk == 7) {
      read7trk();
	if (args.rewind) {
	  std::cerr << "rewinding tape..." << std::endl;
	  system("/bin/mt -f /dev/rmt/4mbn rew 2>&1 /dev/null");
	  std::cerr << "...done" << std::endl;
	}
    }
    else if (args.trk == 9) {
      read9trk();
	if (args.rewind) {
	  std::cerr << "rewinding tape..." << std::endl;
	  rew_string+=(args.device_name+" rewind 2>&1 /dev/null");
	  system(rew_string.toChar());
	  std::cerr << "...done" << std::endl;
	}
    }

    perror("Program exiting with status");
  }
}
