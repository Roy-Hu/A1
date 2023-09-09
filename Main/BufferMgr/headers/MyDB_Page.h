
#ifndef PAGE_H
#define PAGE_H

#include <memory>
#include "MyDB_Table.h"

// page handles are basically smart pointers
using namespace std;

class MyDB_Page;
typedef shared_ptr <MyDB_Page> MyDB_PagePtr;

class MyDB_BufferManager;

class MyDB_Page {

public:

	~MyDB_Page ();

	MyDB_Page (MyDB_BufferManager& bufManager, MyDB_TablePtr tb, long pos);

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS
	void * getBytes ();
	void wroteBytes ();
	void increaseRefCnt ();
	void decreaseRefCnt ();

private:

	friend class MyDB_BufferManager;
	
	// YOUR CODE HERE
    int pageNum;
	int refCount;
	bool dirty;
	bool pin;
    void *bytes;
	MyDB_TablePtr table;

	MyDB_BufferManager& bufferManager;
};

#endif

