
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include "MyDB_PageHandle.h"

void * MyDB_PageHandleBase :: getBytes () {
	return page->getBytes();
}

void MyDB_PageHandleBase :: wroteBytes () {
	return page->wroteBytes();
}

MyDB_PageHandleBase :: ~MyDB_PageHandleBase () {
	page->decreaseRefCnt();
}

MyDB_PageHandleBase :: MyDB_PageHandleBase (MyDB_PagePtr pg) {
	page = pg;
	page->increaseRefCnt();
}

MyDB_PagePtr MyDB_PageHandleBase :: getPage() {
	return page;
};

#endif

