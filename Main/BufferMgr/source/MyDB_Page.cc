
#ifndef PAGE_C
#define PAGE_C

#include <memory>
#include "MyDB_Page.h"
#include "MyDB_BufferManager.h"

MyDB_Page :: ~MyDB_Page () {
}

MyDB_Page :: MyDB_Page (MyDB_BufferManager& bufManager, MyDB_TablePtr tb, long pgNum) 
    : bufferManager(bufManager) {
    table = tb;
    pageNum = pgNum;
    bytes = nullptr;
    dirty = false;
    pin = false;
}

void * MyDB_Page :: getBytes () {
    bufferManager.accessPage(*this);
    return bytes;
}

void MyDB_Page :: wroteBytes () {
    dirty = true;
}

void MyDB_Page :: increaseRefCnt () {
    refCount++;
}

void MyDB_Page :: decreaseRefCnt () {
    if (--refCount == 0) {
        bufferManager.releasePage(*this);
    }
}

#endif
