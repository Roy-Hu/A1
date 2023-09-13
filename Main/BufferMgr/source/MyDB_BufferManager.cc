
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>
#include <fcntl.h>
#include <iostream>
#include <algorithm>
#include <unistd.h>

using namespace std;

MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr whichTable, long i) {
	if (tableFdMap.count(whichTable) == 0) {
		int fd = open(whichTable->getStorageLoc().c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
		if (fd != -1) {
			tableFdMap[whichTable] = fd;
		} else {
			cout << "Fail to open file " << whichTable->getStorageLoc().c_str() << endl;
			exit(0);	
		}
	}

	pair <MyDB_TablePtr, long> tablePageId = make_pair (whichTable, i);
	if (idPageMap.count(tablePageId) == 0) {
		MyDB_PagePtr newPage = make_shared <MyDB_Page> (*this, whichTable, i);
		idPageMap[tablePageId] = newPage;
	}

	return make_shared <MyDB_PageHandleBase> (idPageMap[tablePageId]);
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
	if (tableFdMap.count(nullptr) == 0) {
		int fd = open(tempFile.c_str(), O_CREAT | O_RDWR, 0666);
		if (fd != -1) {
			// cout << "open file " << tempFile.c_str() << endl;
			tableFdMap[nullptr] = fd;
		} else {
			cout << "Fail to temp file" << endl;
			exit(0);	
		}
	}

	long pageNum;
	if (unusedPageNumInTmpFile.size() == 0) {
		pageNum = topTmpPageNum++;
	} else {
		pageNum = unusedPageNumInTmpFile.front();
		unusedPageNumInTmpFile.erase(unusedPageNumInTmpFile.begin());
	}

	pair <MyDB_TablePtr, long> tablePageId = make_pair (nullptr, pageNum);
	if (idPageMap.count(tablePageId) == 0) {
		MyDB_PagePtr newPage = make_shared <MyDB_Page> (*this, nullptr, pageNum);
		idPageMap[tablePageId] = newPage;
	}

	MyDB_PagePtr page = idPageMap[tablePageId];
	
	return make_shared <MyDB_PageHandleBase> (idPageMap[tablePageId]);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long i) {
	if (tableFdMap.count(whichTable) == 0) {
		int fd = open(whichTable->getStorageLoc().c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);

		if (fd != -1) {
			tableFdMap[whichTable] = fd;
		} else {
			cout << "Fail to open file " << whichTable->getStorageLoc().c_str() << endl;
			exit(0);	
		}
	}

	pair <MyDB_TablePtr, long> tablePageId = make_pair (whichTable, i);
	if (idPageMap.count(tablePageId) == 0) {
		MyDB_PagePtr newPage = make_shared <MyDB_Page> (*this, whichTable, i);
		idPageMap[tablePageId] = newPage;
	}

	vector<MyDB_PagePtr>::iterator it = find(pagesInRAM.begin(), pagesInRAM.end(), idPageMap[tablePageId]);
	// If page already in RAM, remove it from the pagesInRAM so that it will never be kicked out
	if (it != pagesInRAM.end()) {
		pagesInRAM.erase(it);
	// Page is not previos pinned and is not in RAM
	} else if (idPageMap[tablePageId]->bytes == nullptr){
		MyDB_PagePtr page = idPageMap[tablePageId];

		// if no space left in the RAM, try finding a victim to kick
		if (unusedRAM.size() == 0) {
			findAndKickVictim();
			if (unusedRAM.size() == 0) {
				cout << "fail to kick victim, RAM space is still full" << endl;
				return nullptr;
			}
		} 

		page->bytes = unusedRAM.back();
		unusedRAM.pop_back();

		int fd = tableFdMap[page->table];
		lseek(fd, page->pageNum * pageSize, SEEK_SET);
		read(fd, page->bytes, pageSize);	
	}

	return make_shared <MyDB_PageHandleBase> (idPageMap[tablePageId]);	
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	if (tableFdMap.count(nullptr) == 0) {
		if (int fd = open(tempFile.c_str(), O_CREAT | O_RDWR, 0666) != -1) {
			tableFdMap[nullptr] = fd;
		} else {
			cout << "Fail to open file " << tempFile << endl;
			exit(0);	
		}
	}
	long pageNum;
	if (unusedPageNumInTmpFile.size() == 0) {
		pageNum = topTmpPageNum++;
	} else {
		pageNum = unusedPageNumInTmpFile.front();
		unusedPageNumInTmpFile.erase(unusedPageNumInTmpFile.begin());
	}

	pair <MyDB_TablePtr, long> tablePageId = make_pair (nullptr, pageNum);
	if (idPageMap.count(tablePageId) == 0) {
		MyDB_PagePtr newPage = make_shared <MyDB_Page> (*this, nullptr, pageNum);
		idPageMap[tablePageId] = newPage;
	}
	
	if (unusedRAM.size() == 0) {
		findAndKickVictim();
		if (unusedRAM.size() == 0) {
			cout << "fail to kick victim, RAM space is still full" << endl;
			return nullptr;
		}
	} 

	MyDB_PagePtr page = idPageMap[tablePageId];

	page->bytes = unusedRAM.back();
	unusedRAM.pop_back();
	
	return make_shared <MyDB_PageHandleBase> (page);
  }

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {
	// Add back to pagesInRAM so that the page can be kicked
	pagesInRAM.push_back(unpinMe->getPage());
}

void MyDB_BufferManager :: findAndKickVictim() {
	MyDB_PagePtr victim = pagesInRAM.front();
	if (victim->dirty) {
		int fd = tableFdMap[victim->table];
		lseek(fd, victim->pageNum * pageSize, SEEK_SET);
		if (write(fd, victim->bytes, pageSize) == -1) {
			cout << "Fail to write back bytes to Table: " << victim->table->getStorageLoc().c_str() << ", Page Num: " << victim->pageNum << endl;
		}
		victim->dirty = false;
	}

	unusedRAM.push_back(victim->bytes);
	pagesInRAM.erase(pagesInRAM.begin());

	victim->bytes = nullptr;
}

void MyDB_BufferManager :: accessPage (MyDB_Page &page) {
	pair <MyDB_TablePtr, long> tablePageId = make_pair (page.table, page.pageNum);
	MyDB_PagePtr pagePtr= idPageMap[tablePageId];

	vector<MyDB_PagePtr>::iterator it = find(pagesInRAM.begin(), pagesInRAM.end(), pagePtr);
	// Find page in the RAM
	if (it != pagesInRAM.end()) {
		pagesInRAM.erase(it);
		pagesInRAM.push_back(*it);
		return;

	// Page in the RAM but not in pagesInRAM => pinned page
	} else if (it == pagesInRAM.end() && pagePtr->bytes != nullptr) {
		return;
	// Page isn't in the RAM
	} else {
		// if no space left in the RAM, try finding a victim to kick
		if (unusedRAM.size() == 0) {
			findAndKickVictim();
			if (unusedRAM.size() == 0) {
				cout << "fail to kick victim, RAM space is still full" << endl;
				return;
			}
		} 

		pagePtr->bytes = unusedRAM.back();
		unusedRAM.pop_back();

		int fd = tableFdMap[pagePtr->table];
		lseek(fd, pagePtr->pageNum * pageSize, SEEK_SET);
		read(fd, pagePtr->bytes, pageSize);	
		pagesInRAM.push_back(pagePtr);
	}
}

void MyDB_BufferManager :: releasePage(MyDB_Page &page) {
	pair <MyDB_TablePtr, long> tablePageId = make_pair (page.table, page.pageNum);
	MyDB_PagePtr pagePtr= idPageMap[tablePageId];

	vector<MyDB_PagePtr>::iterator it = find(pagesInRAM.begin(), pagesInRAM.end(), pagePtr);

	// cout << "Release page num " << page.pageNum << endl;
	// We dont need to remember this page in the disk and RAM anymore since it is a anomonous page 
	if (pagePtr->table == nullptr) {
		unusedPageNumInTmpFile.push_back(pagePtr->pageNum);

		if (it != pagesInRAM.end()) {
			pagesInRAM.erase(it);
		}

		if (pagePtr->bytes != nullptr) {
			unusedRAM.push_back(pagePtr->bytes);
			pagePtr->bytes = nullptr;
		}
		// The pagePtr will automated be deleted here since no hanlders for this page exist anymore
		idPageMap.erase(tablePageId);

	// Page in the RAM but not in pagesInRAM => pinned page
	} else if (it == pagesInRAM.end() && pagePtr->bytes != nullptr) {
		// only add the page back to the pagesInRAM
		pagesInRAM.push_back(pagePtr);
	// For non-anomolous page that is in the pageInRAM, recycle the RAM
	} else if (it != pagesInRAM.end()) { 
		// write it back to disk if the page is dirty since we are going to erease the bytes in ram
		if (pagePtr->dirty) {
			int fd = tableFdMap[pagePtr->table];
			lseek(fd, pagePtr->pageNum * pageSize, SEEK_SET);

			if (write(fd, pagePtr->bytes, pageSize) == -1) {
				cout << "Fail to write back bytes to Table: " << pagePtr->table->getStorageLoc().c_str() << ", Page Num: " << pagePtr->pageNum << endl;
			}			

			pagePtr->dirty = false;
		}

		unusedRAM.push_back(pagePtr->bytes);
		pagesInRAM.erase(it);
		pagePtr->bytes = nullptr;
	}
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pgSize, size_t numPgs, string tmpFile) {
	pageSize = pgSize;
	numPages = numPgs;
	tempFile = tmpFile;

	for (int i = 0; i < numPgs; i++) {
		unusedRAM.push_back(malloc(pgSize));
	}
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
	for (auto idPage : idPageMap) {
		MyDB_PagePtr pagePtr = idPage.second;
		if (pagePtr->bytes != nullptr) {
			int fd = tableFdMap[pagePtr->table];
			lseek(fd, pagePtr->pageNum * pageSize, SEEK_SET);

			if (write(fd, pagePtr->bytes, pageSize) == -1) {
				cout << "Fail to write back bytes to Table: " << pagePtr->table->getStorageLoc().c_str() << ", Page Num: " << pagePtr->pageNum << endl;
			}			

			free(pagePtr->bytes);
		}
	}      

	for (auto ram : unusedRAM) {
		free(ram);
	}
    for (auto fd : tableFdMap) {
        close(fd.second);
	}

	idPageMap.clear();
	unusedRAM.clear();
	tableFdMap.clear();
	pagesInRAM.clear();
	unusedPageNumInTmpFile.clear();
}

#endif


