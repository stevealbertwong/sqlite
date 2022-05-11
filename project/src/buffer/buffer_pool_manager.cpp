/*
 * buffer_pool_manager.cpp
 *
 */
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * 
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     DiskManager *disk_manager,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  

  // new/malloc/ptr == 1 global var, shared by many
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

/* 2 ways to get pages */

/**
 * find an existing page 
 * 
 * NOTE: once fetched, and before unpin(), page being used so not on LRU queue
 * HINT: see venn diagram of pages in RAM in free list vs page table vs lru
 * meta == 5 structs == page, lru, pagetable, free list, disk manager 
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) { 

  // 0. see if requested page in RAM
  Page *page = page_table_.Find(page_id);

  // 1. if exist, pin page, u() meta, return immediately
  if(page != nullptr){    
    ++page->pin_count_;
    lru_replacer_->Erase(page);
    return page;


  // 2. if not, then find a vacant spot in RAM, then read target page from disk into vacant page
  } else {
    
    // 2.1 free list page 
    if(!free_list_->empty()){ // vacant spot from free list
      page = free_list_->front();
      free_list_->pop_front();



    // 2.2 lru to FIFO swap existing page to disk
    }else{ // vacant spot from kicking victim page back to disk
      if(!lru_replacer_->Victim(page)){ // empty page -> victim page 
        return nullptr; // NO ANY vacant spot in RAM !!!!!
      }

      // 2.1.1 if lru page is dirty (diff from disk), then flush to u() disk
      if(page->is_dirty_){
        page_id_t dirty_page_id = page->page_id_;
        char *dirty_page_data = page->GetData();

                
        if(ENABLE_LOGGING){
          // dirty page is recored in WAL already 
          // WAL must flush before dirty page !!!!
          
          // 1. block unti WAL flushed 
          log_manager_->ForceFlushWAL();

          // 2. init DPT oldestLSN for fetched page 

        }

        disk_manager_->WritePage(dirty_page_id, dirty_page_data);
      }

      // 2.2 u() meta of the deleted page 
      lru_replacer_->Erase(page);
      page_table_->Remove(page);

    }
    
    // 2.3 r() target page from disk + u() meta of the new page 
    disk_manager_->ReadPage(page_id, page); // fills page->data
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page->page_id_ = page_id;
    page_table_->Insert(page_id, page);

    return page;
  }

}


/*
 * get a new and clean page
 * 
 * 1. choose a victim page either from free list or lru replacer
 * (NOTE: always choose from free list first)
 * 
 * 2. update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 * 
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) { 
  
  
  // 1. find a vacant spot in RAM
  Page *empty_page = nullptr;
  // 1.1 alway try free list first, if not then lru to FIFO swap existing page to disk
  if(!free_list_->empty()){ // vacant spot from free list
    empty_page = free_list_->front();
    free_list_.pop_front();
  
  // 1.2
  }else{ // vacant spot from kicking victim page back to disk
    if(!lru_replacer_->Victim(empty_page)){ // empty page -> victim page 
      return nullptr; // NO ANY vacant spot in RAM !!!!!
    }

    // 1.2.1 if lru page is dirty (diff from disk), then flush to u() disk
    if(empty_page->is_dirty){
      page_id_t dirty_page_id = empty_page->page_id_;
      char *dirty_page_data = empty_page->GetData();
      disk_manager_->WritePage(dirty_page_id, dirty_page_data);
    }

    // 1.2.2 u() meta of the deleted page 
    lru_replacer_->Erase(empty_page);
    page_table_->Remove(empty_page);    
  }

  // 2. u() meta there's a new page 
  page_id = disk_manager_->AllocatePage(); // RAM to keep track of the used disk addr
  empty_page->pin_count_ = 1;
  empty_page->is_dirty = false;
  empty_page->page_id = page_id;
  page_table_->Insert(page_id, empty_page);
    
  return page;
}



//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

/* 3 ways to delete pages */

/*
 * MUST call after fetch()
 * fetch() to ++ pin_count, unpin() to -- pin_count 
 * 
 * 1. find the page in RAM
 * 2. if pin_count <= 0 before this call, return false 
 * 3. if pin_count > 0, decrement it, put page to LRU when pin_count is 0 
 * 4. to set page dirty if user changed the page after fetch()  
 * 
 *
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  
  
  Page *page = page_table_->Find(page_id, page);
  if (page == nullptr){
    return false; // page MUST be in RAM for it to have pin_count
  }
  if (page->pin_count_ <= 0){
    return false;
  }
  if(--page->pin_count_ == 0){
    lru_replacer->Insert(page);
  }
  if(is_dirty){
    page->is_dirty = true;
  }
  
  return true;
}


/*
 * kinda useless since so simple
 * page_id -> page -> flush() to disk 
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) { 
  
  Page *page = page_table_->Find(page_id, page);
  if(page == nullptr){
    return false;
  }

  disk_manager_->WritePage(page_id, page->GetData());
  return true; 
}


/*
 * delete page in both RAM n disk
 * 
 * u() 5 meta
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) { 
  
  Page *page = page_table_->Find(page_id, page);
  if(page == nullptr){
    return false;
  }
  if (page->pin_count_ > 0){
    return false;
  }
  
  // delete from disk
  disk_manager_->DeallocatePage(page_id);

  // delete from meta 
  page_table_->Remove(page_id);
  lru_replacer_->Erase(page);

  // free up page slot in RAM 
  page->is_dirty = false;
  page->page_id = EMPTY_PAGE_ID;
  page->pin_count_ = 0;
  free_list->push_back(page);

  return true; // succesefully deleted
}



} // namespace cmudb
