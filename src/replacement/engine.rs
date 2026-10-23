//! Filesystem operations share metadata only at namespace/save boundaries.
use super::{store::*,working::*,seal};
use anyhow::{Context,Result,ensure};
use std::{collections::BTreeMap,fs::File,path::Path,sync::{Arc,Mutex,RwLock,atomic::{AtomicBool,AtomicU64,Ordering}},time::Duration};

type Invalidate=Arc<dyn Fn(&str,u32)->Result<()>+Send+Sync>;
pub struct Handle {
    pub entry:Mutex<Entry>,pub scratch:Option<Arc<Mutex<Scratch>>>,pub writable:bool,
    closed:AtomicBool,pub delete:AtomicBool,pub directory:Mutex<Option<Vec<Entry>>>,
}
pub struct Engine {
    pub store:Mutex<Store>,pub working:WorkingLayer,
    handles:RwLock<BTreeMap<u64,Arc<Handle>>>,next:AtomicU64,
    invalidate:RwLock<Option<Invalidate>>,pub stopping:AtomicBool,
    pub error:Mutex<Option<String>>,_owner:File,
}
impl Engine {
    pub fn open(root:&Path,repo:Option<&str>)->Result<Arc<Self>> {
        std::fs::create_dir_all(root)?;
        let owner=std::fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(root.join("owner.lock"))?;
        owner.try_lock().context("STORE_ALREADY_OPEN")?;
        let store=Store::open(root,repo)?;let working=WorkingLayer::new(root,store.content.clone())?;let store=Mutex::new(store);
        seal::recover(&store,&working)?;
        let engine=Arc::new(Self{store,working,handles:RwLock::new(BTreeMap::new()),next:AtomicU64::new(1),invalidate:RwLock::new(None),stopping:AtomicBool::new(false),error:Mutex::new(None),_owner:owner});
        let weak=Arc::downgrade(&engine);
        std::thread::Builder::new().name("tkfs-sealer".into()).spawn(move||loop{
            std::thread::sleep(Duration::from_millis(50));
            let Some(engine)=weak.upgrade()else{break;};if engine.stopping.load(Ordering::Acquire){break;}
            let files=engine.working.pending(false);
            let result=seal::seal_batch(&engine.store,&engine.working,&files).and_then(|_|engine.working.reap_clean());
            if let Err(error)=result {*lock(&engine.error)=Some(format!("{error:#}"));}
        })?;
        Ok(engine)
    }
    pub fn set_invalidator(&self,callback:Invalidate) {*self.invalidate.write().unwrap()=Some(callback);}
    pub fn lookup(&self,path:&str)->Result<Entry> {lock(&self.store).lookup(path)}
    pub fn open_file(&self,path:&str,create:bool,directory:bool,writable:bool)->Result<u64> {
        ensure!(!self.stopping.load(Ordering::Acquire),"STOPPING");
        let (entry,basis)={let mut store=lock(&self.store);let e=if create {store.create(path,if directory{EntryKind::Directory}else{EntryKind::File})?}else{store.lookup(path)?};
            ensure!(!directory || e.kind==EntryKind::Directory,"NOT_A_DIRECTORY");
            let basis=if e.kind==EntryKind::File{Some(store.basis(&e.id)?)}else{None};(e,basis)};
        let scratch=basis.map(|basis|self.working.open(basis,writable)).transpose()?;
        let token=self.next.fetch_add(1,Ordering::Relaxed);
        self.handles.write().unwrap().insert(token,Arc::new(Handle{entry:Mutex::new(entry),scratch,writable,closed:AtomicBool::new(false),delete:AtomicBool::new(false),directory:Mutex::new(None)}));Ok(token)
    }
    pub fn handle(&self,token:u64)->Result<Arc<Handle>> {self.handles.read().unwrap().get(&token).cloned().context("INVALID_HANDLE")}
    pub fn info(&self,token:u64)->Result<(Entry,u64)> {let h=self.handle(token)?;let e=lock(&h.entry).clone();let len=if let Some(s)=&h.scratch{lock(s).len()?}else{0};Ok((e,len))}
    pub fn read(&self,token:u64,offset:u64,out:&mut[u8])->Result<usize> {let h=self.handle(token)?;let s=h.scratch.as_ref().context("IS_A_DIRECTORY")?;lock(s).read(self.working.content(),offset,out)}
    pub fn write(&self,token:u64,offset:u64,bytes:&[u8],append:bool,constrained:bool)->Result<usize> {let h=self.handle(token)?;ensure!(h.writable,"ACCESS_DENIED");let s=h.scratch.as_ref().context("IS_A_DIRECTORY")?;lock(s).write(self.working.content(),offset,bytes,append,constrained)}
    pub fn truncate(&self,token:u64,size:u64,allocation:bool)->Result<()> {
        let h=self.handle(token)?;ensure!(h.writable,"ACCESS_DENIED");let mut s=lock(h.scratch.as_ref().context("IS_A_DIRECTORY")?);
        if allocation && size>=s.len()?{return Ok(());}
        let count=s.len()?.min(s.base_visible_len).div_ceil(crate::model::content::CHUNK_SIZE);for index in 0..count {s.hydrate_chunk(self.working.content(),index)?;}
        s.truncate(size)
    }
    pub fn flush(&self,token:Option<u64>)->Result<()> {
        let files=if let Some(token)=token{self.handle(token)?.scratch.clone().into_iter().collect()}else{self.working.pending(true)};
        seal::seal_batch(&self.store,&self.working,&files)?;
        // Also covers namespace-only fsyncs with no dirty content event.
        lock(&self.store).db.execute_batch("PRAGMA wal_checkpoint(FULL);")?;
        *lock(&self.error)=None;Ok(())
    }
    pub fn cleanup(&self,token:u64,delete:bool)->Result<()> {
        let h=self.handle(token)?;
        if delete || h.delete.load(Ordering::Acquire) {let entity=lock(&h.entry).id.clone();let mut store=lock(&self.store);if !store.entry(&entity)?.deleted {store.delete(&entity)?;}}
        if !h.closed.swap(true,Ordering::AcqRel) {if let Some(s)=&h.scratch{self.working.close(s,h.writable);}}
        Ok(())
    }
    pub fn close(&self,token:u64)->Result<()> {self.cleanup(token,false)?;self.handles.write().unwrap().remove(&token);Ok(())}
    pub fn children(&self,token:u64,restart:bool)->Result<Vec<Entry>> {let h=self.handle(token)?;let mut cache=lock(&h.directory);if restart || cache.is_none(){*cache=Some(lock(&self.store).projection().children(&lock(&h.entry).id));}Ok(cache.as_ref().unwrap().clone())}
    pub fn rename(&self,token:u64,target:&str,replace:bool)->Result<()> {let h=self.handle(token)?;let entity=lock(&h.entry).id.clone();let mut store=lock(&self.store);store.rename(&entity,target,replace)?;*lock(&h.entry)=store.entry(&entity)?;Ok(())}
    pub fn basic(&self,token:u64,attributes:u32,times:[u64;4])->Result<()> {
        let h=self.handle(token)?;let entity=lock(&h.entry).id.clone();let mut store=lock(&self.store);let mut e=store.entry(&entity)?;
        for (old,new) in [&mut e.times.created,&mut e.times.accessed,&mut e.times.modified,&mut e.times.changed].into_iter().zip(times){if new!=0 && new!=u64::MAX{*old=new;}}
        store.set_times(&entity,e.times,None,(attributes!=u32::MAX).then_some(attributes))?;*lock(&h.entry)=store.entry(&entity)?;Ok(())
    }
    pub fn apply_remote(&self,events:&[Event])->Result<Vec<String>> {
        let mut store=lock(&self.store);let mut before=BTreeMap::new();for event in events{if let Ok(path)=store.projection().path(&event.entity){before.insert(event.entity.clone(),path);}}
        let applied=store.apply(events)?;let mut notifications=BTreeMap::new();
        for event in events.iter().filter(|e|applied.contains(&e.id) && e.branch==store.active) {
            if let Some(path)=before.get(&event.entity){notifications.insert(path.clone(),3);}
            if let Ok(path)=store.projection().path(&event.entity){notifications.insert(path,if event.state.deleted{2}else{3});}
        } drop(store);
        if let Some(callback)=self.invalidate.read().unwrap().as_ref(){for (path,action) in notifications {callback(&path,action)?;}}
        Ok(applied)
    }
    pub fn shutdown(&self)->Result<()> {self.stopping.store(true,Ordering::Release);self.flush(None)?;self.working.reap_clean()?;Ok(())}
}
