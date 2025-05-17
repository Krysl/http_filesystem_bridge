#![allow(unused)]

use log::debug;
use std::{
    future::Future,
    pin::Pin,
    sync::{mpsc, Arc, Mutex, RwLock},
    thread,
};
use tokio::runtime::Runtime;

#[derive(Debug)]
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

type SyncFunction = dyn FnOnce() + Send + 'static;
type AsyncFunction = dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<(), reqwest::Error>> + Send>>
    + Send
    + 'static;

enum FunctionCallback {
    Sync(Box<SyncFunction>),
    Async(Box<AsyncFunction>),
}

enum Job {
    Sync(Box<SyncFunction>),
    Async(Box<AsyncFunction>),
}

impl ThreadPool {
    /// Create a new ThreadPool.
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero.
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Job::Sync(Box::new(f));

        self.sender.as_ref().unwrap().send(job).unwrap();
    }

    pub fn execute_async<F>(&self, f: F)
    where
        F: FnOnce() -> Pin<Box<dyn Future<Output = Result<(), reqwest::Error>> + Send>>
            + Send
            + 'static,
    {
        let job = Job::Async(Box::new(f));

        self.sender.as_ref().unwrap().send(job).unwrap();
    }
    pub fn working_num(&self) -> u32 {
        self.workers
            .iter()
            .filter(|w| *w.is_working.read().unwrap())
            .count() as u32
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());

        for worker in self.workers.drain(..) {
            debug!("Shutting down worker {}", worker.id);

            worker.thread.join().unwrap();
        }
    }
}

#[derive(Debug)]
struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
    is_working: Arc<RwLock<bool>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let rt = Runtime::new().unwrap();
        let is_working = Arc::new(RwLock::new(false));
        let _is_working = Arc::clone(&is_working);

        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv();
            {
                let mut working = _is_working.write().unwrap();
                *working = true;
            }
            match message {
                Ok(job) => {
                    debug!("Worker {id} got a job; executing.");

                    match job {
                        Job::Sync(f) => f(),
                        Job::Async(f) => {
                            let fut = f();
                            // futures::executor::block_on(fut);
                            // tokio::spawn(fut);
                            let ret = rt.block_on(fut);
                            match ret {
                                Ok(_) => debug!("Worker {id} finished async job."),
                                Err(e) => debug!("Worker {id} failed async job: {e}"),
                            }
                        }
                    }
                }
                Err(_) => {
                    debug!("Worker {id} disconnected; shutting down.");
                    break;
                }
            }
            {
                let mut working = _is_working.write().unwrap();
                *working = false;
            }
        });

        Worker {
            id,
            thread,
            is_working,
        }
    }
}
